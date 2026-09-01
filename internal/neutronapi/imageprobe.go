package neutronapi

import (
	"context"
	"fmt"
	"time"

	"github.com/openstack-k8s-operators/lib-common/modules/common/helper"
	"github.com/openstack-k8s-operators/lib-common/modules/common/job"
	"github.com/openstack-k8s-operators/lib-common/modules/common/pod"
	"github.com/openstack-k8s-operators/lib-common/modules/users"
	neutronv1beta1 "github.com/openstack-k8s-operators/neutron-operator/api/v1beta1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
)

// TEMPORARY (OSPRH-33113): the wsgi annotation (NeutronWSGILabel) now
// defaults to true on the openstack-operator side ahead of the separate,
// global rollout of WSGI-only (Eventlet-removed) images across every
// openstack-k8s-operators default image pin. Until that rollout completes,
// some deployments may still resolve ContainerImage to a pre-WSGI image. In
// that case IsWSGIEffective below falls back to the Eventlet strategy
// regardless of what the annotation says, by inspecting the resolved image
// for NeutronServerBinaryPath via ImageProbeJob.
//
// Remove this whole file and its two call sites (reverting them to
// instance.IsWSGI()) once WSGI-only images are the default everywhere.

// NeutronServerBinaryPath is only present in pre-WSGI (Eventlet-capable)
// Neutron images. Its absence indicates a WSGI-only (Eventlet-removed) image.
const NeutronServerBinaryPath = "/usr/bin/neutron-server"

// ImageProbeCommand exits 0 if NeutronServerBinaryPath is present (a
// pre-WSGI, Eventlet-capable image) and non-zero otherwise, so the result
// can be read straight off the Job's Succeeded/Failed status.
var ImageProbeCommand = fmt.Sprintf("test -f %s", NeutronServerBinaryPath)

// imageProbeHash is only used as the job.NewJob jobType for log messages.
const imageProbeHash = "imageprobe"

// ImageProbeJobName returns the name of the ImageProbeJob for cr.
func ImageProbeJobName(cr *neutronv1beta1.NeutronAPI) string {
	return cr.Name + "-image-probe"
}

// ImageProbeJob builds the Job used to detect whether cr.Spec.ContainerImage
// is a pre-WSGI (Eventlet-capable) image, by checking for the presence of
// NeutronServerBinaryPath.
func ImageProbeJob(
	cr *neutronv1beta1.NeutronAPI,
	labels map[string]string,
	annotations map[string]string,
) *batchv1.Job {
	name := ImageProbeJobName(cr)

	probeJob := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:        name,
			Namespace:   cr.Namespace,
			Annotations: annotations,
			Labels:      labels,
		},
		Spec: batchv1.JobSpec{
			// A single attempt is enough: the probe command is a
			// deterministic file check, not something that benefits from
			// retries.
			BackoffLimit: ptr.To(int32(0)),
			Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					RestartPolicy:                corev1.RestartPolicyNever,
					ServiceAccountName:           cr.RbacResourceName(),
					AutomountServiceAccountToken: ptr.To(false),
					SecurityContext:              pod.RestrictivePodSecurityContext(users.NeutronUID, users.NeutronGID),
					Containers: []corev1.Container{
						{
							Name:            name,
							Command:         []string{"/bin/bash"},
							Args:            []string{"-c", ImageProbeCommand},
							Image:           cr.Spec.ContainerImage,
							SecurityContext: pod.RestrictiveSecurityContext(users.NeutronUID, users.NeutronGID),
						},
					},
				},
			},
		},
	}

	if cr.Spec.NodeSelector != nil {
		probeJob.Spec.Template.Spec.NodeSelector = *cr.Spec.NodeSelector
	}

	return probeJob
}

// IsWSGIEffective returns the deployment strategy to actually use, layering
// a runtime safety net on top of instance.IsWSGI(): if the wsgi annotation
// says wsgi, it fires off (but does not block reconcile on) ImageProbeJob
// and falls back to the Eventlet strategy once that Job finds
// NeutronServerBinaryPath in the resolved ContainerImage.
//
// The probe result is read straight off the Job's own status on every call
// instead of being persisted on the NeutronAPI: job.DoJob already tracks
// whether ImageProbeJob needs to be (re)created via a hash stored as an
// annotation on the Job itself, so no CR status/hash bookkeeping is needed
// here. Until the Job completes, this just keeps returning instance.IsWSGI().
func IsWSGIEffective(
	ctx context.Context,
	h *helper.Helper,
	instance *neutronv1beta1.NeutronAPI,
	labels map[string]string,
	annotations map[string]string,
) bool {
	if !instance.IsWSGI() {
		return false
	}

	probeJobDef := ImageProbeJob(instance, labels, annotations)
	probeJob := job.NewJob(probeJobDef, imageProbeHash, instance.Spec.PreserveJobs, time.Duration(5)*time.Second, "")
	if _, err := probeJob.DoJob(ctx, h); err != nil {
		// A Failed image-probe Job just means NeutronServerBinaryPath was
		// not found (a WSGI-only image) -- not a real error.
		h.GetLogger().Info(fmt.Sprintf("Image probe Job %s: %v", probeJobDef.Name, err))
	}

	existingProbeJob := &batchv1.Job{}
	err := h.GetClient().Get(ctx, types.NamespacedName{Name: probeJobDef.Name, Namespace: instance.Namespace}, existingProbeJob)
	if err == nil && existingProbeJob.Status.Succeeded > 0 {
		return false
	}
	return true
}
