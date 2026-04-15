package neutronapi

import (
	"context"
	"fmt"
	"os"
	"time"

	neutronv1 "github.com/openstack-k8s-operators/neutron-operator/api/v1beta1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

const (
	// DeploymentStrategyAnnotation stores the detected deployment strategy
	DeploymentStrategyAnnotation = "neutron.openstack.org/deployment-strategy"
	// ContainerImageHashAnnotation stores the hash of the container image used for detection
	ContainerImageHashAnnotation = "neutron.openstack.org/image-hash"
	// DetectionTimeoutSeconds is the maximum time to wait for detection job completion
	DetectionTimeoutSeconds = 30
)

// StrategyDetector handles detection of the appropriate deployment strategy
type StrategyDetector struct {
	client client.Client
}

// NewStrategyDetector creates a new strategy detector
func NewStrategyDetector(client client.Client) *StrategyDetector {
	return &StrategyDetector{
		client: client,
	}
}

// DetectStrategy determines which deployment strategy to use for the given NeutronAPI instance
func (d *StrategyDetector) DetectStrategy(ctx context.Context, instance *neutronv1.NeutronAPI) (DeploymentStrategy, error) {
	// Check for environment variable to force strategy (useful for testing)
	if forceStrategy := os.Getenv("NEUTRON_DEPLOYMENT_STRATEGY"); forceStrategy != "" {
		switch forceStrategy {
		case "eventlet":
			return &EventletStrategy{}, nil
		case "uwsgi":
			return &UwsgiStrategy{}, nil
		case "gunicorn":
			return &GunicornStrategy{}, nil
		}
	}

	// Check if we have a cached detection result
	if strategy := d.getCachedStrategy(instance); strategy != nil {
		return strategy, nil
	}

	// Perform binary detection with fallback
	isEventlet, err := d.detectNeutronServerBinary(ctx, instance)
	if err != nil {
		// Fallback to eventlet strategy on detection failure for backwards compatibility
		fmt.Printf("Warning: neutron-server binary detection failed (%v), falling back to eventlet strategy\n", err)
		strategy := &EventletStrategy{}
		// Still try to cache the fallback result
		if cacheErr := d.cacheStrategy(ctx, instance, strategy); cacheErr != nil {
			fmt.Printf("Warning: failed to cache fallback strategy: %v\n", cacheErr)
		}
		return strategy, nil
	}

	var strategy DeploymentStrategy
	if isEventlet {
		strategy = &EventletStrategy{}
	} else {
		// For non-eventlet deployments, choose between uwsgi and gunicorn
		// Default to uwsgi for backwards compatibility, but allow gunicorn via env var
		if wsgiServer := os.Getenv("NEUTRON_WSGI_SERVER"); wsgiServer == "gunicorn" {
			strategy = &GunicornStrategy{}
		} else {
			strategy = &UwsgiStrategy{}
		}
	}

	// Cache the detection result
	if err := d.cacheStrategy(ctx, instance, strategy); err != nil {
		// Log warning but continue - caching failure is not critical
		fmt.Printf("Warning: failed to cache strategy detection result: %v\n", err)
	}

	return strategy, nil
}

// getCachedStrategy retrieves cached strategy detection result
func (d *StrategyDetector) getCachedStrategy(instance *neutronv1.NeutronAPI) DeploymentStrategy {
	annotations := instance.GetAnnotations()
	if annotations == nil {
		return nil
	}

	strategyType, exists := annotations[DeploymentStrategyAnnotation]
	if !exists {
		return nil
	}

	// Check if the container image hash matches (strategy is valid for current image)
	imageHash, hashExists := annotations[ContainerImageHashAnnotation]
	if !hashExists || imageHash != hashContainerImage(instance.Spec.ContainerImage) {
		return nil
	}

	switch strategyType {
	case "eventlet":
		return &EventletStrategy{}
	case "uwsgi":
		return &UwsgiStrategy{}
	case "gunicorn":
		return &GunicornStrategy{}
	default:
		return nil
	}
}

// cacheStrategy stores the strategy detection result in annotations
func (d *StrategyDetector) cacheStrategy(ctx context.Context, instance *neutronv1.NeutronAPI, strategy DeploymentStrategy) error {
	// Create a patch to update annotations
	patch := client.MergeFrom(instance.DeepCopy())

	if instance.Annotations == nil {
		instance.Annotations = make(map[string]string)
	}

	instance.Annotations[DeploymentStrategyAnnotation] = strategy.GetDeploymentType()
	instance.Annotations[ContainerImageHashAnnotation] = hashContainerImage(instance.Spec.ContainerImage)

	return d.client.Patch(ctx, instance, patch)
}

// detectNeutronServerBinary creates a detection job to check for neutron-server binary
func (d *StrategyDetector) detectNeutronServerBinary(ctx context.Context, instance *neutronv1.NeutronAPI) (bool, error) {
	jobName := fmt.Sprintf("neutron-detection-%s-%s", instance.Name, hashContainerImage(instance.Spec.ContainerImage)[:8])

	// Create detection job
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      jobName,
			Namespace: instance.Namespace,
		},
		Spec: batchv1.JobSpec{
			TTLSecondsAfterFinished: ptr.To(int32(120)), // Clean up after 2 minutes
			Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					RestartPolicy: corev1.RestartPolicyNever,
					Containers: []corev1.Container{
						{
							Name:    "detector",
							Image:   instance.Spec.ContainerImage,
							Command: []string{"/bin/sh"},
							Args:    []string{"-c", "test -f /usr/bin/neutron-server"},
							SecurityContext: &corev1.SecurityContext{
								RunAsUser:                ptr.To(NeutronUID),
								RunAsGroup:               ptr.To(NeutronGID),
								RunAsNonRoot:             ptr.To(true),
								AllowPrivilegeEscalation: ptr.To(false),
								ReadOnlyRootFilesystem:   ptr.To(true),
								Capabilities: &corev1.Capabilities{
									Drop: []corev1.Capability{"ALL"},
								},
							},
						},
					},
				},
			},
		},
	}

	// Set owner reference for cleanup (best effort - don't fail if this doesn't work)
	_ = controllerutil.SetControllerReference(instance, job, d.client.Scheme())

	// Create the job with error handling
	if err := d.client.Create(ctx, job); err != nil {
		if errors.IsAlreadyExists(err) {
			// Job already exists, try to get its result
			return d.waitForDetectionJob(ctx, jobName, instance.Namespace)
		}
		return false, fmt.Errorf("failed to create detection job: %w", err)
	}

	// Wait for job completion with timeout
	timeoutCtx, cancel := context.WithTimeout(ctx, DetectionTimeoutSeconds*time.Second)
	defer cancel()

	isEventlet, err := d.waitForDetectionJob(timeoutCtx, jobName, instance.Namespace)

	// Clean up the job (best effort - don't block on this)
	go func() {
		deleteCtx, deleteCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer deleteCancel()
		_ = d.client.Delete(deleteCtx, job)
	}()

	return isEventlet, err
}

// waitForDetectionJob waits for the detection job to complete and returns the result
func (d *StrategyDetector) waitForDetectionJob(ctx context.Context, jobName, namespace string) (bool, error) {
	ticker := time.NewTicker(1 * time.Second) // Check more frequently for faster response
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return false, fmt.Errorf("detection job cancelled or timed out: %w", ctx.Err())

		case <-ticker.C:
			job := &batchv1.Job{}
			err := d.client.Get(ctx, types.NamespacedName{
				Name:      jobName,
				Namespace: namespace,
			}, job)
			if err != nil {
				if errors.IsNotFound(err) {
					// Job might not be created yet or was deleted
					continue
				}
				return false, fmt.Errorf("failed to get detection job status: %w", err)
			}

			// Check if job is complete
			for _, condition := range job.Status.Conditions {
				if condition.Type == batchv1.JobComplete && condition.Status == corev1.ConditionTrue {
					// Job succeeded - neutron-server binary exists (eventlet)
					return true, nil
				}
				if condition.Type == batchv1.JobFailed && condition.Status == corev1.ConditionTrue {
					// Job failed - neutron-server binary does not exist (uwsgi)
					return false, nil
				}
			}
			// Job still running, continue waiting
		}
	}
}

// hashContainerImage creates a simple hash of the container image for caching
func hashContainerImage(image string) string {
	// Simple hash implementation - in production might want to use crypto/sha256
	hash := 0
	for _, c := range image {
		hash = 31*hash + int(c)
	}
	return fmt.Sprintf("%x", hash)
}
