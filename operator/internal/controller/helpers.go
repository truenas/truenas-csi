package controller

import (
	"os"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/utils/ptr"

	csiv1alpha1 "github.com/truenas/truenas-csi/operator/api/v1alpha1"
)

// buildTrueNASEnvVars creates the environment variables for TrueNAS CSI containers
func buildTrueNASEnvVars(csi *csiv1alpha1.TrueNASCSI) []corev1.EnvVar {
	return []corev1.EnvVar{
		{Name: "CSI_ENDPOINT", Value: CSISocketPath},
		fieldRefEnvVar("NODE_ID", "spec.nodeName"),
		configMapEnvVar("TRUENAS_URL", ConfigMapName, "truenasURL", false),
		secretEnvVar("TRUENAS_API_KEY", csi.Spec.CredentialsSecret, "api-key"),
		configMapEnvVar("TRUENAS_DEFAULT_POOL", ConfigMapName, "defaultPool", false),
		configMapEnvVar("TRUENAS_NFS_SERVER", ConfigMapName, "nfsServer", true),
		configMapEnvVar("TRUENAS_ISCSI_PORTAL", ConfigMapName, "iscsiPortal", true),
		configMapEnvVar("TRUENAS_NVMEOF_PORTAL", ConfigMapName, "nvmeofPortal", true),
		configMapEnvVar("TRUENAS_ISCSI_IQN_BASE", ConfigMapName, "iscsiIQNBase", true),
		configMapEnvVar("TRUENAS_INSECURE_SKIP_VERIFY", ConfigMapName, "truenasInsecure", true),
	}
}

// fieldRefEnvVar creates an environment variable from a field reference
func fieldRefEnvVar(name, fieldPath string) corev1.EnvVar {
	return corev1.EnvVar{
		Name: name,
		ValueFrom: &corev1.EnvVarSource{
			FieldRef: &corev1.ObjectFieldSelector{FieldPath: fieldPath},
		},
	}
}

// configMapEnvVar creates an environment variable from a ConfigMap key
func configMapEnvVar(name, configMapName, key string, optional bool) corev1.EnvVar {
	return corev1.EnvVar{
		Name: name,
		ValueFrom: &corev1.EnvVarSource{
			ConfigMapKeyRef: &corev1.ConfigMapKeySelector{
				LocalObjectReference: corev1.LocalObjectReference{Name: configMapName},
				Key:                  key,
				Optional:             ptr.To(optional),
			},
		},
	}
}

// secretEnvVar creates an environment variable from a Secret key
func secretEnvVar(name, secretName, key string) corev1.EnvVar {
	return corev1.EnvVar{
		Name: name,
		ValueFrom: &corev1.EnvVarSource{
			SecretKeyRef: &corev1.SecretKeySelector{
				LocalObjectReference: corev1.LocalObjectReference{Name: secretName},
				Key:                  key,
			},
		},
	}
}

// SidecarConfig defines the configuration for building a sidecar container
type SidecarConfig struct {
	Name         string
	ImageEnvVar  string
	Args         []string
	VolumeMounts []corev1.VolumeMount
}

// buildSidecarContainer creates a sidecar container from configuration
func buildSidecarContainer(config SidecarConfig) corev1.Container {
	return corev1.Container{
		Name:            config.Name,
		Image:           getSidecarImage(config.ImageEnvVar),
		ImagePullPolicy: corev1.PullIfNotPresent,
		Args:            config.Args,
		VolumeMounts:    config.VolumeMounts,
	}
}

// getSidecarImage returns the sidecar image from environment variable.
// Unlike the old implementation, this does NOT fall back to hardcoded defaults.
// Sidecar images must be configured via environment variables.
func getSidecarImage(envVar string) string {
	return os.Getenv(envVar)
}

// mustParseQuantity parses a resource quantity, panicking on invalid input.
// Use only with compile-time constant strings.
func mustParseQuantity(s string) resource.Quantity {
	return resource.MustParse(s)
}

// socketDirVolumeMount returns the standard socket directory volume mount
func socketDirVolumeMount() corev1.VolumeMount {
	return corev1.VolumeMount{Name: VolumeSocketDir, MountPath: "/csi"}
}

// getDriverImage returns the driver image, using the default if not specified
func getDriverImage(csi *csiv1alpha1.TrueNASCSI) string {
	if csi.Spec.DriverImage != "" {
		return csi.Spec.DriverImage
	}
	return DefaultDriverImage
}

// getLogLevel returns the log level, using the default if not specified
func getLogLevel(csi *csiv1alpha1.TrueNASCSI) int32 {
	if csi.Spec.LogLevel > 0 {
		return csi.Spec.LogLevel
	}
	return DefaultLogLevel
}

// getControllerReplicas returns the controller replicas, using the default if not specified
func getControllerReplicas(csi *csiv1alpha1.TrueNASCSI) int32 {
	if csi.Spec.ControllerReplicas > 0 {
		return csi.Spec.ControllerReplicas
	}
	return DefaultControllerReplicas
}

// getNamespace returns the namespace for CSI components
func getNamespace(csi *csiv1alpha1.TrueNASCSI) string {
	if csi.Spec.Namespace != "" {
		return csi.Spec.Namespace
	}
	return CSINamespace
}

// extractImageTag extracts the tag from an image reference
func extractImageTag(image string) string {
	if idx := strings.LastIndex(image, ":"); idx != -1 {
		return image[idx+1:]
	}
	return "latest"
}
