package controller

import (
	"context"

	csiv1alpha1 "github.com/truenas/truenas-csi/operator/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
)

// buildControllerVolumes returns the volumes for the controller deployment
func buildControllerVolumes(csi *csiv1alpha1.TrueNASCSI) []corev1.Volume {
	volumes := []corev1.Volume{
		emptyDirVolume(VolumeSocketDir),
	}
	// If a RootCertificateBundle is specified, add a volume for it
	if csi.Spec.RootCertificateBundle.Name != "" {
		// Default key is "ca-bundle.crt" if not specified
		key := "ca-bundle.crt"
		path := "ca-bundle.crt"

		// Detect if running on OpenShift to determine the correct mount path and filename
		// The assumption is that on OpenShift the UBI-based image is being run
		detectOpenShift, err := checkOpenShift(context.Background())
		if err != nil {
			detectOpenShift = false
		}

		if csi.Spec.RootCertificateBundle.Key != "" {
			key = csi.Spec.RootCertificateBundle.Key
		}
		if detectOpenShift {
			path = UBIRootCertFilename
		} else {
			path = DistrolessRootCertFilename
		}

		volumes = append(volumes, corev1.Volume{
			Name: SharedRootCAVolumeName,
			VolumeSource: corev1.VolumeSource{
				ConfigMap: &corev1.ConfigMapVolumeSource{
					LocalObjectReference: corev1.LocalObjectReference{Name: csi.Spec.RootCertificateBundle.Name},
					Items: []corev1.KeyToPath{
						{Key: key, Path: path},
					},
				},
			},
		})
	}
	return volumes
}

// buildNodeVolumes returns the volumes for the node daemonset
func buildNodeVolumes() []corev1.Volume {
	hostPathDirectory := corev1.HostPathDirectory
	hostPathDirectoryOrCreate := corev1.HostPathDirectoryOrCreate
	hostPathFileOrCreate := corev1.HostPathFileOrCreate

	return []corev1.Volume{
		hostPathVolume(VolumeRegistrationDir, HostPathRegistrationDir, &hostPathDirectoryOrCreate),
		hostPathVolume(VolumePluginDir, HostPathPluginDir, &hostPathDirectoryOrCreate),
		hostPathVolume(VolumeKubeletDir, HostPathKubeletDir, &hostPathDirectory),
		hostPathVolume(VolumeDeviceDir, HostPathDeviceDir, nil),
		hostPathVolume(VolumeModulesDir, HostPathModulesDir, &hostPathDirectory),
		hostPathVolume(VolumeISCSIDir, HostPathISCSIDir, &hostPathDirectory),
		hostPathVolume(VolumeISCSILib, HostPathISCSILib, &hostPathDirectoryOrCreate),
		hostPathVolume(VolumeConnectorDir, HostPathConnectorDir, &hostPathDirectoryOrCreate),
		hostPathVolume(VolumeHostRoot, HostPathRoot, &hostPathDirectory),
		hostPathVolume(VolumeSocketDir, HostPathPluginDir, &hostPathDirectoryOrCreate),
		hostPathVolume(VolumeHostFstab, HostPathFstab, &hostPathFileOrCreate),
	}
}

// emptyDirVolume creates an EmptyDir volume
func emptyDirVolume(name string) corev1.Volume {
	return corev1.Volume{
		Name: name,
		VolumeSource: corev1.VolumeSource{
			EmptyDir: &corev1.EmptyDirVolumeSource{},
		},
	}
}

// hostPathVolume creates a HostPath volume
func hostPathVolume(name, path string, pathType *corev1.HostPathType) corev1.Volume {
	vol := corev1.Volume{
		Name: name,
		VolumeSource: corev1.VolumeSource{
			HostPath: &corev1.HostPathVolumeSource{
				Path: path,
			},
		},
	}
	if pathType != nil {
		vol.VolumeSource.HostPath.Type = pathType
	}
	return vol
}

// buildNodeVolumeMounts returns the volume mounts for the node container
func buildNodeVolumeMounts() []corev1.VolumeMount {
	mountPropagationBidirectional := corev1.MountPropagationBidirectional

	return []corev1.VolumeMount{
		{Name: VolumePluginDir, MountPath: "/csi"},
		{Name: VolumeKubeletDir, MountPath: "/var/lib/kubelet", MountPropagation: &mountPropagationBidirectional},
		{Name: VolumeDeviceDir, MountPath: "/dev"},
		{Name: VolumeModulesDir, MountPath: "/lib/modules", ReadOnly: true},
		{Name: VolumeISCSIDir, MountPath: "/etc/iscsi", MountPropagation: &mountPropagationBidirectional},
		{Name: VolumeISCSILib, MountPath: "/var/lib/iscsi", MountPropagation: &mountPropagationBidirectional},
		{Name: VolumeConnectorDir, MountPath: HostPathConnectorDir},
		{Name: VolumeHostRoot, MountPath: "/host", MountPropagation: &mountPropagationBidirectional},
		{Name: VolumeHostFstab, MountPath: "/etc/fstab"},
	}
}

// buildNodeDriverRegistrarVolumeMounts returns the volume mounts for the node driver registrar
func buildNodeDriverRegistrarVolumeMounts() []corev1.VolumeMount {
	return []corev1.VolumeMount{
		{Name: VolumePluginDir, MountPath: "/csi"},
		{Name: VolumeRegistrationDir, MountPath: "/registration"},
	}
}
