package controller

import (
	corev1 "k8s.io/api/core/v1"
)

// buildControllerVolumes returns the volumes for the controller deployment
func buildControllerVolumes() []corev1.Volume {
	return []corev1.Volume{
		emptyDirVolume(VolumeSocketDir),
	}
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
