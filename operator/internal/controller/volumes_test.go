package controller

import "testing"

// The driver's connector files record which protocol and target each staged volume
// uses. NodeUnstageVolume gets no publish context to fall back on, so this state has
// to be backed by a host path: keeping it in the container's writable layer means a
// csi-node restart strands every volume staged on that node.
func TestNodeConnectorDirIsHostBacked(t *testing.T) {
	var volume, mounted bool

	for _, v := range buildNodeVolumes() {
		if v.Name != VolumeConnectorDir {
			continue
		}
		if v.HostPath == nil {
			t.Fatalf("volume %q is not a host path", VolumeConnectorDir)
		}
		if v.HostPath.Path != HostPathConnectorDir {
			t.Errorf("volume %q host path = %q, want %q", VolumeConnectorDir, v.HostPath.Path, HostPathConnectorDir)
		}
		volume = true
	}
	if !volume {
		t.Errorf("node pod has no %q volume", VolumeConnectorDir)
	}

	for _, m := range buildNodeVolumeMounts() {
		if m.Name == VolumeConnectorDir {
			if m.MountPath != HostPathConnectorDir {
				t.Errorf("volume %q mount path = %q, want %q", VolumeConnectorDir, m.MountPath, HostPathConnectorDir)
			}
			mounted = true
		}
	}
	if !mounted {
		t.Errorf("node container does not mount %q", VolumeConnectorDir)
	}
}
