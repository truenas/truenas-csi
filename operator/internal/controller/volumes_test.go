package controller

import (
	"strings"
	"testing"
)

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

// The postStart hook's shim enters the host mount namespace and must look iscsiadm
// up on PATH. Naming an absolute path makes execvp skip the search, so hosts that
// install iscsiadm anywhere but /usr/sbin (Talos uses /usr/local/sbin) fail every
// iSCSI login with a bare exit 127.
func TestNodePostStartResolvesHostISCSIAdmOnPath(t *testing.T) {
	command := buildNodePostStartCommand()
	if len(command) != 3 {
		t.Fatalf("postStart command = %v, want [/bin/sh -c script]", command)
	}
	script := command[2]

	nsenter := "nsenter --mount=" + HostMountNamespace + " -- "
	if !strings.Contains(script, nsenter+ISCSIAdmBinary+` "$@"`) {
		t.Errorf("shim does not invoke the host's iscsiadm by name:\n%s", script)
	}
	if strings.Contains(script, nsenter+ContainerISCSIAdmPath) {
		t.Errorf("shim names an absolute host path for iscsiadm:\n%s", script)
	}
	if !strings.Contains(script, "export PATH="+HostBinarySearchPath) {
		t.Errorf("shim does not set the host binary search path:\n%s", script)
	}

	// The container's own iscsiadm is still backed up and replaced by the shim, and
	// the shim being in place is what the hook succeeds or fails on.
	for _, want := range []string{
		"mv " + ContainerISCSIAdmPath + " " + ContainerISCSIAdmPath + ".orig",
		"> " + ContainerISCSIAdmPath + " && chmod +x " + ContainerISCSIAdmPath,
		"test -x " + ContainerISCSIAdmPath,
	} {
		if !strings.Contains(script, want) {
			t.Errorf("postStart script is missing %q:\n%s", want, script)
		}
	}

	// NVMe-oF is optional, so its modprobes must not decide the hook's verdict.
	for _, mod := range []string{"nvme_tcp", "nvme_fabrics"} {
		if !strings.Contains(script, "modprobe "+mod+" 2>/dev/null || true") {
			t.Errorf("modprobe of %s is not tolerant of failure:\n%s", mod, script)
		}
	}
}
