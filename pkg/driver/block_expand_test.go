package driver

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/go-logr/logr"
	"k8s.io/mount-utils"
	"k8s.io/utils/exec"
	testingexec "k8s.io/utils/exec/testing"
)

// partitionTableExec scripts an exec whose single command answers the way blkid does
// for a device a workload has partitioned. mount-utils reports a partition table as
// the sentinel format "unknown data, probably partitions", which its resizer has no
// case for and rejects outright.
func partitionTableExec() *testingexec.FakeExec {
	cmd := &testingexec.FakeCmd{
		CombinedOutputScript: []testingexec.FakeAction{
			func() ([]byte, []byte, error) { return []byte("DEVNAME=/dev/sda\nPTTYPE=gpt\n"), nil, nil },
		},
	}
	return &testingexec.FakeExec{
		CommandScript: []testingexec.FakeCommandAction{
			func(name string, args ...string) exec.Cmd { return testingexec.InitFakeCmd(cmd, name, args...) },
		},
	}
}

// A raw block volume is published as the device itself, and the volume capability on
// NodeExpandVolume is optional and carries no block access type from every CO, so the
// volume path has to be what decides. Getting this wrong sends a partitioned device to
// the filesystem resizer, which fails the expansion and with it the pod's MapVolume.
func TestIsBlockVolumeExpansion(t *testing.T) {
	dir := t.TempDir()

	mountPath := filepath.Join(dir, "globalmount")
	if err := os.Mkdir(mountPath, 0o750); err != nil {
		t.Fatalf("failed to create mount path: %v", err)
	}

	// Stands in for the block device bind-mounted over the published target file,
	// which a test cannot create without privileges. Both are non-directories.
	devicePath := filepath.Join(dir, "pvc-abc-device")
	if err := os.WriteFile(devicePath, nil, 0o600); err != nil {
		t.Fatalf("failed to create device path: %v", err)
	}

	blockCap := &csi.VolumeCapability{
		AccessType: &csi.VolumeCapability_Block{Block: &csi.VolumeCapability_BlockVolume{}},
	}
	mountCap := &csi.VolumeCapability{
		AccessType: &csi.VolumeCapability_Mount{Mount: &csi.VolumeCapability_MountVolume{FsType: DefaultFSType}},
	}
	accessModeOnly := &csi.VolumeCapability{
		AccessMode: &csi.VolumeCapability_AccessMode{Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER},
	}

	tests := []struct {
		name       string
		capability *csi.VolumeCapability
		path       string
		want       bool
	}{
		{"block capability", blockCap, devicePath, true},
		{"mount capability on a directory", mountCap, mountPath, false},
		{"no capability on a directory", nil, mountPath, false},
		{"no capability on a device", nil, devicePath, true},
		{"access mode only on a device", accessModeOnly, devicePath, true},
		// A block PersistentVolume still carries the StorageClass fsType, so a CO
		// may describe it with a mount capability. The published path does not lie.
		{"mount capability on a device", mountCap, devicePath, true},
		{"missing path", nil, filepath.Join(dir, "gone"), false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isBlockVolumeExpansion(tt.capability, tt.path); got != tt.want {
				t.Errorf("isBlockVolumeExpansion() = %v, want %v", got, tt.want)
			}
		})
	}
}

// Expanding a raw block volume must stop at the device rescan. Probing the device for
// a filesystem finds whatever the workload put there, and a partition table (every
// KubeVirt disk has one) makes the resizer fail, which strands the volume: the node
// expansion never completes and the pod cannot map the device again.
func TestISCSIExpand_BlockVolumeSkipsFilesystemResize(t *testing.T) {
	dir := useTempConnectorDir(t)
	const volumeID = "tank/pvc-abc"

	cpath := filepath.Join(dir, sanitizeISCSIVolumeID(volumeID)+".connector")
	connector := `{"volume_name":"pvc-abc","target_iqn":"iqn.2000-01.io.truenas:pvc-abc",` +
		`"target_portal":["10.0.0.1:3260"],"devices":[{"name":"sda"}],"mount_target_device":{"name":"sda"}}`
	if err := os.WriteFile(cpath, []byte(connector), 0o600); err != nil {
		t.Fatalf("failed to write connector file: %v", err)
	}

	req := &ExpandRequest{
		VolumeID:      volumeID,
		VolumePath:    "/var/lib/kubelet/plugins/kubernetes.io/csi/volumeDevices/publish/pvc-abc/abc",
		CapacityBytes: 11 * GiB,
		IsBlockVolume: true,
	}

	fake := partitionTableExec()
	handler := &ISCSIHandler{
		mounter: &mount.SafeFormatAndMount{Interface: mount.NewFakeMounter(nil), Exec: fake},
		resizer: mount.NewResizeFs(fake),
		log:     logr.Discard(),
	}

	result, err := handler.Expand(context.Background(), req)
	if err != nil {
		t.Fatalf("Expand() = %v, want nil", err)
	}
	if result.CapacityBytes != req.CapacityBytes {
		t.Errorf("CapacityBytes = %d, want %d", result.CapacityBytes, req.CapacityBytes)
	}
	if fake.CommandCalls != 0 {
		t.Errorf("the device was probed for a filesystem %d time(s), want none", fake.CommandCalls)
	}
}

// The same partition table on a filesystem volume is a genuine failure and must stay
// one: reporting success would leave the filesystem short of the expanded device.
func TestISCSIExpand_FilesystemVolumeStillResizes(t *testing.T) {
	dir := useTempConnectorDir(t)
	const volumeID = "tank/pvc-abc"

	cpath := filepath.Join(dir, sanitizeISCSIVolumeID(volumeID)+".connector")
	connector := `{"volume_name":"pvc-abc","target_iqn":"iqn.2000-01.io.truenas:pvc-abc",` +
		`"target_portal":["10.0.0.1:3260"],"devices":[{"name":"sda"}],"mount_target_device":{"name":"sda"}}`
	if err := os.WriteFile(cpath, []byte(connector), 0o600); err != nil {
		t.Fatalf("failed to write connector file: %v", err)
	}

	fake := partitionTableExec()
	handler := &ISCSIHandler{
		mounter: &mount.SafeFormatAndMount{Interface: mount.NewFakeMounter(nil), Exec: fake},
		resizer: mount.NewResizeFs(fake),
		log:     logr.Discard(),
	}

	_, err := handler.Expand(context.Background(), &ExpandRequest{
		VolumeID:      volumeID,
		VolumePath:    "/var/lib/kubelet/plugins/kubernetes.io/csi/csi.truenas.io/abc/globalmount",
		CapacityBytes: 11 * GiB,
	})
	if err == nil {
		t.Fatal("Expand() of a filesystem volume the resizer cannot grow should fail")
	}
	if !strings.Contains(err.Error(), "resize") {
		t.Errorf("Expand() = %v, want a resize failure", err)
	}
	if fake.CommandCalls != 1 {
		t.Errorf("the device was probed %d time(s), want once", fake.CommandCalls)
	}
}

// NVMe-oF block volumes reach the resizer through the same expand path as iSCSI.
func TestNVMeOFExpand_BlockVolumeSkipsFilesystemResize(t *testing.T) {
	useTempConnectorDir(t)
	const volumeID = "tank/pvc-abc"

	info := nvmeConnectorInfo{
		VolumeID:      volumeID,
		SubNQN:        "nqn.2011-06.com.truenas:csi-pvc-abc",
		NamespaceUUID: "e1f2a3b4-0000-1111-2222-333344445555",
		DevicePath:    "/dev/nvme0n1",
	}
	data, err := json.Marshal(info)
	if err != nil {
		t.Fatalf("failed to marshal connector: %v", err)
	}
	if err := os.WriteFile(nvmeConnectorPath(volumeID), data, 0o600); err != nil {
		t.Fatalf("failed to write connector file: %v", err)
	}

	calls := mockNVMeExec(t)
	fake := partitionTableExec()
	handler := &NVMeOFHandler{
		mounter: &mount.SafeFormatAndMount{Interface: mount.NewFakeMounter(nil), Exec: fake},
		resizer: mount.NewResizeFs(fake),
		log:     logr.Discard(),
	}

	req := &ExpandRequest{
		VolumeID:      volumeID,
		VolumePath:    "/var/lib/kubelet/plugins/kubernetes.io/csi/volumeDevices/publish/pvc-abc/abc",
		CapacityBytes: 11 * GiB,
		IsBlockVolume: true,
	}

	result, err := handler.Expand(context.Background(), req)
	if err != nil {
		t.Fatalf("Expand() = %v, want nil", err)
	}
	if result.CapacityBytes != req.CapacityBytes {
		t.Errorf("CapacityBytes = %d, want %d", result.CapacityBytes, req.CapacityBytes)
	}
	if fake.CommandCalls != 0 {
		t.Errorf("the device was probed for a filesystem %d time(s), want none", fake.CommandCalls)
	}

	// The namespace rescan is the part a block volume does need: without it the
	// workload keeps seeing the old size.
	var rescanned bool
	for _, c := range *calls {
		if c.name == "nvme" && hasArg(c.args, "ns-rescan") {
			rescanned = true
		}
	}
	if !rescanned {
		t.Error("the NVMe namespace was not rescanned, the new size stays invisible")
	}
}
