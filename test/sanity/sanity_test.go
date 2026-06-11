package sanity

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	csi "github.com/container-storage-interface/spec/lib/go/csi"
	sanity "github.com/kubernetes-csi/csi-test/v5/pkg/sanity"
	"github.com/truenas/truenas-csi/pkg/driver"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"k8s.io/klog/v2/textlogger"
)

const (
	// Test timeouts
	driverStartTimeout = 30 * time.Second

	// Default test configuration
	defaultTestPool = "tank"
)

// TestSanity runs the CSI sanity test suite against the TrueNAS CSI driver.
// This is an integration test that requires a real TrueNAS instance.
//
// Required environment variables:
//   - TRUENAS_URL: WebSocket URL (e.g., wss://10.0.0.1/api/current)
//   - TRUENAS_API_KEY: API key for authentication
//   - TRUENAS_DEFAULT_POOL: Storage pool to use (default: tank)
//   - TRUENAS_NFS_SERVER: NFS server IP/hostname
//   - TRUENAS_ISCSI_PORTAL: iSCSI portal (e.g., 10.0.0.1:3260)
//
// Optional environment variables:
//   - TRUENAS_INSECURE_SKIP_VERIFY: Set to "true" for self-signed certs
//   - TRUENAS_ISCSI_IQN_BASE: Custom IQN prefix
//   - TRUENAS_NVMEOF_PORTAL: NVMe-oF portal (e.g., 10.0.0.1:4420); auto-derived if unset
//
// Run with: go test -v -tags=integration ./test/sanity/...
func TestSanity(t *testing.T) {
	// Skip if not running integration tests
	if os.Getenv("TRUENAS_URL") == "" {
		t.Skip("Skipping sanity test: TRUENAS_URL not set. Set environment variables to run integration tests.")
	}

	// Create temporary directories for the test
	tmpDir, err := os.MkdirTemp("", "csi-sanity-")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	endpoint := filepath.Join(tmpDir, "csi.sock")
	targetPath := filepath.Join(tmpDir, "target")
	stagingPath := filepath.Join(tmpDir, "staging")

	// Build driver configuration from environment
	config := buildTestConfig(endpoint)

	// Start the driver
	drv, err := driver.NewDriver(config)
	if err != nil {
		t.Fatalf("Failed to create driver: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start driver in background
	errCh := make(chan error, 1)
	go func() {
		errCh <- drv.Run(ctx)
	}()

	// Wait for driver to be ready
	if err := waitForSocket(endpoint, driverStartTimeout); err != nil {
		t.Fatalf("Driver failed to start: %v", err)
	}

	// Configure sanity test
	sanityConfig := sanity.NewTestConfig()
	sanityConfig.Address = "unix://" + endpoint
	sanityConfig.TargetPath = targetPath
	sanityConfig.StagingPath = stagingPath
	sanityConfig.SecretsFile = "" // No secrets file needed

	// Set test parameters
	sanityConfig.TestVolumeSize = 1 * 1024 * 1024 * 1024 // 1 GiB
	sanityConfig.TestVolumeParametersFile = ""

	// Create parameters for NFS volumes (simpler to test)
	sanityConfig.TestVolumeParameters = map[string]string{
		"protocol": "nfs",
	}

	// Run sanity tests
	sanity.Test(t, sanityConfig)

	// Stop driver
	cancel()
	drv.Stop()

	// Check for driver errors
	select {
	case err := <-errCh:
		if err != nil && err != context.Canceled {
			t.Errorf("Driver error: %v", err)
		}
	default:
	}
}

// TestSanityISCSI runs sanity tests specifically for iSCSI volumes.
// Requires privileged access for iSCSI operations.
func TestSanityISCSI(t *testing.T) {
	if os.Getenv("TRUENAS_URL") == "" {
		t.Skip("Skipping iSCSI sanity test: TRUENAS_URL not set")
	}

	// iSCSI tests require root/privileged access
	if os.Geteuid() != 0 {
		t.Skip("Skipping iSCSI sanity test: requires root privileges")
	}

	tmpDir, err := os.MkdirTemp("", "csi-sanity-iscsi-")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	endpoint := filepath.Join(tmpDir, "csi.sock")
	targetPath := filepath.Join(tmpDir, "target")
	stagingPath := filepath.Join(tmpDir, "staging")

	config := buildTestConfig(endpoint)

	drv, err := driver.NewDriver(config)
	if err != nil {
		t.Fatalf("Failed to create driver: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- drv.Run(ctx)
	}()

	if err := waitForSocket(endpoint, driverStartTimeout); err != nil {
		t.Fatalf("Driver failed to start: %v", err)
	}

	sanityConfig := sanity.NewTestConfig()
	sanityConfig.Address = "unix://" + endpoint
	sanityConfig.TargetPath = targetPath
	sanityConfig.StagingPath = stagingPath
	sanityConfig.TestVolumeSize = 1 * 1024 * 1024 * 1024

	// iSCSI parameters
	sanityConfig.TestVolumeParameters = map[string]string{
		"protocol": "iscsi",
	}

	sanity.Test(t, sanityConfig)

	cancel()
	drv.Stop()
}

// TestSanityNVMeOF runs sanity tests specifically for NVMe-oF/TCP volumes.
// Requires privileged access (nvme connect / kernel modules) and TrueNAS 25.10+.
func TestSanityNVMeOF(t *testing.T) {
	if os.Getenv("TRUENAS_URL") == "" {
		t.Skip("Skipping NVMe-oF sanity test: TRUENAS_URL not set")
	}

	// NVMe-oF tests require root/privileged access for nvme connect.
	if os.Geteuid() != 0 {
		t.Skip("Skipping NVMe-oF sanity test: requires root privileges")
	}

	tmpDir, err := os.MkdirTemp("", "csi-sanity-nvmeof-")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	endpoint := filepath.Join(tmpDir, "csi.sock")
	targetPath := filepath.Join(tmpDir, "target")
	stagingPath := filepath.Join(tmpDir, "staging")

	config := buildTestConfig(endpoint)

	drv, err := driver.NewDriver(config)
	if err != nil {
		t.Fatalf("Failed to create driver: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- drv.Run(ctx)
	}()

	if err := waitForSocket(endpoint, driverStartTimeout); err != nil {
		t.Fatalf("Driver failed to start: %v", err)
	}

	sanityConfig := sanity.NewTestConfig()
	sanityConfig.Address = "unix://" + endpoint
	sanityConfig.TargetPath = targetPath
	sanityConfig.StagingPath = stagingPath
	sanityConfig.TestVolumeSize = 1 * 1024 * 1024 * 1024

	// NVMe-oF parameters
	sanityConfig.TestVolumeParameters = map[string]string{
		"protocol": "nvmeof",
	}

	sanity.Test(t, sanityConfig)

	cancel()
	drv.Stop()
}

// TestSanityNVMeOFDHCHAP runs the sanity suite for NVMe-oF with DH-CHAP auth.
// Requires root and TrueNAS 25.10+. Set TRUENAS_TEST_NVMEOF_HOSTNQN and
// TRUENAS_TEST_NVMEOF_DHCHAP_KEY (optionally TRUENAS_TEST_NVMEOF_DHCHAP_CTRL_KEY
// for mutual auth). Generate a key with: nvme gen-dhchap-key
func TestSanityNVMeOFDHCHAP(t *testing.T) {
	if os.Getenv("TRUENAS_URL") == "" {
		t.Skip("Skipping NVMe-oF DH-CHAP sanity test: TRUENAS_URL not set")
	}
	if os.Geteuid() != 0 {
		t.Skip("Skipping NVMe-oF DH-CHAP sanity test: requires root privileges")
	}
	hostNQN := os.Getenv("TRUENAS_TEST_NVMEOF_HOSTNQN")
	dhchapKey := os.Getenv("TRUENAS_TEST_NVMEOF_DHCHAP_KEY")
	if hostNQN == "" || dhchapKey == "" {
		t.Skip("Skipping NVMe-oF DH-CHAP sanity test: set TRUENAS_TEST_NVMEOF_HOSTNQN and TRUENAS_TEST_NVMEOF_DHCHAP_KEY")
	}

	tmpDir, err := os.MkdirTemp("", "csi-sanity-nvmeof-dhchap-")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	endpoint := filepath.Join(tmpDir, "csi.sock")
	targetPath := filepath.Join(tmpDir, "target")
	stagingPath := filepath.Join(tmpDir, "staging")

	config := buildTestConfig(endpoint)

	drv, err := driver.NewDriver(config)
	if err != nil {
		t.Fatalf("Failed to create driver: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- drv.Run(ctx)
	}()

	if err := waitForSocket(endpoint, driverStartTimeout); err != nil {
		t.Fatalf("Driver failed to start: %v", err)
	}

	sanityConfig := sanity.NewTestConfig()
	sanityConfig.Address = "unix://" + endpoint
	sanityConfig.TargetPath = targetPath
	sanityConfig.StagingPath = stagingPath
	sanityConfig.TestVolumeSize = 1 * 1024 * 1024 * 1024

	params := map[string]string{
		"protocol":         "nvmeof",
		"nvmeof.hostNQN":   hostNQN,
		"nvmeof.dhchapKey": dhchapKey,
	}
	if ctrl := os.Getenv("TRUENAS_TEST_NVMEOF_DHCHAP_CTRL_KEY"); ctrl != "" {
		params["nvmeof.dhchapCtrlKey"] = ctrl
	}
	sanityConfig.TestVolumeParameters = params

	sanity.Test(t, sanityConfig)

	cancel()
	drv.Stop()
}

// TestNVMeOFBlockVolume exercises the raw block-volume path (not covered by the
// mount-based sanity suite): provision an NVMe-oF volume with Block access type,
// stage + publish it as a raw device, and verify the published target is a real
// block device that accepts read/write. Requires root and TrueNAS 25.10+.
func TestNVMeOFBlockVolume(t *testing.T) {
	if os.Getenv("TRUENAS_URL") == "" {
		t.Skip("Skipping NVMe-oF block test: TRUENAS_URL not set")
	}
	if os.Geteuid() != 0 {
		t.Skip("Skipping NVMe-oF block test: requires root privileges")
	}

	tmpDir, err := os.MkdirTemp("", "csi-nvmeof-block-")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	endpoint := filepath.Join(tmpDir, "csi.sock")
	stagingPath := filepath.Join(tmpDir, "staging")
	targetPath := filepath.Join(tmpDir, "block-target") // a file for block bind-mount

	config := buildTestConfig(endpoint)
	drv, err := driver.NewDriver(config)
	if err != nil {
		t.Fatalf("Failed to create driver: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = drv.Run(ctx) }()
	if err := waitForSocket(endpoint, driverStartTimeout); err != nil {
		t.Fatalf("Driver failed to start: %v", err)
	}
	defer drv.Stop()

	conn, err := grpc.NewClient("unix://"+endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("Failed to dial driver: %v", err)
	}
	defer conn.Close()
	cc := csi.NewControllerClient(conn)
	nc := csi.NewNodeClient(conn)

	tctx := context.Background()
	blockCap := &csi.VolumeCapability{
		AccessType: &csi.VolumeCapability_Block{Block: &csi.VolumeCapability_BlockVolume{}},
		AccessMode: &csi.VolumeCapability_AccessMode{Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER},
	}

	ni, err := nc.NodeGetInfo(tctx, &csi.NodeGetInfoRequest{})
	if err != nil {
		t.Fatalf("NodeGetInfo: %v", err)
	}

	cv, err := cc.CreateVolume(tctx, &csi.CreateVolumeRequest{
		Name:               "sanity-nvmeof-block",
		CapacityRange:      &csi.CapacityRange{RequiredBytes: 1 << 30},
		VolumeCapabilities: []*csi.VolumeCapability{blockCap},
		Parameters:         map[string]string{"protocol": "nvmeof"},
	})
	if err != nil {
		t.Fatalf("CreateVolume(block): %v", err)
	}
	volID := cv.Volume.VolumeId
	volCtx := cv.Volume.VolumeContext
	defer func() { _, _ = cc.DeleteVolume(tctx, &csi.DeleteVolumeRequest{VolumeId: volID}) }()

	cp, err := cc.ControllerPublishVolume(tctx, &csi.ControllerPublishVolumeRequest{
		VolumeId: volID, NodeId: ni.NodeId, VolumeCapability: blockCap,
	})
	if err != nil {
		t.Fatalf("ControllerPublishVolume: %v", err)
	}
	defer func() {
		_, _ = cc.ControllerUnpublishVolume(tctx, &csi.ControllerUnpublishVolumeRequest{VolumeId: volID, NodeId: ni.NodeId})
	}()

	if _, err := nc.NodeStageVolume(tctx, &csi.NodeStageVolumeRequest{
		VolumeId: volID, StagingTargetPath: stagingPath, VolumeCapability: blockCap,
		PublishContext: cp.PublishContext, VolumeContext: volCtx,
	}); err != nil {
		t.Fatalf("NodeStageVolume(block): %v", err)
	}
	defer func() {
		_, _ = nc.NodeUnstageVolume(tctx, &csi.NodeUnstageVolumeRequest{VolumeId: volID, StagingTargetPath: stagingPath})
	}()

	if _, err := nc.NodePublishVolume(tctx, &csi.NodePublishVolumeRequest{
		VolumeId: volID, StagingTargetPath: stagingPath, TargetPath: targetPath,
		VolumeCapability: blockCap, PublishContext: cp.PublishContext, VolumeContext: volCtx,
	}); err != nil {
		t.Fatalf("NodePublishVolume(block): %v", err)
	}
	defer func() {
		_, _ = nc.NodeUnpublishVolume(tctx, &csi.NodeUnpublishVolumeRequest{VolumeId: volID, TargetPath: targetPath})
	}()

	// The published target must be a real block device.
	fi, err := os.Stat(targetPath)
	if err != nil {
		t.Fatalf("stat block target: %v", err)
	}
	if fi.Mode()&os.ModeDevice == 0 || fi.Mode()&os.ModeCharDevice != 0 {
		t.Fatalf("target %s is not a block device (mode=%v)", targetPath, fi.Mode())
	}

	// Write and read back a page to prove the raw device is usable (page-sized
	// buffer avoids block-device alignment constraints).
	const pageSize = 4096
	want := make([]byte, pageSize)
	copy(want, []byte("truenas-csi-nvmeof-block-readwrite-check"))
	f, err := os.OpenFile(targetPath, os.O_RDWR, 0)
	if err != nil {
		t.Fatalf("open block device: %v", err)
	}
	defer f.Close()
	if _, err := f.WriteAt(want, 0); err != nil {
		t.Fatalf("write to block device: %v", err)
	}
	_ = f.Sync()
	got := make([]byte, pageSize)
	if _, err := f.ReadAt(got, 0); err != nil {
		t.Fatalf("read from block device: %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("block device read-back mismatch")
	}

	t.Logf("NVMe-oF block volume OK: %s is a block device, write/read verified", targetPath)
}

// buildTestConfig creates a DriverConfig from environment variables.
func buildTestConfig(endpoint string) *driver.DriverConfig {
	pool := os.Getenv("TRUENAS_DEFAULT_POOL")
	if pool == "" {
		pool = defaultTestPool
	}

	config := &driver.DriverConfig{
		NodeID:        "sanity-test-node",
		Endpoint:      "unix://" + endpoint,
		TrueNASURL:    os.Getenv("TRUENAS_URL"),
		TrueNASAPIKey: os.Getenv("TRUENAS_API_KEY"),
		DefaultPool:   pool,
		NFSServer:     os.Getenv("TRUENAS_NFS_SERVER"),
		ISCSIPortal:   os.Getenv("TRUENAS_ISCSI_PORTAL"),
		ISCSIIQNBase:  os.Getenv("TRUENAS_ISCSI_IQN_BASE"),
		NVMeOFPortal:  os.Getenv("TRUENAS_NVMEOF_PORTAL"),
		Logger:        textlogger.NewLogger(textlogger.NewConfig()),
	}

	if os.Getenv("TRUENAS_INSECURE_SKIP_VERIFY") == "true" {
		config.TrueNASInsecure = true
	}

	return config
}

// waitForSocket waits for the Unix socket to become available.
func waitForSocket(socketPath string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)

	for time.Now().Before(deadline) {
		if _, err := os.Stat(socketPath); err == nil {
			return nil
		}
		time.Sleep(100 * time.Millisecond)
	}

	return fmt.Errorf("socket %s not ready after %v", socketPath, timeout)
}
