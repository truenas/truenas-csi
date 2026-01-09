package sanity

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/iXsystems/truenas_k8_driver/pkg/driver"
	sanity "github.com/kubernetes-csi/csi-test/v5/pkg/sanity"
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
