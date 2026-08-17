//go:build integration

package client

import (
	"context"
	"crypto/tls"
	"fmt"
	"os"
	"testing"
	"time"
)

// Integration test configuration from environment variables:
// - TRUENAS_URL: WebSocket URL to TrueNAS API (required)
// - TRUENAS_API_KEY: API key for authentication (required)
// - TRUENAS_TEST_POOL: Pool to use for tests (default: "tank")
// - TRUENAS_INSECURE_SKIP_VERIFY: Skip TLS verification (default: "true")

// Shared client for all integration tests (set up in TestMain)
var (
	sharedClient *Client
	testPool     string
)

// TestMain sets up a shared client connection for all integration tests.
// This avoids TrueNAS rate limiting on authentication by reusing one connection.
func TestMain(m *testing.M) {
	// Check required environment variables
	url := os.Getenv("TRUENAS_URL")
	apiKey := os.Getenv("TRUENAS_API_KEY")

	if url == "" || apiKey == "" {
		fmt.Println("TRUENAS_URL and TRUENAS_API_KEY must be set for integration tests")
		os.Exit(1)
	}

	// Set up test pool
	testPool = os.Getenv("TRUENAS_TEST_POOL")
	if testPool == "" {
		testPool = "tank"
	}

	// Configure TLS
	insecure := os.Getenv("TRUENAS_INSECURE_SKIP_VERIFY") != "false"
	var tlsConfig *tls.Config
	if insecure {
		tlsConfig = &tls.Config{InsecureSkipVerify: true}
	}

	// Create and connect shared client
	sharedClient = New(Config{
		URL:          url,
		APIKey:       apiKey,
		TLSConfig:    tlsConfig,
		CallTimeout:  30 * time.Second,
		PingInterval: 1 * time.Hour, // Disable during tests
	})

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	err := sharedClient.Connect(ctx)
	cancel()

	if err != nil {
		fmt.Printf("Failed to connect to TrueNAS: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("Connected to TrueNAS, running integration tests with pool: %s\n", testPool)

	// Run tests
	code := m.Run()

	// Cleanup
	sharedClient.Close()

	os.Exit(code)
}

// getTestClient returns the shared client for integration tests.
func getTestClient(t *testing.T) *Client {
	t.Helper()
	if sharedClient == nil || !sharedClient.Connected() {
		t.Fatal("shared client not connected")
	}
	return sharedClient
}

// getTestPool returns the pool name for integration tests.
func getTestPool(t *testing.T) string {
	t.Helper()
	return testPool
}

// testDatasetName generates a unique dataset name for testing.
func testDatasetName(pool, suffix string) string {
	return fmt.Sprintf("%s/csi-test-%d-%s", pool, time.Now().UnixNano(), suffix)
}

// =============================================================================
// Connection Tests
// =============================================================================

func TestIntegration_Connect(t *testing.T) {
	// This test creates its own client to test the connection process
	url := os.Getenv("TRUENAS_URL")
	apiKey := os.Getenv("TRUENAS_API_KEY")
	insecure := os.Getenv("TRUENAS_INSECURE_SKIP_VERIFY") != "false"

	var tlsConfig *tls.Config
	if insecure {
		tlsConfig = &tls.Config{InsecureSkipVerify: true}
	}

	client := New(Config{
		URL:          url,
		APIKey:       apiKey,
		TLSConfig:    tlsConfig,
		CallTimeout:  30 * time.Second,
		PingInterval: 1 * time.Hour,
	})
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	err := client.Connect(ctx)
	if err != nil {
		t.Fatalf("Connect failed: %v", err)
	}

	if !client.Connected() {
		t.Error("Expected client to be connected")
	}
}

func TestIntegration_Ping(t *testing.T) {
	client := getTestClient(t)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err := client.Ping(ctx)
	if err != nil {
		t.Fatalf("Ping failed: %v", err)
	}
}

// The keepalive loop pings with an API call so it also exercises the session, not
// just the socket. Verify the appliance accepts those pings and the connection
// survives several of them.
func TestIntegration_KeepAlive(t *testing.T) {
	url := os.Getenv("TRUENAS_URL")
	apiKey := os.Getenv("TRUENAS_API_KEY")
	insecure := os.Getenv("TRUENAS_INSECURE_SKIP_VERIFY") != "false"

	var tlsConfig *tls.Config
	if insecure {
		tlsConfig = &tls.Config{InsecureSkipVerify: true}
	}

	const pingInterval = 1 * time.Second

	client := New(Config{
		URL:          url,
		APIKey:       apiKey,
		TLSConfig:    tlsConfig,
		CallTimeout:  30 * time.Second,
		PingInterval: pingInterval,
	})
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := client.Connect(ctx); err != nil {
		t.Fatalf("Connect failed: %v", err)
	}

	// Let several keepalive pings run. A failing ping would drop the connection.
	time.Sleep(3*pingInterval + 500*time.Millisecond)

	if !client.Connected() {
		t.Fatal("connection did not survive the keepalive pings")
	}

	if err := client.Ping(ctx); err != nil {
		t.Fatalf("call after keepalive pings failed: %v", err)
	}
}

// =============================================================================
// Dataset Tests
// =============================================================================

func TestIntegration_DatasetCRUD(t *testing.T) {
	client := getTestClient(t)
	pool := getTestPool(t)
	ctx := context.Background()

	datasetName := testDatasetName(pool, "crud")

	// Cleanup on test completion
	t.Cleanup(func() {
		client.DeleteDataset(ctx, datasetName, &DatasetDeleteOptions{
			Recursive: true,
			Force:     true,
		})
	})

	// CREATE
	t.Run("Create", func(t *testing.T) {
		opts := &DatasetCreateOptions{
			Name:     datasetName,
			RefQuota: 1073741824, // 1 GB
			Comments: "CSI integration test dataset",
		}
		dataset, err := client.CreateDataset(ctx, opts)
		if err != nil {
			t.Fatalf("CreateDataset failed: %v", err)
		}
		if dataset.ID != datasetName {
			t.Errorf("Expected ID %s, got %s", datasetName, dataset.ID)
		}
	})

	// READ
	t.Run("Get", func(t *testing.T) {
		dataset, err := client.GetDataset(ctx, datasetName)
		if err != nil {
			t.Fatalf("GetDataset failed: %v", err)
		}
		if dataset.ID != datasetName {
			t.Errorf("Expected ID %s, got %s", datasetName, dataset.ID)
		}
		if dataset.RefQuota != 1073741824 {
			t.Errorf("Expected RefQuota 1073741824, got %d", dataset.RefQuota)
		}
	})

	// UPDATE
	t.Run("Update", func(t *testing.T) {
		newQuota := int64(2147483648) // 2 GB
		updates := &DatasetUpdateOptions{
			RefQuota: &newQuota,
		}
		err := client.UpdateDataset(ctx, datasetName, updates)
		if err != nil {
			t.Fatalf("UpdateDataset failed: %v", err)
		}

		// Verify update
		dataset, err := client.GetDataset(ctx, datasetName)
		if err != nil {
			t.Fatalf("GetDataset after update failed: %v", err)
		}
		if dataset.RefQuota != newQuota {
			t.Errorf("Expected RefQuota %d, got %d", newQuota, dataset.RefQuota)
		}
	})

	// DELETE
	t.Run("Delete", func(t *testing.T) {
		err := client.DeleteDataset(ctx, datasetName, &DatasetDeleteOptions{
			Recursive: true,
			Force:     true,
		})
		if err != nil {
			t.Fatalf("DeleteDataset failed: %v", err)
		}

		// Verify deletion
		_, err = client.GetDataset(ctx, datasetName)
		if err == nil {
			t.Error("Expected error getting deleted dataset")
		}
		if !IsNotFoundError(err) {
			t.Errorf("Expected not found error, got: %v", err)
		}
	})
}

func TestIntegration_DatasetZVOL(t *testing.T) {
	client := getTestClient(t)
	pool := getTestPool(t)
	ctx := context.Background()

	zvolName := testDatasetName(pool, "zvol")

	t.Cleanup(func() {
		client.DeleteDataset(ctx, zvolName, &DatasetDeleteOptions{
			Recursive: true,
			Force:     true,
		})
	})

	// Create ZVOL
	opts := &DatasetCreateOptions{
		Name:    zvolName,
		Type:    "VOLUME",
		Volsize: 1073741824, // 1 GB
	}
	dataset, err := client.CreateDataset(ctx, opts)
	if err != nil {
		t.Fatalf("CreateDataset (ZVOL) failed: %v", err)
	}

	if dataset.Type != "VOLUME" {
		t.Errorf("Expected Type VOLUME, got %s", dataset.Type)
	}
	if dataset.Volsize != 1073741824 {
		t.Errorf("Expected Volsize 1073741824, got %d", dataset.Volsize)
	}
}

// =============================================================================
// NFS Share Tests
// =============================================================================

func TestIntegration_NFSShareCRUD(t *testing.T) {
	client := getTestClient(t)
	pool := getTestPool(t)
	ctx := context.Background()

	datasetName := testDatasetName(pool, "nfs")

	// Create dataset first
	_, err := client.CreateDataset(ctx, &DatasetCreateOptions{
		Name: datasetName,
	})
	if err != nil {
		t.Fatalf("Failed to create test dataset: %v", err)
	}

	t.Cleanup(func() {
		client.DeleteDataset(ctx, datasetName, &DatasetDeleteOptions{
			Recursive: true,
			Force:     true,
		})
	})

	sharePath := "/mnt/" + datasetName
	var shareID int

	// CREATE
	t.Run("Create", func(t *testing.T) {
		opts := &NFSShareCreateOptions{
			Path:    sharePath,
			Comment: "CSI integration test NFS share",
			Enabled: true,
		}
		share, err := client.CreateNFSShare(ctx, opts)
		if err != nil {
			t.Fatalf("CreateNFSShare failed: %v", err)
		}
		if share.Path != sharePath {
			t.Errorf("Expected Path %s, got %s", sharePath, share.Path)
		}
		shareID = share.ID
	})

	// READ by ID
	t.Run("Get", func(t *testing.T) {
		share, err := client.GetNFSShare(ctx, shareID)
		if err != nil {
			t.Fatalf("GetNFSShare failed: %v", err)
		}
		if share.ID != shareID {
			t.Errorf("Expected ID %d, got %d", shareID, share.ID)
		}
	})

	// READ by Path
	t.Run("GetByPath", func(t *testing.T) {
		share, err := client.GetNFSShareByPath(ctx, sharePath)
		if err != nil {
			t.Fatalf("GetNFSShareByPath failed: %v", err)
		}
		if share.ID != shareID {
			t.Errorf("Expected ID %d, got %d", shareID, share.ID)
		}
	})

	// DELETE
	t.Run("Delete", func(t *testing.T) {
		err := client.DeleteNFSShare(ctx, shareID)
		if err != nil {
			t.Fatalf("DeleteNFSShare failed: %v", err)
		}
	})
}

// =============================================================================
// iSCSI Tests
// =============================================================================

func TestIntegration_ISCSIFullWorkflow(t *testing.T) {
	client := getTestClient(t)
	pool := getTestPool(t)
	ctx := context.Background()

	zvolName := testDatasetName(pool, "iscsi")
	targetName := fmt.Sprintf("csi-test-%d", time.Now().UnixNano())

	// Create ZVOL for extent
	_, err := client.CreateDataset(ctx, &DatasetCreateOptions{
		Name:    zvolName,
		Type:    "VOLUME",
		Volsize: 1073741824, // 1 GB
	})
	if err != nil {
		t.Fatalf("Failed to create test ZVOL: %v", err)
	}

	var targetID, extentID, teID int

	t.Cleanup(func() {
		// Cleanup in reverse order
		if teID > 0 {
			client.DeleteISCSITargetExtent(ctx, teID, nil)
		}
		if extentID > 0 {
			client.DeleteISCSIExtent(ctx, extentID, nil)
		}
		if targetID > 0 {
			client.DeleteISCSITarget(ctx, targetID, nil)
		}
		client.DeleteDataset(ctx, zvolName, &DatasetDeleteOptions{
			Recursive: true,
			Force:     true,
		})
	})

	// Create Target (use portal ID 1 for integration tests — the default portal)
	t.Run("CreateTarget", func(t *testing.T) {
		target, err := client.CreateISCSITarget(ctx, targetName, "test-alias", 1)
		if err != nil {
			t.Fatalf("CreateISCSITarget failed: %v", err)
		}
		targetID = target.ID
		if target.Name != targetName {
			t.Errorf("Expected Name %s, got %s", targetName, target.Name)
		}
	})

	// Create Extent
	t.Run("CreateExtent", func(t *testing.T) {
		disk := "zvol/" + zvolName
		extent, err := client.CreateISCSIExtent(ctx, targetName+"-extent", disk, 512)
		if err != nil {
			t.Fatalf("CreateISCSIExtent failed: %v", err)
		}
		extentID = extent.ID
		if extent.Disk != disk {
			t.Errorf("Expected Disk %s, got %s", disk, extent.Disk)
		}
	})

	// Create Target-Extent Association
	t.Run("CreateTargetExtent", func(t *testing.T) {
		te, err := client.CreateISCSITargetExtent(ctx, targetID, extentID, 0)
		if err != nil {
			t.Fatalf("CreateISCSITargetExtent failed: %v", err)
		}
		teID = te.ID
		if te.Target != targetID {
			t.Errorf("Expected Target %d, got %d", targetID, te.Target)
		}
		if te.Extent != extentID {
			t.Errorf("Expected Extent %d, got %d", extentID, te.Extent)
		}
	})

	// Query Target by Name
	t.Run("GetTargetByName", func(t *testing.T) {
		target, err := client.GetISCSITargetByName(ctx, targetName)
		if err != nil {
			t.Fatalf("GetISCSITargetByName failed: %v", err)
		}
		if target.ID != targetID {
			t.Errorf("Expected ID %d, got %d", targetID, target.ID)
		}
	})

	// Query Extent by Disk
	t.Run("GetExtentByDisk", func(t *testing.T) {
		disk := "zvol/" + zvolName
		extent, err := client.GetISCSIExtentByDisk(ctx, disk)
		if err != nil {
			t.Fatalf("GetISCSIExtentByDisk failed: %v", err)
		}
		if extent.ID != extentID {
			t.Errorf("Expected ID %d, got %d", extentID, extent.ID)
		}
	})

	// Query Target-Extent by Extent
	t.Run("GetTargetExtentByExtent", func(t *testing.T) {
		te, err := client.GetISCSITargetExtentByExtent(ctx, extentID)
		if err != nil {
			t.Fatalf("GetISCSITargetExtentByExtent failed: %v", err)
		}
		if te.ID != teID {
			t.Errorf("Expected ID %d, got %d", teID, te.ID)
		}
	})
}

// =============================================================================
// Snapshot Tests
// =============================================================================

func TestIntegration_SnapshotWorkflow(t *testing.T) {
	client := getTestClient(t)
	pool := getTestPool(t)
	ctx := context.Background()

	datasetName := testDatasetName(pool, "snap")
	snapshotName := "test-snap"
	cloneName := testDatasetName(pool, "clone")

	// Create dataset
	_, err := client.CreateDataset(ctx, &DatasetCreateOptions{
		Name: datasetName,
	})
	if err != nil {
		t.Fatalf("Failed to create test dataset: %v", err)
	}

	t.Cleanup(func() {
		client.DeleteDataset(ctx, cloneName, &DatasetDeleteOptions{
			Recursive: true,
			Force:     true,
		})
		client.DeleteSnapshot(ctx, datasetName+"@"+snapshotName)
		client.DeleteDataset(ctx, datasetName, &DatasetDeleteOptions{
			Recursive: true,
			Force:     true,
		})
	})

	// Create Snapshot
	t.Run("Create", func(t *testing.T) {
		snap, err := client.CreateSnapshot(ctx, datasetName, snapshotName, false)
		if err != nil {
			t.Fatalf("CreateSnapshot failed: %v", err)
		}
		expectedID := datasetName + "@" + snapshotName
		if snap.ID != expectedID {
			t.Errorf("Expected ID %s, got %s", expectedID, snap.ID)
		}
	})

	// List Snapshots
	t.Run("List", func(t *testing.T) {
		snapshots, err := client.ListSnapshots(ctx, datasetName)
		if err != nil {
			t.Fatalf("ListSnapshots failed: %v", err)
		}
		if len(snapshots) != 1 {
			t.Errorf("Expected 1 snapshot, got %d", len(snapshots))
		}
	})

	// Clone Snapshot
	t.Run("Clone", func(t *testing.T) {
		snapshotID := datasetName + "@" + snapshotName
		dataset, err := client.CloneSnapshot(ctx, snapshotID, cloneName)
		if err != nil {
			t.Fatalf("CloneSnapshot failed: %v", err)
		}
		if dataset.ID != cloneName {
			t.Errorf("Expected ID %s, got %s", cloneName, dataset.ID)
		}
	})

	// Delete Snapshot (after removing clone dependency)
	t.Run("Delete", func(t *testing.T) {
		// First delete clone
		err := client.DeleteDataset(ctx, cloneName, &DatasetDeleteOptions{
			Force: true,
		})
		if err != nil {
			t.Fatalf("Failed to delete clone: %v", err)
		}

		// Now delete snapshot
		snapshotID := datasetName + "@" + snapshotName
		err = client.DeleteSnapshot(ctx, snapshotID)
		if err != nil {
			t.Fatalf("DeleteSnapshot failed: %v", err)
		}
	})
}

// =============================================================================
// Pool Tests
// =============================================================================

func TestIntegration_ListPools(t *testing.T) {
	client := getTestClient(t)
	ctx := context.Background()

	pools, err := client.ListPools(ctx)
	if err != nil {
		t.Fatalf("ListPools failed: %v", err)
	}

	if len(pools) == 0 {
		t.Error("Expected at least one pool")
	}

	// Check that test pool exists
	pool := getTestPool(t)
	found := false
	for _, p := range pools {
		if p.Name == pool {
			found = true
			if p.Status != "ONLINE" {
				t.Errorf("Expected pool status ONLINE, got %s", p.Status)
			}
			break
		}
	}
	if !found {
		t.Errorf("Test pool %s not found in pool list", pool)
	}
}

func TestIntegration_GetPool(t *testing.T) {
	client := getTestClient(t)
	ctx := context.Background()

	pool := getTestPool(t)
	p, err := client.GetPool(ctx, pool)
	if err != nil {
		t.Fatalf("GetPool failed: %v", err)
	}

	if p.Name != pool {
		t.Errorf("Expected pool name %s, got %s", pool, p.Name)
	}
	if p.Status != "ONLINE" {
		t.Errorf("Expected pool status ONLINE, got %s", p.Status)
	}
}

func TestIntegration_GetAvailableSpace(t *testing.T) {
	client := getTestClient(t)
	ctx := context.Background()

	pool := getTestPool(t)
	space, err := client.GetAvailableSpace(ctx, pool)
	if err != nil {
		t.Fatalf("GetAvailableSpace failed: %v", err)
	}

	if space <= 0 {
		t.Errorf("Expected positive available space, got %d", space)
	}

	t.Logf("Available space in %s: %d bytes (%.2f GB)", pool, space, float64(space)/(1024*1024*1024))
}

// =============================================================================
// iSCSI Auth Tests
// =============================================================================

func TestIntegration_ISCSIAuth(t *testing.T) {
	client := getTestClient(t)
	ctx := context.Background()

	// Get next available tag
	nextTag, err := client.GetNextISCSIAuthTag(ctx)
	if err != nil {
		t.Fatalf("GetNextISCSIAuthTag failed: %v", err)
	}

	t.Logf("Next available auth tag: %d", nextTag)

	// Create auth credential
	opts := &ISCSIAuthCreateOptions{
		Tag:    nextTag,
		User:   fmt.Sprintf("testuser%d", nextTag),
		Secret: "testsecret123", // 12-16 chars required
	}
	auth, err := client.CreateISCSIAuth(ctx, opts)
	if err != nil {
		t.Fatalf("CreateISCSIAuth failed: %v", err)
	}

	t.Cleanup(func() {
		client.DeleteISCSIAuth(ctx, auth.ID)
	})

	if auth.Tag != nextTag {
		t.Errorf("Expected tag %d, got %d", nextTag, auth.Tag)
	}

	// Query by tag
	queried, err := client.GetISCSIAuthByTag(ctx, nextTag)
	if err != nil {
		t.Fatalf("GetISCSIAuthByTag failed: %v", err)
	}
	if queried.ID != auth.ID {
		t.Errorf("Expected ID %d, got %d", auth.ID, queried.ID)
	}
}

// =============================================================================
// iSCSI Initiator Tests
// =============================================================================

func TestIntegration_ISCSIInitiator(t *testing.T) {
	client := getTestClient(t)
	ctx := context.Background()

	opts := &ISCSIInitiatorCreateOptions{
		Initiators: []string{"iqn.1993-08.org.debian:01:test*"},
		Comment:    "CSI integration test initiator",
	}
	initiator, err := client.CreateISCSIInitiator(ctx, opts)
	if err != nil {
		t.Fatalf("CreateISCSIInitiator failed: %v", err)
	}

	t.Cleanup(func() {
		client.DeleteISCSIInitiator(ctx, initiator.ID)
	})

	if len(initiator.Initiators) != 1 {
		t.Errorf("Expected 1 initiator pattern, got %d", len(initiator.Initiators))
	}
}
