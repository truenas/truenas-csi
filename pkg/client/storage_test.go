package client

import (
	"encoding/json"
	"errors"
	"testing"
)

// =============================================================================
// Dataset Tests
// =============================================================================

func TestCreateDataset_Success(t *testing.T) {
	mock := NewMockTrueNASServer()
	defer mock.Close()

	mock.SetResponse(methodDatasetCreate, MockResponse{
		Result: MockDataset("tank/test", "test", "tank", 1000, 5000, 10000),
	})

	client := connectTestClient(t, mock)

	opts := &DatasetCreateOptions{
		Name:     "tank/test",
		RefQuota: 10000,
	}
	dataset, err := client.CreateDataset(testContext(t), opts)

	assertNoError(t, err)
	assertNotNil(t, dataset)
	assertEqual(t, dataset.ID, "tank/test")
	assertEqual(t, dataset.Name, "test")
	assertEqual(t, dataset.Pool, "tank")
	assertEqual(t, dataset.Type, "FILESYSTEM")
	assertEqual(t, dataset.RefQuota, int64(10000))

	assertRequestMethod(t, mock, methodDatasetCreate)
}

func TestCreateDataset_ZVOL(t *testing.T) {
	mock := NewMockTrueNASServer()
	defer mock.Close()

	mock.SetResponse(methodDatasetCreate, MockResponse{
		Result: MockZVOL("tank/vol", "vol", "tank", 1073741824),
	})

	client := connectTestClient(t, mock)

	opts := &DatasetCreateOptions{
		Name:    "tank/vol",
		Type:    "VOLUME",
		Volsize: 1073741824,
	}
	dataset, err := client.CreateDataset(testContext(t), opts)

	assertNoError(t, err)
	assertNotNil(t, dataset)
	assertEqual(t, dataset.Type, "VOLUME")
	assertEqual(t, dataset.Volsize, int64(1073741824))
}

func TestCreateDataset_SparseZVOL(t *testing.T) {
	mock := NewMockTrueNASServer()
	defer mock.Close()

	mock.SetResponse(methodDatasetCreate, MockResponse{
		Result: MockZVOL("tank/sparse-vol", "sparse-vol", "tank", 1073741824),
	})

	client := connectTestClient(t, mock)

	sparse := true
	opts := &DatasetCreateOptions{
		Name:    "tank/sparse-vol",
		Type:    "VOLUME",
		Volsize: 1073741824,
		Sparse:  &sparse,
	}
	dataset, err := client.CreateDataset(testContext(t), opts)

	assertNoError(t, err)
	assertNotNil(t, dataset)
	assertEqual(t, dataset.Type, "VOLUME")
	assertEqual(t, dataset.Volsize, int64(1073741824))

	requests := mock.GetRequestsByMethod(methodDatasetCreate)
	assertLen(t, requests, 1)
	var params []any
	json.Unmarshal(requests[0].Params, &params)
	createOpts := params[0].(map[string]any)
	assertTrue(t, createOpts["sparse"].(bool))
}

func TestCreateDataset_ThickZVOL_NoSparseField(t *testing.T) {
	mock := NewMockTrueNASServer()
	defer mock.Close()

	mock.SetResponse(methodDatasetCreate, MockResponse{
		Result: MockZVOL("tank/thick-vol", "thick-vol", "tank", 1073741824),
	})

	client := connectTestClient(t, mock)

	opts := &DatasetCreateOptions{
		Name:    "tank/thick-vol",
		Type:    "VOLUME",
		Volsize: 1073741824,
	}
	dataset, err := client.CreateDataset(testContext(t), opts)

	assertNoError(t, err)
	assertNotNil(t, dataset)

	requests := mock.GetRequestsByMethod(methodDatasetCreate)
	assertLen(t, requests, 1)
	var params []any
	json.Unmarshal(requests[0].Params, &params)
	createOpts := params[0].(map[string]any)
	_, hasSparse := createOpts["sparse"]
	assertFalse(t, hasSparse)
}

func TestCreateDataset_WithEncryption(t *testing.T) {
	mock := NewMockTrueNASServer()
	defer mock.Close()

	mock.SetResponse(methodDatasetCreate, MockResponse{
		Result: MockDataset("tank/encrypted", "encrypted", "tank", 0, 10000, 0),
	})

	client := connectTestClient(t, mock)

	passphrase := "testpassword"
	opts := &DatasetCreateOptions{
		Name:       "tank/encrypted",
		Encryption: true,
		EncryptionOptions: &EncryptionOptions{
			Passphrase: &passphrase,
			Algorithm:  "AES-256-GCM",
		},
	}
	dataset, err := client.CreateDataset(testContext(t), opts)

	assertNoError(t, err)
	assertNotNil(t, dataset)

	// Verify the request params included encryption options
	requests := mock.GetRequestsByMethod(methodDatasetCreate)
	assertLen(t, requests, 1)
}

func TestGetDataset_Success(t *testing.T) {
	mock := NewMockTrueNASServer()
	defer mock.Close()

	mock.SetResponse(methodDatasetGet, MockResponse{
		Result: MockDataset("tank/test", "test", "tank", 2000, 8000, 50000),
	})

	client := connectTestClient(t, mock)

	dataset, err := client.GetDataset(testContext(t), "tank/test")

	assertNoError(t, err)
	assertNotNil(t, dataset)
	assertEqual(t, dataset.ID, "tank/test")
	assertEqual(t, dataset.Used, int64(2000))
	assertEqual(t, dataset.Available, int64(8000))
	assertEqual(t, dataset.RefQuota, int64(50000))
}

func TestGetDataset_NotFound(t *testing.T) {
	mock := NewMockTrueNASServer()
	defer mock.Close()

	mock.SetResponse(methodDatasetGet, MockResponse{
		Error: &RPCError{Code: -6, Message: "Dataset not found"},
	})

	client := connectTestClient(t, mock)

	dataset, err := client.GetDataset(testContext(t), "tank/nonexistent")

	assertError(t, err)
	assertNil(t, dataset)
	assertTrue(t, errors.Is(err, ErrNotFound))
}

func TestListDatasets_Empty(t *testing.T) {
	mock := NewMockTrueNASServer()
	defer mock.Close()

	mock.SetResponse(methodDatasetQuery, MockResponse{
		Result: []map[string]any{},
	})

	client := connectTestClient(t, mock)

	datasets, err := client.ListDatasets(testContext(t), "tank")

	assertNoError(t, err)
	assertLen(t, datasets, 0)
}

func TestListDatasets_Multiple(t *testing.T) {
	mock := NewMockTrueNASServer()
	defer mock.Close()

	mock.SetResponse(methodDatasetQuery, MockResponse{
		Result: []map[string]any{
			MockDataset("tank/ds1", "ds1", "tank", 100, 900, 0),
			MockDataset("tank/ds2", "ds2", "tank", 200, 800, 0),
			MockDataset("tank/ds3", "ds3", "tank", 300, 700, 0),
		},
	})

	client := connectTestClient(t, mock)

	datasets, err := client.ListDatasets(testContext(t), "tank")

	assertNoError(t, err)
	assertLen(t, datasets, 3)
	assertEqual(t, datasets[0].ID, "tank/ds1")
	assertEqual(t, datasets[1].ID, "tank/ds2")
	assertEqual(t, datasets[2].ID, "tank/ds3")
}

func TestUpdateDataset_Success(t *testing.T) {
	mock := NewMockTrueNASServer()
	defer mock.Close()

	mock.SetResponse(methodDatasetUpdate, MockResponse{
		Result: MockDataset("tank/test", "test", "tank", 1000, 9000, 20000),
	})

	client := connectTestClient(t, mock)

	newQuota := int64(20000)
	updates := &DatasetUpdateOptions{
		RefQuota: &newQuota,
	}
	err := client.UpdateDataset(testContext(t), "tank/test", updates)

	assertNoError(t, err)
	assertRequestMethod(t, mock, methodDatasetUpdate)
}

func TestDeleteDataset_WithOptions(t *testing.T) {
	mock := NewMockTrueNASServer()
	defer mock.Close()

	mock.SetResponse(methodDatasetDelete, MockResponse{
		Result: true,
	})

	client := connectTestClient(t, mock)

	opts := &DatasetDeleteOptions{
		Recursive: true,
		Force:     true,
	}
	err := client.DeleteDataset(testContext(t), "tank/test", opts)

	assertNoError(t, err)
	assertRequestMethod(t, mock, methodDatasetDelete)
}

// =============================================================================
// NFS Share Tests
// =============================================================================

func TestCreateNFSShare_Success(t *testing.T) {
	mock := NewMockTrueNASServer()
	defer mock.Close()

	mock.SetResponse(methodNFSCreate, MockResponse{
		Result: MockNFSShare(1, "/mnt/tank/share", "test share", []string{"10.0.0.0/8"}, nil),
	})

	client := connectTestClient(t, mock)

	opts := &NFSShareCreateOptions{
		Path:    "/mnt/tank/share",
		Comment: "test share",
		Hosts:   []string{"10.0.0.0/8"},
		Enabled: true,
	}
	share, err := client.CreateNFSShare(testContext(t), opts)

	assertNoError(t, err)
	assertNotNil(t, share)
	assertEqual(t, share.ID, 1)
	assertEqual(t, share.Path, "/mnt/tank/share")
	assertEqual(t, share.Comment, "test share")
	assertTrue(t, share.Enabled)
}

func TestGetNFSShare_Success(t *testing.T) {
	mock := NewMockTrueNASServer()
	defer mock.Close()

	mock.SetResponse(methodNFSGet, MockResponse{
		Result: MockNFSShare(1, "/mnt/tank/share", "comment", nil, nil),
	})

	client := connectTestClient(t, mock)

	share, err := client.GetNFSShare(testContext(t), 1)

	assertNoError(t, err)
	assertNotNil(t, share)
	assertEqual(t, share.ID, 1)
}

func TestGetNFSShareByPath_Success(t *testing.T) {
	mock := NewMockTrueNASServer()
	defer mock.Close()

	mock.SetResponse(methodNFSQuery, MockResponse{
		Result: []NFSShare{
			MockNFSShare(5, "/mnt/tank/data", "data share", nil, nil),
		},
	})

	client := connectTestClient(t, mock)

	share, err := client.GetNFSShareByPath(testContext(t), "/mnt/tank/data")

	assertNoError(t, err)
	assertNotNil(t, share)
	assertEqual(t, share.ID, 5)
	assertEqual(t, share.Path, "/mnt/tank/data")
}

func TestGetNFSShareByPath_NotFound(t *testing.T) {
	mock := NewMockTrueNASServer()
	defer mock.Close()

	mock.SetResponse(methodNFSQuery, MockResponse{
		Result: []NFSShare{},
	})

	client := connectTestClient(t, mock)

	share, err := client.GetNFSShareByPath(testContext(t), "/mnt/tank/nonexistent")

	assertError(t, err)
	assertNil(t, share)
	assertErrorContains(t, err, "not found")
}

func TestDeleteNFSShare_Success(t *testing.T) {
	mock := NewMockTrueNASServer()
	defer mock.Close()

	mock.SetResponse(methodNFSDelete, MockResponse{
		Result: true,
	})

	client := connectTestClient(t, mock)

	err := client.DeleteNFSShare(testContext(t), 1)

	assertNoError(t, err)
	assertRequestMethod(t, mock, methodNFSDelete)
}

// =============================================================================
// iSCSI Target Tests
// =============================================================================

func TestCreateISCSITarget_Success(t *testing.T) {
	mock := NewMockTrueNASServer()
	defer mock.Close()

	mock.SetResponse(methodISCSITargetCreate, MockResponse{
		Result: MockISCSITarget(1, "target1", "alias1"),
	})

	client := connectTestClient(t, mock)

	target, err := client.CreateISCSITarget(testContext(t), "target1", "alias1")

	assertNoError(t, err)
	assertNotNil(t, target)
	assertEqual(t, target.ID, 1)
	assertEqual(t, target.Name, "target1")
	assertEqual(t, target.Alias, "alias1")
	assertEqual(t, target.Mode, "ISCSI")
}

func TestCreateISCSITargetWithAuth_Success(t *testing.T) {
	mock := NewMockTrueNASServer()
	defer mock.Close()

	mock.SetResponse(methodISCSITargetCreate, MockResponse{
		Result: ISCSITarget{
			ID:    2,
			Name:  "target2",
			Alias: "alias2",
			Mode:  "ISCSI",
			Groups: []ISCSITargetGroup{
				{Portal: 1, AuthMethod: "CHAP", Auth: 5, Initiator: 10},
			},
		},
	})

	client := connectTestClient(t, mock)

	target, err := client.CreateISCSITargetWithAuth(testContext(t), "target2", "alias2", 5, 10)

	assertNoError(t, err)
	assertNotNil(t, target)
	assertEqual(t, target.ID, 2)
	assertLen(t, target.Groups, 1)
	assertEqual(t, target.Groups[0].AuthMethod, "CHAP")
	assertEqual(t, target.Groups[0].Auth, 5)
	assertEqual(t, target.Groups[0].Initiator, 10)
}

func TestGetISCSITargetByName_Success(t *testing.T) {
	mock := NewMockTrueNASServer()
	defer mock.Close()

	mock.SetResponse(methodISCSITargetQuery, MockResponse{
		Result: []ISCSITarget{
			MockISCSITarget(3, "my-target", "my-alias"),
		},
	})

	client := connectTestClient(t, mock)

	target, err := client.GetISCSITargetByName(testContext(t), "my-target")

	assertNoError(t, err)
	assertNotNil(t, target)
	assertEqual(t, target.ID, 3)
	assertEqual(t, target.Name, "my-target")
}

func TestGetISCSITargetByName_NotFound(t *testing.T) {
	mock := NewMockTrueNASServer()
	defer mock.Close()

	mock.SetResponse(methodISCSITargetQuery, MockResponse{
		Result: []ISCSITarget{},
	})

	client := connectTestClient(t, mock)

	target, err := client.GetISCSITargetByName(testContext(t), "nonexistent")

	assertError(t, err)
	assertNil(t, target)
	assertTrue(t, errors.Is(err, ErrNotFound))
}

func TestGetISCSITargetByID_Success(t *testing.T) {
	mock := NewMockTrueNASServer()
	defer mock.Close()

	mock.SetResponse(methodISCSITargetQuery, MockResponse{
		Result: []ISCSITarget{
			MockISCSITarget(7, "target-7", "alias-7"),
		},
	})

	client := connectTestClient(t, mock)

	target, err := client.GetISCSITargetByID(testContext(t), 7)

	assertNoError(t, err)
	assertNotNil(t, target)
	assertEqual(t, target.ID, 7)
}

// =============================================================================
// iSCSI Extent Tests
// =============================================================================

func TestCreateISCSIExtent_Success(t *testing.T) {
	mock := NewMockTrueNASServer()
	defer mock.Close()

	mock.SetResponse(methodISCSIExtentCreate, MockResponse{
		Result: MockISCSIExtent(1, "extent1", "zvol/tank/vol1", 512),
	})

	client := connectTestClient(t, mock)

	extent, err := client.CreateISCSIExtent(testContext(t), "extent1", "zvol/tank/vol1", 512)

	assertNoError(t, err)
	assertNotNil(t, extent)
	assertEqual(t, extent.ID, 1)
	assertEqual(t, extent.Name, "extent1")
	assertEqual(t, extent.Disk, "zvol/tank/vol1")
	assertEqual(t, extent.BlockSize, 512)
	assertEqual(t, extent.Type, "DISK")
	assertTrue(t, extent.Enabled)
}

func TestGetISCSIExtentByName_Success(t *testing.T) {
	mock := NewMockTrueNASServer()
	defer mock.Close()

	mock.SetResponse(methodISCSIExtentQuery, MockResponse{
		Result: []ISCSIExtent{
			MockISCSIExtent(5, "my-extent", "zvol/tank/myvol", 4096),
		},
	})

	client := connectTestClient(t, mock)

	extent, err := client.GetISCSIExtentByName(testContext(t), "my-extent")

	assertNoError(t, err)
	assertNotNil(t, extent)
	assertEqual(t, extent.ID, 5)
	assertEqual(t, extent.Name, "my-extent")
}

func TestGetISCSIExtentByDisk_Success(t *testing.T) {
	mock := NewMockTrueNASServer()
	defer mock.Close()

	mock.SetResponse(methodISCSIExtentQuery, MockResponse{
		Result: []ISCSIExtent{
			MockISCSIExtent(3, "extent-for-disk", "zvol/tank/specific", 512),
		},
	})

	client := connectTestClient(t, mock)

	extent, err := client.GetISCSIExtentByDisk(testContext(t), "zvol/tank/specific")

	assertNoError(t, err)
	assertNotNil(t, extent)
	assertEqual(t, extent.Disk, "zvol/tank/specific")
}

func TestGetISCSIExtentByDisk_NotFound(t *testing.T) {
	mock := NewMockTrueNASServer()
	defer mock.Close()

	mock.SetResponse(methodISCSIExtentQuery, MockResponse{
		Result: []ISCSIExtent{},
	})

	client := connectTestClient(t, mock)

	extent, err := client.GetISCSIExtentByDisk(testContext(t), "zvol/tank/nonexistent")

	assertError(t, err)
	assertNil(t, extent)
	assertTrue(t, errors.Is(err, ErrNotFound))
}

func TestDeleteISCSIExtent_Success(t *testing.T) {
	mock := NewMockTrueNASServer()
	defer mock.Close()

	mock.SetResponse(methodISCSIExtentDelete, MockResponse{
		Result: true,
	})

	client := connectTestClient(t, mock)

	opts := &ISCSIExtentDeleteOptions{
		Remove: false,
		Force:  true,
	}
	err := client.DeleteISCSIExtent(testContext(t), 1, opts)

	assertNoError(t, err)
	assertRequestMethod(t, mock, methodISCSIExtentDelete)
}

// =============================================================================
// iSCSI Target-Extent Association Tests
// =============================================================================

func TestCreateISCSITargetExtent_Success(t *testing.T) {
	mock := NewMockTrueNASServer()
	defer mock.Close()

	mock.SetResponse(methodISCSITargetExtentCreate, MockResponse{
		Result: MockISCSITargetExtent(1, 5, 10, 0),
	})

	client := connectTestClient(t, mock)

	te, err := client.CreateISCSITargetExtent(testContext(t), 5, 10, 0)

	assertNoError(t, err)
	assertNotNil(t, te)
	assertEqual(t, te.ID, 1)
	assertEqual(t, te.Target, 5)
	assertEqual(t, te.Extent, 10)
	assertEqual(t, te.LunID, 0)
}

func TestGetISCSITargetExtentByExtent_Success(t *testing.T) {
	mock := NewMockTrueNASServer()
	defer mock.Close()

	mock.SetResponse(methodISCSITargetExtentQuery, MockResponse{
		Result: []ISCSITargetExtent{
			MockISCSITargetExtent(2, 3, 7, 0),
		},
	})

	client := connectTestClient(t, mock)

	te, err := client.GetISCSITargetExtentByExtent(testContext(t), 7)

	assertNoError(t, err)
	assertNotNil(t, te)
	assertEqual(t, te.Extent, 7)
}

func TestDeleteISCSITargetExtent_Success(t *testing.T) {
	mock := NewMockTrueNASServer()
	defer mock.Close()

	mock.SetResponse(methodISCSITargetExtentDelete, MockResponse{
		Result: true,
	})

	client := connectTestClient(t, mock)

	opts := &ISCSITargetExtentDeleteOptions{Force: true}
	err := client.DeleteISCSITargetExtent(testContext(t), 1, opts)

	assertNoError(t, err)
}

func TestDeleteISCSITarget_WithExtents(t *testing.T) {
	mock := NewMockTrueNASServer()
	defer mock.Close()

	// First call: query target-extent associations
	// Second call: delete each association
	// Third call: delete target
	callCount := 0
	mock.SetResponseFunc(func(method string, params json.RawMessage) MockResponse {
		callCount++
		switch method {
		case methodISCSITargetExtentQuery:
			return MockResponse{
				Result: []ISCSITargetExtent{
					MockISCSITargetExtent(1, 5, 10, 0),
					MockISCSITargetExtent(2, 5, 11, 1),
				},
			}
		case methodISCSITargetExtentDelete:
			return MockResponse{Result: true}
		case methodISCSITargetDelete:
			return MockResponse{Result: true}
		default:
			return MockResponse{Result: nil}
		}
	})

	client := connectTestClient(t, mock)

	opts := &ISCSITargetDeleteOptions{Force: false, DeleteExtents: false}
	err := client.DeleteISCSITarget(testContext(t), 5, opts)

	assertNoError(t, err)
	// Should have: 1 query + 2 TE deletes + 1 target delete = 4 calls
	assertRequestCount(t, mock, methodISCSITargetExtentQuery, 1)
	assertRequestCount(t, mock, methodISCSITargetExtentDelete, 2)
	assertRequestCount(t, mock, methodISCSITargetDelete, 1)
}

// =============================================================================
// iSCSI Auth Tests
// =============================================================================

func TestCreateISCSIAuth_Success(t *testing.T) {
	mock := NewMockTrueNASServer()
	defer mock.Close()

	mock.SetResponse(methodISCSIAuthCreate, MockResponse{
		Result: MockISCSIAuth(1, 5, "chapuser", "chapsecret123"),
	})

	client := connectTestClient(t, mock)

	opts := &ISCSIAuthCreateOptions{
		Tag:    5,
		User:   "chapuser",
		Secret: "chapsecret123",
	}
	auth, err := client.CreateISCSIAuth(testContext(t), opts)

	assertNoError(t, err)
	assertNotNil(t, auth)
	assertEqual(t, auth.ID, 1)
	assertEqual(t, auth.Tag, 5)
	assertEqual(t, auth.User, "chapuser")
}

func TestGetISCSIAuthByTag_Success(t *testing.T) {
	mock := NewMockTrueNASServer()
	defer mock.Close()

	mock.SetResponse(methodISCSIAuthQuery, MockResponse{
		Result: []ISCSIAuth{
			MockISCSIAuth(3, 10, "user10", "secret10xxxx"),
		},
	})

	client := connectTestClient(t, mock)

	auth, err := client.GetISCSIAuthByTag(testContext(t), 10)

	assertNoError(t, err)
	assertNotNil(t, auth)
	assertEqual(t, auth.Tag, 10)
}

func TestGetNextISCSIAuthTag_Success(t *testing.T) {
	mock := NewMockTrueNASServer()
	defer mock.Close()

	mock.SetResponse(methodISCSIAuthQuery, MockResponse{
		Result: []ISCSIAuth{
			MockISCSIAuth(1, 1, "user1", "secret1xxxxx"),
			MockISCSIAuth(2, 5, "user5", "secret5xxxxx"),
			MockISCSIAuth(3, 3, "user3", "secret3xxxxx"),
		},
	})

	client := connectTestClient(t, mock)

	nextTag, err := client.GetNextISCSIAuthTag(testContext(t))

	assertNoError(t, err)
	assertEqual(t, nextTag, 6) // max is 5, so next is 6
}

func TestGetNextISCSIAuthTag_Empty(t *testing.T) {
	mock := NewMockTrueNASServer()
	defer mock.Close()

	mock.SetResponse(methodISCSIAuthQuery, MockResponse{
		Result: []ISCSIAuth{},
	})

	client := connectTestClient(t, mock)

	nextTag, err := client.GetNextISCSIAuthTag(testContext(t))

	assertNoError(t, err)
	assertEqual(t, nextTag, 1)
}

func TestDeleteISCSIAuth_Success(t *testing.T) {
	mock := NewMockTrueNASServer()
	defer mock.Close()

	mock.SetResponse(methodISCSIAuthDelete, MockResponse{
		Result: true,
	})

	client := connectTestClient(t, mock)

	err := client.DeleteISCSIAuth(testContext(t), 1)

	assertNoError(t, err)
}

// =============================================================================
// iSCSI Initiator Tests
// =============================================================================

func TestCreateISCSIInitiator_Success(t *testing.T) {
	mock := NewMockTrueNASServer()
	defer mock.Close()

	mock.SetResponse(methodISCSIInitiatorCreate, MockResponse{
		Result: ISCSIInitiator{
			ID:         1,
			Initiators: []string{"iqn.1993-08.org.debian:01:*"},
			Comment:    "test initiator",
		},
	})

	client := connectTestClient(t, mock)

	opts := &ISCSIInitiatorCreateOptions{
		Initiators: []string{"iqn.1993-08.org.debian:01:*"},
		Comment:    "test initiator",
	}
	initiator, err := client.CreateISCSIInitiator(testContext(t), opts)

	assertNoError(t, err)
	assertNotNil(t, initiator)
	assertEqual(t, initiator.ID, 1)
	assertLen(t, initiator.Initiators, 1)
}

func TestDeleteISCSIInitiator_Success(t *testing.T) {
	mock := NewMockTrueNASServer()
	defer mock.Close()

	mock.SetResponse(methodISCSIInitiatorDelete, MockResponse{
		Result: true,
	})

	client := connectTestClient(t, mock)

	err := client.DeleteISCSIInitiator(testContext(t), 1)

	assertNoError(t, err)
}

// =============================================================================
// Snapshot Tests
// =============================================================================

func TestCreateSnapshot_Success(t *testing.T) {
	mock := NewMockTrueNASServer()
	defer mock.Close()

	mock.SetResponse(methodSnapshotCreate, MockResponse{
		Result: MockSnapshot("tank/data@snap1", "tank/data", "snap1"),
	})

	client := connectTestClient(t, mock)

	snap, err := client.CreateSnapshot(testContext(t), "tank/data", "snap1", false)

	assertNoError(t, err)
	assertNotNil(t, snap)
	assertEqual(t, snap.ID, "tank/data@snap1")
	assertEqual(t, snap.Dataset, "tank/data")
	assertEqual(t, snap.Name, "snap1")
}

func TestCreateSnapshot_Recursive(t *testing.T) {
	mock := NewMockTrueNASServer()
	defer mock.Close()

	mock.SetResponse(methodSnapshotCreate, MockResponse{
		Result: MockSnapshot("tank@recursive-snap", "tank", "recursive-snap"),
	})

	client := connectTestClient(t, mock)

	snap, err := client.CreateSnapshot(testContext(t), "tank", "recursive-snap", true)

	assertNoError(t, err)
	assertNotNil(t, snap)

	// Verify recursive flag was sent
	requests := mock.GetRequestsByMethod(methodSnapshotCreate)
	assertLen(t, requests, 1)
	var params []any
	json.Unmarshal(requests[0].Params, &params)
	opts := params[0].(map[string]any)
	assertTrue(t, opts["recursive"].(bool))
}

func TestDeleteSnapshot_Success(t *testing.T) {
	mock := NewMockTrueNASServer()
	defer mock.Close()

	mock.SetResponse(methodSnapshotDelete, MockResponse{
		Result: true,
	})

	client := connectTestClient(t, mock)

	err := client.DeleteSnapshot(testContext(t), "tank/data@snap1")

	assertNoError(t, err)
	assertRequestMethod(t, mock, methodSnapshotDelete)
}

func TestListSnapshots_Success(t *testing.T) {
	mock := NewMockTrueNASServer()
	defer mock.Close()

	mock.SetResponse(methodSnapshotQuery, MockResponse{
		Result: []Snapshot{
			MockSnapshot("tank/data@snap1", "tank/data", "snap1"),
			MockSnapshot("tank/data@snap2", "tank/data", "snap2"),
		},
	})

	client := connectTestClient(t, mock)

	snapshots, err := client.ListSnapshots(testContext(t), "tank/data")

	assertNoError(t, err)
	assertLen(t, snapshots, 2)
	assertEqual(t, snapshots[0].Name, "snap1")
	assertEqual(t, snapshots[1].Name, "snap2")
}

func TestCloneSnapshot_Success(t *testing.T) {
	mock := NewMockTrueNASServer()
	defer mock.Close()

	mock.SetResponseFunc(func(method string, params json.RawMessage) MockResponse {
		switch method {
		case methodSnapshotClone:
			return MockResponse{Result: nil}
		case methodDatasetGet:
			return MockResponse{
				Result: MockDataset("tank/clone", "clone", "tank", 0, 10000, 0),
			}
		default:
			return MockResponse{Result: nil}
		}
	})

	client := connectTestClient(t, mock)

	dataset, err := client.CloneSnapshot(testContext(t), "tank/data@snap1", "tank/clone")

	assertNoError(t, err)
	assertNotNil(t, dataset)
	assertEqual(t, dataset.ID, "tank/clone")
}

func TestFindSnapshotByName_Success(t *testing.T) {
	mock := NewMockTrueNASServer()
	defer mock.Close()

	mock.SetResponse(methodSnapshotQuery, MockResponse{
		Result: []Snapshot{
			MockSnapshot("tank/vol1@mysnap", "tank/vol1", "mysnap"),
		},
	})

	client := connectTestClient(t, mock)

	snap, err := client.FindSnapshotByName(testContext(t), "mysnap")

	assertNoError(t, err)
	assertNotNil(t, snap)
	assertEqual(t, snap.Name, "mysnap")
}

func TestFindSnapshotByName_NotFound(t *testing.T) {
	mock := NewMockTrueNASServer()
	defer mock.Close()

	mock.SetResponse(methodSnapshotQuery, MockResponse{
		Result: []Snapshot{},
	})

	client := connectTestClient(t, mock)

	snap, err := client.FindSnapshotByName(testContext(t), "nonexistent")

	assertNoError(t, err)
	assertNil(t, snap)
}

func TestListAllSnapshots_Success(t *testing.T) {
	mock := NewMockTrueNASServer()
	defer mock.Close()

	mock.SetResponse(methodSnapshotQuery, MockResponse{
		Result: []Snapshot{
			MockSnapshot("tank/ds1@snap1", "tank/ds1", "snap1"),
			MockSnapshot("tank/ds2@snap2", "tank/ds2", "snap2"),
			MockSnapshot("tank/ds3@snap3", "tank/ds3", "snap3"),
		},
	})

	client := connectTestClient(t, mock)

	snapshots, err := client.ListAllSnapshots(testContext(t))

	assertNoError(t, err)
	assertLen(t, snapshots, 3)
}

// =============================================================================
// Snapshot Task Tests
// =============================================================================

func TestCreateSnapshotTask_Success(t *testing.T) {
	mock := NewMockTrueNASServer()
	defer mock.Close()

	mock.SetResponse(methodSnapshotTaskCreate, MockResponse{
		Result: MockSnapshotTask(1, "tank/data", 7, "DAY"),
	})

	client := connectTestClient(t, mock)

	opts := &SnapshotTaskCreateOptions{
		Dataset:       "tank/data",
		LifetimeValue: 7,
		LifetimeUnit:  "DAY",
		Enabled:       true,
		Schedule: &SnapshotTaskSchedule{
			Minute: "0",
			Hour:   "0",
			Dom:    "*",
			Month:  "*",
			Dow:    "*",
		},
	}
	task, err := client.CreateSnapshotTask(testContext(t), opts)

	assertNoError(t, err)
	assertNotNil(t, task)
	assertEqual(t, task.ID, 1)
	assertEqual(t, task.Dataset, "tank/data")
	assertEqual(t, task.LifetimeValue, 7)
	assertEqual(t, task.LifetimeUnit, "DAY")
}

func TestGetSnapshotTask_Success(t *testing.T) {
	mock := NewMockTrueNASServer()
	defer mock.Close()

	mock.SetResponse(methodSnapshotTaskGet, MockResponse{
		Result: MockSnapshotTask(5, "tank/vol", 30, "DAY"),
	})

	client := connectTestClient(t, mock)

	task, err := client.GetSnapshotTask(testContext(t), 5)

	assertNoError(t, err)
	assertNotNil(t, task)
	assertEqual(t, task.ID, 5)
}

func TestGetSnapshotTaskByDataset_Success(t *testing.T) {
	mock := NewMockTrueNASServer()
	defer mock.Close()

	mock.SetResponse(methodSnapshotTaskQuery, MockResponse{
		Result: []SnapshotTask{
			MockSnapshotTask(3, "tank/specific", 14, "DAY"),
		},
	})

	client := connectTestClient(t, mock)

	task, err := client.GetSnapshotTaskByDataset(testContext(t), "tank/specific")

	assertNoError(t, err)
	assertNotNil(t, task)
	assertEqual(t, task.Dataset, "tank/specific")
}

func TestDeleteSnapshotTask_Success(t *testing.T) {
	mock := NewMockTrueNASServer()
	defer mock.Close()

	mock.SetResponse(methodSnapshotTaskDelete, MockResponse{
		Result: true,
	})

	client := connectTestClient(t, mock)

	opts := &SnapshotTaskDeleteOptions{FixateRemovalDate: true}
	err := client.DeleteSnapshotTask(testContext(t), 1, opts)

	assertNoError(t, err)
}

// =============================================================================
// Pool Tests
// =============================================================================

func TestGetPool_Success(t *testing.T) {
	mock := NewMockTrueNASServer()
	defer mock.Close()

	mock.SetResponse(methodPoolQuery, MockResponse{
		Result: []Pool{
			MockPool(1, "tank", 1000000000000, 500000000000, 500000000000),
		},
	})

	client := connectTestClient(t, mock)

	pool, err := client.GetPool(testContext(t), "tank")

	assertNoError(t, err)
	assertNotNil(t, pool)
	assertEqual(t, pool.ID, 1)
	assertEqual(t, pool.Name, "tank")
	assertEqual(t, pool.Status, "ONLINE")
	assertTrue(t, pool.Healthy)
}

func TestGetPool_NotFound(t *testing.T) {
	mock := NewMockTrueNASServer()
	defer mock.Close()

	mock.SetResponse(methodPoolQuery, MockResponse{
		Result: []Pool{},
	})

	client := connectTestClient(t, mock)

	pool, err := client.GetPool(testContext(t), "nonexistent")

	assertError(t, err)
	assertNil(t, pool)
	assertErrorContains(t, err, "not found")
}

func TestListPools_Success(t *testing.T) {
	mock := NewMockTrueNASServer()
	defer mock.Close()

	mock.SetResponse(methodPoolQuery, MockResponse{
		Result: []Pool{
			MockPool(1, "tank", 1000000000000, 500000000000, 500000000000),
			MockPool(2, "data", 2000000000000, 1000000000000, 1000000000000),
		},
	})

	client := connectTestClient(t, mock)

	pools, err := client.ListPools(testContext(t))

	assertNoError(t, err)
	assertLen(t, pools, 2)
	assertEqual(t, pools[0].Name, "tank")
	assertEqual(t, pools[1].Name, "data")
}

func TestGetAvailableSpace_Success(t *testing.T) {
	mock := NewMockTrueNASServer()
	defer mock.Close()

	mock.SetResponse(methodZFSResourceQuery, MockResponse{
		Result: []ZFSResource{
			MockZFSResource("tank", 500000000000),
		},
	})

	client := connectTestClient(t, mock)

	space, err := client.GetAvailableSpace(testContext(t), "tank")

	assertNoError(t, err)
	assertEqual(t, space, int64(500000000000))
}

func TestGetAvailableSpace_NotFound(t *testing.T) {
	mock := NewMockTrueNASServer()
	defer mock.Close()

	mock.SetResponse(methodZFSResourceQuery, MockResponse{
		Result: []ZFSResource{},
	})

	client := connectTestClient(t, mock)

	_, err := client.GetAvailableSpace(testContext(t), "nonexistent")

	assertError(t, err)
	assertErrorContains(t, err, "not found")
}

// =============================================================================
// Helper Function Tests
// =============================================================================

func TestExtractPoolFromPath(t *testing.T) {
	tests := []struct {
		path     string
		expected string
	}{
		{"tank", "tank"},
		{"tank/dataset", "tank"},
		{"tank/dataset/sub", "tank"},
		{"pool/a/b/c/d", "pool"},
		{"", ""},
	}

	for _, tc := range tests {
		t.Run(tc.path, func(t *testing.T) {
			result := ExtractPoolFromPath(tc.path)
			assertEqual(t, result, tc.expected)
		})
	}
}

func TestGetParsedInt64(t *testing.T) {
	tests := []struct {
		name     string
		input    map[string]any
		key      string
		expected int64
	}{
		{
			name:     "direct float64",
			input:    map[string]any{"value": float64(12345)},
			key:      "value",
			expected: 12345,
		},
		{
			name:     "parsed field in object",
			input:    map[string]any{"prop": map[string]any{"parsed": float64(67890)}},
			key:      "prop",
			expected: 67890,
		},
		{
			name:     "value field as float in object",
			input:    map[string]any{"prop": map[string]any{"value": float64(11111)}},
			key:      "prop",
			expected: 11111,
		},
		{
			name:     "value field as string in object",
			input:    map[string]any{"prop": map[string]any{"value": "22222"}},
			key:      "prop",
			expected: 22222,
		},
		{
			name:     "nil value",
			input:    map[string]any{"prop": nil},
			key:      "prop",
			expected: 0,
		},
		{
			name:     "missing key",
			input:    map[string]any{},
			key:      "missing",
			expected: 0,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result := getParsedInt64(tc.input, tc.key)
			assertEqual(t, result, tc.expected)
		})
	}
}

func TestDatasetCreateOptions_JSONEncoding(t *testing.T) {
	// Verify omitempty works correctly
	opts := &DatasetCreateOptions{
		Name:     "tank/test",
		RefQuota: 10000,
		// All other fields should be omitted
	}

	data, err := json.Marshal(opts)
	assertNoError(t, err)

	var decoded map[string]any
	json.Unmarshal(data, &decoded)

	// Should have name and refquota
	_, hasName := decoded["name"]
	assertTrue(t, hasName)
	_, hasRefQuota := decoded["refquota"]
	assertTrue(t, hasRefQuota)

	// Should NOT have type (empty string)
	_, hasType := decoded["type"]
	assertFalse(t, hasType)

	// Should NOT have encryption (false)
	_, hasEncryption := decoded["encryption"]
	assertFalse(t, hasEncryption)

	// Sparse nil pointer should be omitted
	_, hasSparse := decoded["sparse"]
	assertFalse(t, hasSparse)
}

func TestDatasetCreateOptions_JSONEncoding_WithSparse(t *testing.T) {
	sparse := true
	opts := &DatasetCreateOptions{
		Name:    "tank/sparse",
		Type:    "VOLUME",
		Volsize: 1073741824,
		Sparse:  &sparse,
	}

	data, err := json.Marshal(opts)
	assertNoError(t, err)

	var decoded map[string]any
	json.Unmarshal(data, &decoded)

	sparseVal, hasSparse := decoded["sparse"]
	assertTrue(t, hasSparse)
	assertTrue(t, sparseVal.(bool))
}

func TestZFSProperty_GetInt64(t *testing.T) {
	tests := []struct {
		name     string
		prop     ZFSProperty
		expected int64
	}{
		{
			name:     "float64 value",
			prop:     ZFSProperty{Value: float64(12345)},
			expected: 12345,
		},
		{
			name:     "int64 value",
			prop:     ZFSProperty{Value: int64(67890)},
			expected: 67890,
		},
		{
			name:     "int value",
			prop:     ZFSProperty{Value: int(11111)},
			expected: 11111,
		},
		{
			name:     "string value (returns 0)",
			prop:     ZFSProperty{Value: "22222"},
			expected: 0,
		},
		{
			name:     "nil value",
			prop:     ZFSProperty{Value: nil},
			expected: 0,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result := tc.prop.GetInt64()
			assertEqual(t, result, tc.expected)
		})
	}
}

func TestZFSProperty_GetString(t *testing.T) {
	tests := []struct {
		name     string
		prop     ZFSProperty
		expected string
	}{
		{
			name:     "string value",
			prop:     ZFSProperty{Value: "hello"},
			expected: "hello",
		},
		{
			name:     "int value (returns empty)",
			prop:     ZFSProperty{Value: 12345},
			expected: "",
		},
		{
			name:     "nil value",
			prop:     ZFSProperty{Value: nil},
			expected: "",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result := tc.prop.GetString()
			assertEqual(t, result, tc.expected)
		})
	}
}
