package client

import (
	"encoding/json"
	"testing"
)

// ZFS refuses to destroy a snapshot a clone still depends on unless the destroy is
// deferred, and a refused delete leaves the snapshot behind for good.
func TestDeleteSnapshot_Defers(t *testing.T) {
	mock := NewMockTrueNASServer()
	defer mock.Close()
	mock.SetResponse(methodSnapshotDelete, MockResponse{Result: true})

	client := connectTestClient(t, mock)
	assertNoError(t, client.DeleteSnapshot(testContext(t), "tank/vol@snap"))

	requests := mock.GetRequestsByMethod(methodSnapshotDelete)
	assertLen(t, requests, 1)
	var params []json.RawMessage
	assertNoError(t, json.Unmarshal(requests[0].Params, &params))
	var options SnapshotDeleteOptions
	assertNoError(t, json.Unmarshal(params[1], &options))
	if !options.Defer {
		t.Errorf("delete options = %s, want defer set", params[1])
	}
}

// A deferred snapshot stays in ZFS until its last clone goes, but it was deleted:
// nothing asking for snapshots may see it.
func TestSnapshotQueries_LeaveOutPendingDestroy(t *testing.T) {
	mock := NewMockTrueNASServer()
	defer mock.Close()

	pending := MockSnapshot("tank/vol@deleted", "tank/vol", "deleted")
	pending.Properties = map[string]any{"defer_destroy": map[string]any{"value": "on"}}
	live := MockSnapshot("tank/vol@live", "tank/vol", "live")
	live.Properties = map[string]any{"defer_destroy": map[string]any{"value": "off"}}
	mock.SetResponse(methodSnapshotQuery, MockResponse{Result: []Snapshot{pending, live}})

	client := connectTestClient(t, mock)
	ctx := testContext(t)

	for name, list := range map[string]func() ([]Snapshot, error){
		"ListSnapshots":             func() ([]Snapshot, error) { return client.ListSnapshots(ctx, "tank/vol") },
		"ListAllSnapshots":          func() ([]Snapshot, error) { return client.ListAllSnapshots(ctx) },
		"ListSnapshotsByNamePrefix": func() ([]Snapshot, error) { return client.ListSnapshotsByNamePrefix(ctx, "") },
	} {
		snapshots, err := list()
		assertNoError(t, err)
		if len(snapshots) != 1 || snapshots[0].ID != live.ID {
			t.Errorf("%s = %v, want only %s", name, snapshots, live.ID)
		}
	}

	// The query filter matches both here, so this checks the pending one is skipped.
	found, err := client.FindSnapshotByName(ctx, "deleted")
	assertNoError(t, err)
	if found == nil || found.ID != live.ID {
		t.Errorf("FindSnapshotByName() = %v, want the live snapshot, not the deleted one", found)
	}
}
