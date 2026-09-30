package driver

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/truenas/truenas-csi/pkg/client"
)

// snapshotState returns "live", "pending" (deleted, destroy deferred) or "gone"
// for a snapshot in the fake.
func snapshotState(f *fakeTrueNAS, id string) string {
	for _, s := range f.all("pool.snapshot") {
		if s["id"] == id {
			if s["properties"].(map[string]any)["defer_destroy"].(map[string]any)["value"] == "on" {
				return "pending"
			}
			return "live"
		}
	}
	return "gone"
}

// seedSnapshot stores a snapshot, and when clone is set, a dataset cloned from it.
func seedSnapshot(f *fakeTrueNAS, dataset, name, clone string) string {
	f.mu.Lock()
	defer f.mu.Unlock()
	snapshot := newFakeSnapshot(dataset, name)
	f.records["pool.snapshot"] = append(f.records["pool.snapshot"], snapshot)
	if clone != "" {
		f.records["pool.dataset"] = append(f.records["pool.dataset"], map[string]any{
			"id": clone, "name": clone, "type": datasetTypeVolume, "origin": snapshot["id"],
		})
	}
	return snapshot["id"].(string)
}

// Cloning a volume left its snapshot on the source for good: the delete right
// after the clone was refused, since the clone depends on it, and the error dropped.
func TestCloneFromVolume_SnapshotGoesWithTheClone(t *testing.T) {
	f := newFakeTrueNAS(t)
	s := f.controller()
	ctx := context.Background()

	if err := s.cloneFromVolume(ctx, "tank/pvc-clone", "tank/pvc-src", "tank/pvc-clone"); err != nil {
		t.Fatalf("cloneFromVolume() = %v", err)
	}

	snapshots := f.all("pool.snapshot")
	if len(snapshots) != 1 {
		t.Fatalf("snapshots = %v, want the one the clone was made from", snapshots)
	}
	id := snapshots[0]["id"].(string)
	if !strings.HasPrefix(snapshots[0]["snapshot_name"].(string), cloneSnapshotPrefix) {
		t.Errorf("clone snapshot %s is not named with %q, so the sweep would miss it", id, cloneSnapshotPrefix)
	}
	if got := snapshotState(f, id); got != "pending" {
		t.Fatalf("clone snapshot is %s, want deleted with the destroy deferred", got)
	}

	if err := s.driver.Client().DeleteDataset(ctx, "tank/pvc-clone", &client.DatasetDeleteOptions{Recursive: true, Force: true}); err != nil {
		t.Fatalf("DeleteDataset(clone) = %v", err)
	}
	if got := snapshotState(f, id); got != "gone" {
		t.Errorf("clone snapshot is %s after the clone was deleted, want gone", got)
	}
}

// Deleting a VolumeSnapshot while a volume restored from it exists used to leave
// the ZFS snapshot behind, while Kubernetes was told it was gone.
func TestDeleteSnapshot_DefersWhileARestoredVolumeUsesIt(t *testing.T) {
	f := newFakeTrueNAS(t)
	s := f.controller()
	ctx := context.Background()
	id := seedSnapshot(f, "tank/pvc-src", "snapshot-1", "tank/pvc-restored")

	if _, err := s.DeleteSnapshot(ctx, &csi.DeleteSnapshotRequest{SnapshotId: id}); err != nil {
		t.Fatalf("DeleteSnapshot() = %v", err)
	}
	if got := snapshotState(f, id); got != "pending" {
		t.Fatalf("snapshot is %s, want deleted with the destroy deferred", got)
	}

	// Deleted snapshots must look deleted, even while ZFS still holds them.
	listed, err := s.ListSnapshots(ctx, &csi.ListSnapshotsRequest{SnapshotId: id})
	if err != nil {
		t.Fatalf("ListSnapshots() = %v", err)
	}
	if len(listed.GetEntries()) != 0 {
		t.Errorf("ListSnapshots() still lists the deleted snapshot: %v", listed.GetEntries())
	}
}

// A delete that genuinely fails must be retried, not reported as done.
func TestDeleteSnapshot_ReportsFailure(t *testing.T) {
	f := newFakeTrueNAS(t)
	s := f.controller()
	id := seedSnapshot(f, "tank/pvc-src", "snapshot-1", "")
	f.fail["pool.snapshot.delete"] = true

	_, err := s.DeleteSnapshot(context.Background(), &csi.DeleteSnapshotRequest{SnapshotId: id})
	if status.Code(err) != codes.Internal {
		t.Fatalf("DeleteSnapshot() = %v, want Internal so the snapshotter retries", err)
	}
}

func TestDeleteSnapshot_AlreadyGone(t *testing.T) {
	f := newFakeTrueNAS(t)
	s := f.controller()

	if _, err := s.DeleteSnapshot(context.Background(), &csi.DeleteSnapshotRequest{SnapshotId: "tank/pvc-src@missing"}); err != nil {
		t.Errorf("DeleteSnapshot() of a missing snapshot = %v, want success", err)
	}
}

func TestSweepCloneSnapshots(t *testing.T) {
	f := newFakeTrueNAS(t)
	s := f.controller()

	cloneGone := seedSnapshot(f, "tank/pvc-src", cloneSnapshotPrefix+"a-1", "")
	cloneLives := seedSnapshot(f, "tank/pvc-src", cloneSnapshotPrefix+"b-1", "tank/pvc-b")
	user := seedSnapshot(f, "tank/pvc-src", "snapshot-1", "")

	s.sweepCloneSnapshots(context.Background())

	for id, want := range map[string]string{cloneGone: "gone", cloneLives: "pending", user: "live"} {
		if got := snapshotState(f, id); got != want {
			t.Errorf("%s is %s after the sweep, want %s", id, got, want)
		}
	}
}

// The sweep must never delete the snapshot a clone in progress is about to use.
func TestSweepCloneSnapshots_WaitsForACloneInProgress(t *testing.T) {
	f := newFakeTrueNAS(t)
	s := f.controller()

	s.cloneSnapshotMu.RLock()
	done := make(chan struct{})
	go func() {
		s.sweepCloneSnapshots(context.Background())
		close(done)
	}()

	select {
	case <-done:
		t.Fatal("the sweep ran while a clone held the lock")
	case <-time.After(200 * time.Millisecond):
	}
	s.cloneSnapshotMu.RUnlock()
	<-done
}
