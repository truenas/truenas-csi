package driver

import (
	"context"
	"fmt"
	"strings"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// cloneSnapshotPrefix names the snapshot CreateVolume takes of a source volume to
// clone it. Nothing needs the snapshot once the clone exists, so it is deleted
// straight away, and ZFS destroys it when the clone goes.
const cloneSnapshotPrefix = "csi-clone-"

// cloneSnapshotSweepTimeout bounds the startup sweep of leftover clone snapshots.
const cloneSnapshotSweepTimeout = 5 * time.Minute

// cloneFromVolume clones the dataset source into destination, through a snapshot
// of source that goes away with the clone.
func (s *ControllerServer) cloneFromVolume(ctx context.Context, volumeID, source, destination string) error {
	s.cloneSnapshotMu.RLock()
	defer s.cloneSnapshotMu.RUnlock()

	sanitizedVolumeID := strings.ReplaceAll(volumeID, "/", "-")
	snapshotName := fmt.Sprintf("%s%s-%d", cloneSnapshotPrefix, sanitizedVolumeID, time.Now().Unix())
	snapshot, err := s.driver.Client().CreateSnapshot(ctx, source, snapshotName, false)
	if err != nil {
		return status.Errorf(codes.Internal, "failed to create snapshot for clone: %v", err)
	}

	if _, err := s.driver.Client().CloneSnapshot(ctx, snapshot.ID, destination); err != nil {
		if delErr := s.driver.Client().DeleteSnapshot(ctx, snapshot.ID); delErr != nil {
			s.driver.Log().Error(delErr, "Failed to delete the snapshot of a failed clone", "snapshot", snapshot.ID)
		}
		return status.Errorf(codes.Internal, "failed to clone volume: %v", err)
	}

	// The clone depends on the snapshot, so the delete is deferred: ZFS destroys the
	// snapshot when the clone is deleted.
	if err := s.driver.Client().DeleteSnapshot(ctx, snapshot.ID); err != nil {
		s.driver.Log().Error(err, "Failed to delete the snapshot a clone was made from; it stays on the source volume",
			"snapshot", snapshot.ID, "clone", destination)
	}
	return nil
}

// sweepCloneSnapshots deletes the clone snapshots earlier versions of the driver
// left behind. They deleted them without deferring the destroy, which ZFS refuses
// while the clone exists, so every volume clone left one on its source volume,
// holding on to space there. Each is destroyed now if its clone is gone, or when
// the clone goes.
func (s *ControllerServer) sweepCloneSnapshots(ctx context.Context) {
	ctx, cancel := context.WithTimeout(ctx, cloneSnapshotSweepTimeout)
	defer cancel()

	// A clone in progress holds the read lock between creating its snapshot and
	// cloning from it, so the sweep never deletes a snapshot about to be used.
	s.cloneSnapshotMu.Lock()
	defer s.cloneSnapshotMu.Unlock()

	snapshots, err := s.driver.Client().ListSnapshotsByNamePrefix(ctx, cloneSnapshotPrefix)
	if err != nil {
		s.driver.Log().Error(err, "Failed to look for leftover clone snapshots")
		return
	}

	var deleted int
	for _, snapshot := range snapshots {
		if err := s.driver.Client().DeleteSnapshot(ctx, snapshot.ID); err != nil {
			s.driver.Log().Error(err, "Failed to delete a leftover clone snapshot", "snapshot", snapshot.ID)
			continue
		}
		deleted++
	}
	if deleted > 0 {
		s.driver.Log().Info("Deleted clone snapshots left by an earlier driver version; any a clone still uses is destroyed when that clone is deleted",
			"count", deleted)
	}
}
