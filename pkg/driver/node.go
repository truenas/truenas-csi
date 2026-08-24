package driver

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"syscall"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/mount-utils"
	"k8s.io/utils/exec"
)

const (
	// Device path prefixes used to tell protocols apart from the mount table.
	devicePrefixDev  = "/dev/"
	devicePrefixNVMe = "/dev/nvme"

	// statfsFlagReadOnly is ST_RDONLY from statfs(2), which the syscall package
	// does not export.
	statfsFlagReadOnly = 0x1
)

// statfs is syscall.Statfs; overridable in tests.
var statfs = syscall.Statfs

// NodeServer implements the CSI Node service
type NodeServer struct {
	driver        *Driver
	mounter       mount.Interface
	iscsiHandler  *ISCSIHandler
	nvmeofHandler *NVMeOFHandler
	nfsHandler    *NFSHandler
	volumeLocks   sync.Map // map[string]*sync.Mutex - per-operation locks
	csi.UnimplementedNodeServer
}

// NodeServerConfig holds configuration for creating a new node server
type NodeServerConfig struct {
	Driver  *Driver
	Mounter mount.Interface
}

// NewNodeServer creates a new NodeServer with the provided configuration
func NewNodeServer(cfg *NodeServerConfig) (*NodeServer, error) {
	if cfg.Driver == nil {
		return nil, fmt.Errorf("driver is required")
	}

	mounter := cfg.Mounter
	if mounter == nil {
		mounter = mount.New("")
	}

	// Create SafeFormatAndMount for filesystem operations
	safeMounter := &mount.SafeFormatAndMount{
		Interface: mounter,
		Exec:      exec.New(),
	}

	iscsiHandler, err := NewISCSIHandler(safeMounter, cfg.Driver.Log())
	if err != nil {
		return nil, fmt.Errorf("failed to create iSCSI handler: %w", err)
	}

	nvmeofHandler, err := NewNVMeOFHandler(safeMounter, cfg.Driver.Log())
	if err != nil {
		return nil, fmt.Errorf("failed to create NVMe-oF handler: %w", err)
	}

	return &NodeServer{
		driver:        cfg.Driver,
		mounter:       mounter,
		iscsiHandler:  iscsiHandler,
		nvmeofHandler: nvmeofHandler,
		nfsHandler:    NewNFSHandler(mounter, cfg.Driver.Log()),
	}, nil
}

// TryAcquireLock attempts to acquire a lock for the given key (non-blocking)
// Key format: "volumeID" or "volumeID-targetPath" for granular locking
// Returns true if lock acquired, false if operation already in progress
func (s *NodeServer) TryAcquireLock(key string) bool {
	mu, _ := s.volumeLocks.LoadOrStore(key, &sync.Mutex{})
	return mu.(*sync.Mutex).TryLock()
}

// ReleaseLock releases the lock for the given key
func (s *NodeServer) ReleaseLock(key string) {
	if mu, ok := s.volumeLocks.Load(key); ok {
		mu.(*sync.Mutex).Unlock()
	}
}

// getHandler returns the appropriate protocol handler for the request
func (s *NodeServer) getHandler(publishContext map[string]string) (ProtocolHandler, error) {
	switch publishContext[PublishContextProtocol] {
	case ProtocolISCSI:
		return s.iscsiHandler, nil
	case ProtocolNVMeOF:
		return s.nvmeofHandler, nil
	case ProtocolNFS:
		return s.nfsHandler, nil
	default:
		return nil, fmt.Errorf("unknown or missing protocol in publish context: %q", publishContext[PublishContextProtocol])
	}
}

// handlerForVolume picks the protocol handler for Unstage/Expand, where no publish
// context is available. The connector file written at stage time is the primary
// signal (NVMe first, then iSCSI).
//
// When no connector file exists the protocol is recovered from the mount at path.
// Falling straight through to NFS is not safe: NFS Unstage is a no-op, so a block
// volume whose connector file was lost would be reported as unstaged while its
// staging mount and its session stayed in place, and the next stage would hand that
// stale mount to workloads.
func (s *NodeServer) handlerForVolume(volumeID, path string) ProtocolHandler {
	if _, err := os.Stat(nvmeConnectorPath(volumeID)); err == nil {
		return s.nvmeofHandler
	}
	if _, err := os.Stat(connectorPath(volumeID)); err == nil {
		return s.iscsiHandler
	}

	if handler := s.handlerForMount(path); handler != nil {
		s.driver.Log().Info("No connector file for volume, recovered the protocol from the mount",
			"volumeId", volumeID, "path", path, "protocol", handler.Protocol())
		return handler
	}

	s.driver.Log().V(LogLevelDebug).Info("No connector file and nothing mounted for volume, treating it as NFS",
		"volumeId", volumeID, "path", path)
	return s.nfsHandler
}

// handlerForMount identifies the protocol of whatever is mounted at path, or returns
// nil when path is not mounted or the mount is not recognizable.
func (s *NodeServer) handlerForMount(path string) ProtocolHandler {
	if path == "" {
		return nil
	}

	mounts, err := s.mounter.List()
	if err != nil {
		s.driver.Log().V(LogLevelDebug).Info("Failed to list mounts", "error", err)
		return nil
	}

	// Paths can be mounted over; the last matching entry is the effective mount.
	var handler ProtocolHandler
	target := filepath.Clean(path)
	for _, m := range mounts {
		if filepath.Clean(m.Path) != target {
			continue
		}
		switch {
		case strings.HasPrefix(m.Type, fsTypeNFS):
			handler = s.nfsHandler
		case strings.HasPrefix(m.Device, devicePrefixNVMe):
			handler = s.nvmeofHandler
		case strings.HasPrefix(m.Device, devicePrefixDev):
			handler = s.iscsiHandler
		}
	}
	return handler
}

// readOnlyRequested reports whether a volume capability asks for a read-only mount.
func readOnlyRequested(capability *csi.VolumeCapability) bool {
	switch capability.GetAccessMode().GetMode() {
	case csi.VolumeCapability_AccessMode_SINGLE_NODE_READER_ONLY,
		csi.VolumeCapability_AccessMode_MULTI_NODE_READER_ONLY:
		return true
	}
	return slices.Contains(capability.GetMount().GetMountFlags(), mountOptionReadOnly)
}

// unusableStagedMountReason reports why the mount already present at the staging
// path cannot be reused, or "" when it can be.
//
// A mount that outlives a storage-side interruption is left either unresponsive or
// permanently read-only (ext4 aborts to read-only on I/O error). Repairing the
// filesystem on the appliance does not help while that mount is still in place: the
// staged filesystem is never re-read, so every pod bind-mounted onto it keeps
// failing with EROFS. Such a mount is re-staged instead of being reused.
func unusableStagedMountReason(path string, readOnly bool) string {
	var stat syscall.Statfs_t
	if err := statfs(path, &stat); err != nil {
		return fmt.Sprintf("staged filesystem is not responding: %v", err)
	}
	if !readOnly && stat.Flags&statfsFlagReadOnly != 0 {
		return "staged filesystem is mounted read-only but read-write access was requested"
	}
	return ""
}

// validateVolumeCapability checks if the requested capability is supported
func (s *NodeServer) validateVolumeCapability(cap *csi.VolumeCapability) error {
	if cap == nil {
		return fmt.Errorf("volume capability is nil")
	}

	// Must have either block or mount capability
	if cap.GetBlock() == nil && cap.GetMount() == nil {
		return fmt.Errorf("either block or mount volume capability is required")
	}

	// Check access mode is supported
	if cap.AccessMode == nil {
		return fmt.Errorf("access mode is required")
	}

	supportedModes := s.driver.VolumeCaps()
	for _, supported := range supportedModes {
		if cap.AccessMode.Mode == supported.Mode {
			return nil
		}
	}

	return fmt.Errorf("access mode %v not supported", cap.AccessMode.Mode)
}

// NodeStageVolume mounts the volume to the staging path (iSCSI) or is a no-op (NFS).
func (s *NodeServer) NodeStageVolume(ctx context.Context, req *csi.NodeStageVolumeRequest) (*csi.NodeStageVolumeResponse, error) {
	s.driver.Log().V(LogLevelDebug).Info("NodeStageVolume called", "volumeId", req.VolumeId, "stagingTargetPath", req.StagingTargetPath)

	// Validate request
	if req.VolumeId == "" {
		return nil, status.Error(codes.InvalidArgument, "volume ID is required")
	}
	if req.StagingTargetPath == "" {
		return nil, status.Error(codes.InvalidArgument, "staging target path is required")
	}
	if req.VolumeCapability == nil {
		return nil, status.Error(codes.InvalidArgument, "volume capability is required")
	}

	// Validate volume capability is supported
	if err := s.validateVolumeCapability(req.VolumeCapability); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "unsupported volume capability: %v", err)
	}

	// Acquire volume lock using volumeID-stagingPath combination. Taken before the
	// idempotency check below, which may have to tear a broken mount back down.
	lockKey := fmt.Sprintf("%s-%s", req.VolumeId, req.StagingTargetPath)
	if !s.TryAcquireLock(lockKey) {
		return nil, status.Errorf(codes.Aborted, "operation already in progress for volume %s", req.VolumeId)
	}
	defer s.ReleaseLock(lockKey)

	// Get appropriate handler
	s.driver.Log().Info("NodeStageVolume received", "volumeId", req.VolumeId, "publishContext", req.PublishContext)
	handler, err := s.getHandler(req.PublishContext)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "failed to determine protocol: %v", err)
	}

	// Check if already staged (idempotency), but only reuse a mount that still
	// works: a dead or read-only staging mount is silently inherited by every pod
	// that publishes from it, so it is torn down and staged again instead.
	notMounted, err := s.mounter.IsLikelyNotMountPoint(req.StagingTargetPath)
	if err != nil && !os.IsNotExist(err) {
		return nil, status.Errorf(codes.Internal, "failed to check staging path: %v", err)
	}
	if !notMounted {
		reason := unusableStagedMountReason(req.StagingTargetPath, readOnlyRequested(req.VolumeCapability))
		if reason == "" {
			s.driver.Log().V(LogLevelDebug).Info("Volume already staged", "volumeId", req.VolumeId, "stagingTargetPath", req.StagingTargetPath)
			return &csi.NodeStageVolumeResponse{}, nil
		}

		s.driver.Log().Info("Existing staging mount is unusable, staging it again",
			"volumeId", req.VolumeId, "stagingTargetPath", req.StagingTargetPath, "reason", reason)
		unstageReq := &UnstageRequest{VolumeID: req.VolumeId, StagingPath: req.StagingTargetPath}
		if err := handler.Unstage(ctx, unstageReq); err != nil {
			return nil, status.Errorf(codes.Internal, "failed to release the unusable staging mount: %v", err)
		}
	}

	// Check if this is a block volume request
	isBlockVolume := req.VolumeCapability.GetBlock() != nil

	// Extract filesystem type for mount volumes
	fsType := DefaultFSType
	var mountFlags []string
	if req.VolumeCapability.GetMount() != nil {
		if req.VolumeCapability.GetMount().FsType != "" {
			fsType = req.VolumeCapability.GetMount().FsType
		}
		mountFlags = req.VolumeCapability.GetMount().GetMountFlags()
	}

	// Build stage request
	stageReq := &StageRequest{
		VolumeID:         req.VolumeId,
		StagingPath:      req.StagingTargetPath,
		FSType:           fsType,
		MountFlags:       mountFlags,
		VolumeCapability: req.VolumeCapability,
		PublishContext:   req.PublishContext,
		VolumeContext:    req.VolumeContext,
		IsBlockVolume:    isBlockVolume,
	}

	// Stage volume
	if _, err := handler.Stage(ctx, stageReq); err != nil {
		return nil, status.Errorf(codes.Internal, "failed to stage volume: %v", err)
	}

	s.driver.Log().V(LogLevelDebug).Info("Successfully staged volume", "volumeId", req.VolumeId, "stagingTargetPath", req.StagingTargetPath)
	return &csi.NodeStageVolumeResponse{}, nil
}

// NodeUnstageVolume unmounts and disconnects the volume from the staging path.
func (s *NodeServer) NodeUnstageVolume(ctx context.Context, req *csi.NodeUnstageVolumeRequest) (*csi.NodeUnstageVolumeResponse, error) {
	s.driver.Log().V(LogLevelDebug).Info("NodeUnstageVolume called", "volumeId", req.VolumeId, "stagingTargetPath", req.StagingTargetPath)

	// Validate request
	if req.VolumeId == "" {
		return nil, status.Error(codes.InvalidArgument, "volume ID is required")
	}
	if req.StagingTargetPath == "" {
		return nil, status.Error(codes.InvalidArgument, "staging target path is required")
	}

	// Acquire volume lock using volumeID-stagingPath combination
	lockKey := fmt.Sprintf("%s-%s", req.VolumeId, req.StagingTargetPath)
	if !s.TryAcquireLock(lockKey) {
		return nil, status.Errorf(codes.Aborted, "operation already in progress for volume %s", req.VolumeId)
	}
	defer s.ReleaseLock(lockKey)

	// No publish context here — pick the handler from the connector file, or from
	// the staging mount itself when the connector file is gone.
	handler := s.handlerForVolume(req.VolumeId, req.StagingTargetPath)

	// Build unstage request
	unstageReq := &UnstageRequest{
		VolumeID:    req.VolumeId,
		StagingPath: req.StagingTargetPath,
	}

	// Unstage volume
	if err := handler.Unstage(ctx, unstageReq); err != nil {
		return nil, status.Errorf(codes.Internal, "failed to unstage volume: %v", err)
	}

	s.driver.Log().V(LogLevelDebug).Info("Successfully unstaged volume", "volumeId", req.VolumeId, "stagingTargetPath", req.StagingTargetPath)
	return &csi.NodeUnstageVolumeResponse{}, nil
}

// NodePublishVolume bind-mounts the staged volume to the target path.
func (s *NodeServer) NodePublishVolume(ctx context.Context, req *csi.NodePublishVolumeRequest) (*csi.NodePublishVolumeResponse, error) {
	s.driver.Log().V(LogLevelDebug).Info("NodePublishVolume called", "volumeId", req.VolumeId, "targetPath", req.TargetPath)

	// Validate request
	if req.VolumeId == "" {
		return nil, status.Error(codes.InvalidArgument, "volume ID is required")
	}
	if req.TargetPath == "" {
		return nil, status.Error(codes.InvalidArgument, "target path is required")
	}
	if req.VolumeCapability == nil {
		return nil, status.Error(codes.InvalidArgument, "volume capability is required")
	}

	// Validate volume capability is supported
	if err := s.validateVolumeCapability(req.VolumeCapability); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "unsupported volume capability: %v", err)
	}

	// Check if this is a block volume request
	isBlockVolume := req.VolumeCapability.GetBlock() != nil

	// Check if already mounted/published (idempotency)
	if isBlockVolume {
		// For block volumes, check if target path exists and is a device
		if _, err := os.Stat(req.TargetPath); err == nil {
			s.driver.Log().V(LogLevelDebug).Info("Block volume already published", "volumeId", req.VolumeId, "targetPath", req.TargetPath)
			return &csi.NodePublishVolumeResponse{}, nil
		}
	} else {
		notMounted, err := s.mounter.IsLikelyNotMountPoint(req.TargetPath)
		if err != nil && !os.IsNotExist(err) {
			return nil, status.Errorf(codes.Internal, "failed to check mount point: %v", err)
		}
		if !notMounted {
			s.driver.Log().V(LogLevelDebug).Info("Volume already mounted", "volumeId", req.VolumeId, "targetPath", req.TargetPath)
			return &csi.NodePublishVolumeResponse{}, nil
		}
	}

	// Acquire volume lock using volumeID-targetPath combination
	lockKey := fmt.Sprintf("%s-%s", req.VolumeId, req.TargetPath)
	if !s.TryAcquireLock(lockKey) {
		return nil, status.Errorf(codes.Aborted, "operation already in progress for volume %s", req.VolumeId)
	}
	defer s.ReleaseLock(lockKey)

	// Get appropriate handler
	handler, err := s.getHandler(req.PublishContext)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "failed to determine protocol: %v", err)
	}

	// Extract mount options
	fsType := ""
	var mountFlags []string
	if req.VolumeCapability.GetMount() != nil {
		fsType = req.VolumeCapability.GetMount().FsType
		mountFlags = req.VolumeCapability.GetMount().GetMountFlags()
	}

	// Build publish request
	publishReq := &PublishRequest{
		VolumeID:         req.VolumeId,
		StagingPath:      req.StagingTargetPath,
		TargetPath:       req.TargetPath,
		FSType:           fsType,
		MountFlags:       mountFlags,
		ReadOnly:         req.Readonly,
		VolumeCapability: req.VolumeCapability,
		PublishContext:   req.PublishContext,
		VolumeContext:    req.VolumeContext,
		IsBlockVolume:    isBlockVolume,
	}

	// Publish volume
	if err := handler.Publish(ctx, publishReq); err != nil {
		return nil, status.Errorf(codes.Internal, "failed to publish volume: %v", err)
	}

	s.driver.Log().V(LogLevelDebug).Info("Successfully published volume", "volumeId", req.VolumeId, "targetPath", req.TargetPath)
	return &csi.NodePublishVolumeResponse{}, nil
}

// NodeUnpublishVolume unmounts the volume from the target path.
func (s *NodeServer) NodeUnpublishVolume(ctx context.Context, req *csi.NodeUnpublishVolumeRequest) (*csi.NodeUnpublishVolumeResponse, error) {
	s.driver.Log().V(LogLevelDebug).Info("NodeUnpublishVolume called", "volumeId", req.VolumeId, "targetPath", req.TargetPath)

	// Validate request
	if req.VolumeId == "" {
		return nil, status.Error(codes.InvalidArgument, "volume ID is required")
	}
	if req.TargetPath == "" {
		return nil, status.Error(codes.InvalidArgument, "target path is required")
	}

	// Acquire volume lock using volumeID-targetPath combination
	lockKey := fmt.Sprintf("%s-%s", req.VolumeId, req.TargetPath)
	if !s.TryAcquireLock(lockKey) {
		return nil, status.Errorf(codes.Aborted, "operation already in progress for volume %s", req.VolumeId)
	}
	defer s.ReleaseLock(lockKey)

	// CleanupMountPoint handles: check if mounted, unmount, remove path (idempotent)
	if err := mount.CleanupMountPoint(req.TargetPath, s.mounter, true); err != nil {
		return nil, status.Errorf(codes.Internal, "failed to cleanup mount point: %v", err)
	}

	s.driver.Log().V(LogLevelDebug).Info("Successfully unpublished volume", "volumeId", req.VolumeId, "targetPath", req.TargetPath)
	return &csi.NodeUnpublishVolumeResponse{}, nil
}

// NodeGetInfo returns the node ID and topology information.
func (s *NodeServer) NodeGetInfo(ctx context.Context, req *csi.NodeGetInfoRequest) (*csi.NodeGetInfoResponse, error) {
	s.driver.Log().V(LogLevelDebug).Info("NodeGetInfo called")

	return &csi.NodeGetInfoResponse{
		NodeId: s.driver.NodeID(),
		// MaxVolumesPerNode: 0 means no limit
		// No pool-based topology: TrueNAS storage (NFS/iSCSI/NVMe-oF) is
		// network-attached and reachable from every node, so volumes must not be
		// constrained to a pool's node label. Only the per-node key is advertised.
		AccessibleTopology: &csi.Topology{
			Segments: map[string]string{
				"topology.truenas.io/node": s.driver.NodeID(),
			},
		},
	}, nil
}

// NodeGetCapabilities returns the capabilities of the node service.
func (s *NodeServer) NodeGetCapabilities(ctx context.Context, req *csi.NodeGetCapabilitiesRequest) (*csi.NodeGetCapabilitiesResponse, error) {
	s.driver.Log().V(LogLevelDebug).Info("NodeGetCapabilities called")

	return &csi.NodeGetCapabilitiesResponse{
		Capabilities: s.driver.NodeCaps(),
	}, nil
}

// NodeGetVolumeStats returns capacity statistics for a mounted volume.
func (s *NodeServer) NodeGetVolumeStats(ctx context.Context, req *csi.NodeGetVolumeStatsRequest) (*csi.NodeGetVolumeStatsResponse, error) {
	s.driver.Log().V(LogLevelDebug).Info("NodeGetVolumeStats called", "volumeId", req.VolumeId, "volumePath", req.VolumePath)

	if req.VolumeId == "" {
		return nil, status.Error(codes.InvalidArgument, "volume ID is required")
	}
	if req.VolumePath == "" {
		return nil, status.Error(codes.InvalidArgument, "volume path is required")
	}

	_, err := os.Stat(req.VolumePath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, status.Errorf(codes.NotFound, "volume path %s does not exist", req.VolumePath)
		}
		return nil, status.Errorf(codes.Internal, "failed to stat volume path: %v", err)
	}

	stats, err := s.getFSStats(req.VolumePath)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to get filesystem stats: %v", err)
	}

	return &csi.NodeGetVolumeStatsResponse{
		Usage: []*csi.VolumeUsage{
			{
				Unit:      csi.VolumeUsage_BYTES,
				Available: stats.availableBytes,
				Total:     stats.totalBytes,
				Used:      stats.usedBytes,
			},
			{
				Unit:      csi.VolumeUsage_INODES,
				Available: stats.availableInodes,
				Total:     stats.totalInodes,
				Used:      stats.usedInodes,
			},
		},
	}, nil
}

// NodeExpandVolume expands the filesystem on iSCSI volumes after controller expansion.
func (s *NodeServer) NodeExpandVolume(ctx context.Context, req *csi.NodeExpandVolumeRequest) (*csi.NodeExpandVolumeResponse, error) {
	s.driver.Log().V(LogLevelDebug).Info("NodeExpandVolume called", "volumeId", req.VolumeId, "volumePath", req.VolumePath)

	if req.VolumeId == "" {
		return nil, status.Error(codes.InvalidArgument, "volume ID is required")
	}
	if req.VolumePath == "" {
		return nil, status.Error(codes.InvalidArgument, "volume path is required")
	}

	// Verify volume path exists
	if _, err := os.Stat(req.VolumePath); os.IsNotExist(err) {
		return nil, status.Errorf(codes.NotFound, "volume path %s does not exist", req.VolumePath)
	}

	// Acquire volume lock using volumeID-volumePath combination
	lockKey := fmt.Sprintf("%s-%s", req.VolumeId, req.VolumePath)
	if !s.TryAcquireLock(lockKey) {
		return nil, status.Errorf(codes.Aborted, "operation already in progress for volume %s", req.VolumeId)
	}
	defer s.ReleaseLock(lockKey)

	// Determine capacity
	var capacityBytes int64
	if req.CapacityRange != nil {
		capacityBytes = req.CapacityRange.RequiredBytes
	}

	// No publish context here — pick the handler from the connector file, or from
	// the mount itself when the connector file is gone (expansion is a no-op for NFS).
	handler := s.handlerForVolume(req.VolumeId, req.VolumePath)

	expandReq := &ExpandRequest{
		VolumeID:      req.VolumeId,
		VolumePath:    req.VolumePath,
		CapacityBytes: capacityBytes,
	}

	// Expand volume
	result, err := handler.Expand(ctx, expandReq)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to expand volume: %v", err)
	}

	return &csi.NodeExpandVolumeResponse{CapacityBytes: result.CapacityBytes}, nil
}

type fsStats struct {
	totalBytes      int64
	availableBytes  int64
	usedBytes       int64
	totalInodes     int64
	availableInodes int64
	usedInodes      int64
}

// getFSStats retrieves filesystem statistics using statfs.
func (s *NodeServer) getFSStats(path string) (*fsStats, error) {
	var stat syscall.Statfs_t
	if err := syscall.Statfs(path, &stat); err != nil {
		return nil, fmt.Errorf("statfs failed: %v", err)
	}

	blockSize := stat.Frsize
	if blockSize == 0 {
		blockSize = stat.Bsize
	}

	totalBytes := int64(stat.Blocks) * blockSize
	availableBytes := int64(stat.Bavail) * blockSize
	freeBytes := int64(stat.Bfree) * blockSize
	usedBytes := totalBytes - freeBytes

	totalInodes := int64(stat.Files)
	availableInodes := int64(stat.Ffree)
	usedInodes := totalInodes - availableInodes

	return &fsStats{
		totalBytes:      totalBytes,
		availableBytes:  availableBytes,
		usedBytes:       usedBytes,
		totalInodes:     totalInodes,
		availableInodes: availableInodes,
		usedInodes:      usedInodes,
	}, nil
}
