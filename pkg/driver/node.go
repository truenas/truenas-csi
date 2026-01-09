package driver

import (
	"context"
	"fmt"
	"os"
	"sync"
	"syscall"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/mount-utils"
	"k8s.io/utils/exec"
)

// NodeServer implements the CSI Node service
type NodeServer struct {
	driver       *Driver
	mounter      mount.Interface
	iscsiHandler *ISCSIHandler
	nfsHandler   *NFSHandler
	volumeLocks  sync.Map // map[string]*sync.Mutex - per-operation locks
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

	return &NodeServer{
		driver:       cfg.Driver,
		mounter:      mounter,
		iscsiHandler: NewISCSIHandler(safeMounter, cfg.Driver.Log()),
		nfsHandler:   NewNFSHandler(mounter, cfg.Driver.Log()),
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
	case ProtocolNFS:
		return s.nfsHandler, nil
	default:
		return nil, fmt.Errorf("unknown or missing protocol in publish context: %q", publishContext[PublishContextProtocol])
	}
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

	// Check if already staged (idempotency)
	notMounted, err := s.mounter.IsLikelyNotMountPoint(req.StagingTargetPath)
	if err != nil && !os.IsNotExist(err) {
		return nil, status.Errorf(codes.Internal, "failed to check staging path: %v", err)
	}
	if !notMounted {
		s.driver.Log().V(LogLevelDebug).Info("Volume already staged", "volumeId", req.VolumeId, "stagingTargetPath", req.StagingTargetPath)
		return &csi.NodeStageVolumeResponse{}, nil
	}

	// Acquire volume lock using volumeID-stagingPath combination
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

	// Check if this is a block volume request
	isBlockVolume := req.VolumeCapability.GetBlock() != nil

	// Extract filesystem type for mount volumes
	fsType := "ext4"
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

	// Determine handler by checking if iSCSI connector file exists
	var handler ProtocolHandler
	cpath := connectorPath(req.VolumeId)
	if _, err := os.Stat(cpath); err == nil {
		// Connector file exists, this is an iSCSI volume
		handler = s.iscsiHandler
	} else {
		// No connector file, assume NFS
		handler = s.nfsHandler
	}

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
		AccessibleTopology: &csi.Topology{
			Segments: map[string]string{
				"topology.truenas.io/node": s.driver.NodeID(),
				"topology.truenas.io/pool": s.driver.DefaultPool(),
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

	// Determine handler by checking if iSCSI connector file exists
	var handler ProtocolHandler
	cpath := connectorPath(req.VolumeId)
	if _, err := os.Stat(cpath); err == nil {
		// Connector file exists, this is an iSCSI volume
		handler = s.iscsiHandler
	} else {
		// No connector file, assume NFS (expansion is a no-op for NFS)
		handler = s.nfsHandler
	}

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
