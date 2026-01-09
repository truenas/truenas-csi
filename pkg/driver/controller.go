package driver

import (
	"context"
	"fmt"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/iXsystems/truenas_k8_driver/pkg/client"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	// defaultOperationTimeout is the default timeout for TrueNAS API operations
	defaultOperationTimeout = 5 * time.Minute

	// shortOperationTimeout is used for quick operations like queries
	shortOperationTimeout = 30 * time.Second

	// nfsShareCleanupDelay is the time to wait after deleting an NFS share
	// before deleting the underlying dataset. This allows the NFS server to
	// release client state and kernel references. Similar to csi-driver-nfs's
	// timeout-based cleanup approach.
	nfsShareCleanupDelay = 2 * time.Second

	// Snapshot task defaults
	defaultSnapshotRetention     = 2
	defaultSnapshotRetentionUnit = "WEEK"
	defaultSnapshotNamingSchema  = "auto-%Y-%m-%d_%H-%M"

	// Encryption defaults
	defaultEncryptionAlgorithm = "AES-256-GCM"

	// Cron schedule field count
	cronFieldCount = 5

	// iSCSI target name length limit (TrueNAS enforces max 120 characters)
	maxISCSITargetNameLength = 120

	// iSCSI extent name length limit (TrueNAS enforces max 64 characters)
	maxISCSIExtentNameLength = 64
)

// Validation sets use map[T]struct{} for memory efficiency (empty struct = 0 bytes)
var (
	ValidCompressionAlgorithms = map[string]struct{}{
		"OFF": {}, "LZ4": {}, "GZIP": {}, "GZIP-1": {}, "GZIP-9": {},
		"ZSTD": {}, "ZSTD-1": {}, "ZSTD-3": {}, "ZSTD-5": {}, "ZSTD-7": {}, "ZSTD-9": {},
		"ZLE": {}, "LZJB": {},
	}

	ValidVolBlockSizes = map[string]struct{}{
		"512": {}, "1K": {}, "2K": {}, "4K": {}, "8K": {},
		"16K": {}, "32K": {}, "64K": {}, "128K": {},
	}

	ValidISCSIBlockSizes = map[int]struct{}{
		512: {}, 1024: {}, 2048: {}, 4096: {},
	}
)

// ControllerServer implements the CSI Controller service
type ControllerServer struct {
	driver *Driver
	csi.UnimplementedControllerServer
}

// withTimeout wraps a context with a timeout if it doesn't already have a deadline
func withTimeout(ctx context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	if _, ok := ctx.Deadline(); ok {
		// Context already has a deadline, don't override
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, timeout)
}

// NewControllerServer creates a new controller server
func NewControllerServer(d *Driver) *ControllerServer {
	return &ControllerServer{
		driver: d,
	}
}

// validateVolumeCapabilities checks if the requested capabilities are supported
func (s *ControllerServer) validateVolumeCapabilities(caps []*csi.VolumeCapability) error {
	supportedModes := s.driver.VolumeCaps()
	for _, cap := range caps {
		// Must have either block or mount capability
		if cap.GetBlock() == nil && cap.GetMount() == nil {
			return fmt.Errorf("either block or mount volume capability is required")
		}
		if cap.AccessMode == nil {
			return fmt.Errorf("access mode is required")
		}
		modeSupported := false
		for _, supported := range supportedModes {
			if cap.AccessMode.Mode == supported.Mode {
				modeSupported = true
				break
			}
		}
		if !modeSupported {
			return fmt.Errorf("access mode %v not supported", cap.AccessMode.Mode)
		}
	}
	return nil
}

// validateStorageClassParameters validates StorageClass parameters
func (s *ControllerServer) validateStorageClassParameters(ctx context.Context, parameters map[string]string) error {
	// Validate compression algorithm
	if val, ok := parameters["compression"]; ok {
		if _, valid := ValidCompressionAlgorithms[strings.ToUpper(val)]; !valid {
			return fmt.Errorf("invalid compression algorithm: %s (valid: OFF, LZ4, GZIP, ZSTD, ZLE, LZJB)", val)
		}
	}

	// Validate protocol
	if val, ok := parameters["protocol"]; ok {
		val = strings.ToLower(val)
		if val != ProtocolNFS && val != ProtocolISCSI {
			return fmt.Errorf("invalid protocol: %s (valid: nfs, iscsi)", val)
		}
	}

	// Validate pool exists
	if pool, ok := parameters["pool"]; ok {
		if _, err := s.driver.Client().GetPool(ctx, pool); err != nil {
			return fmt.Errorf("pool %s does not exist or is not accessible", pool)
		}
	}

	// Validate volblocksize (iSCSI)
	if val, ok := parameters["volblocksize"]; ok {
		if _, valid := ValidVolBlockSizes[strings.ToUpper(val)]; !valid {
			return fmt.Errorf("invalid volblocksize: %s (valid: 512, 1K, 2K, 4K, 8K, 16K, 32K, 64K, 128K)", val)
		}
	}

	// Validate iSCSI blocksize
	if val, ok := parameters["iscsi.blocksize"]; ok {
		bs, err := strconv.Atoi(val)
		if err != nil {
			return fmt.Errorf("invalid iscsi.blocksize: %s (valid: 512, 1024, 2048, 4096)", val)
		}
		if _, valid := ValidISCSIBlockSizes[bs]; !valid {
			return fmt.Errorf("invalid iscsi.blocksize: %s (valid: 512, 1024, 2048, 4096)", val)
		}
	}

	// Validate snapshot schedule format
	if schedule, ok := parameters["snapshot.schedule"]; ok && schedule != "" {
		parts := strings.Fields(schedule)
		if len(parts) != cronFieldCount {
			return fmt.Errorf("invalid snapshot.schedule: expected %d fields (minute hour dom month dow), got %d", cronFieldCount, len(parts))
		}
	}

	// Validate snapshot retention
	if val, ok := parameters["snapshot.retention"]; ok && val != "" {
		days, err := strconv.Atoi(val)
		if err != nil || days < 1 || days > 365 {
			return fmt.Errorf("invalid snapshot.retention: %s (valid: 1-365 days)", val)
		}
	}

	return nil
}

// CreateVolume creates a new volume on TrueNAS, either as an NFS share or iSCSI target.
func (s *ControllerServer) CreateVolume(ctx context.Context, req *csi.CreateVolumeRequest) (*csi.CreateVolumeResponse, error) {
	ctx, cancel := withTimeout(ctx, defaultOperationTimeout)
	defer cancel()

	s.driver.Log().V(LogLevelDebug).Info("CreateVolume called", "name", req.Name)

	if req.Name == "" {
		return nil, status.Error(codes.InvalidArgument, "volume name is required")
	}

	if len(req.VolumeCapabilities) == 0 {
		return nil, status.Error(codes.InvalidArgument, "volume capabilities are required")
	}

	if err := s.validateVolumeCapabilities(req.VolumeCapabilities); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid volume capabilities: %v", err)
	}

	var requiredBytes int64
	if req.CapacityRange != nil {
		requiredBytes = req.CapacityRange.RequiredBytes
		if requiredBytes < 0 {
			return nil, status.Error(codes.InvalidArgument, "required bytes cannot be negative")
		}
	}

	parameters := req.Parameters
	if parameters == nil {
		parameters = make(map[string]string)
	}

	// Validate StorageClass parameters
	if err := s.validateStorageClassParameters(ctx, parameters); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid storage class parameters: %v", err)
	}

	protocol := s.driver.GetProtocolFromParameters(parameters)
	pool := s.driver.GetPoolFromParameters(parameters)

	volumeName := SanitizeVolumeName(req.Name)
	volumeID := s.driver.GenerateVolumeID(pool, volumeName)
	datasetPath := pool + "/" + volumeName

	existingDataset, err := s.driver.Client().GetDataset(ctx, datasetPath)
	if err == nil && existingDataset != nil {
		// Volume already exists - check if compatible (idempotency)
		var existingCapacity int64
		if existingDataset.Type == "VOLUME" {
			// iSCSI ZVOL - use Volsize
			existingCapacity = existingDataset.Volsize
		} else {
			// NFS filesystem - use RefQuota
			existingCapacity = existingDataset.RefQuota
		}

		s.driver.Log().V(LogLevelDebug).Info("Checking existing volume capacity",
			"volumeId", volumeID,
			"datasetType", existingDataset.Type,
			"existingCapacity", existingCapacity,
			"requestedCapacity", requiredBytes,
			"refQuota", existingDataset.RefQuota,
			"volsize", existingDataset.Volsize)

		// If requested capacity is larger than existing, return AlreadyExists error
		// Special case: if existingCapacity is 0, it means no quota is set (unlimited)
		if requiredBytes > 0 && existingCapacity > 0 && existingCapacity < requiredBytes {
			return nil, status.Errorf(codes.AlreadyExists,
				"volume %s already exists with capacity %d bytes, but %d bytes requested",
				volumeID, existingCapacity, requiredBytes)
		}

		// Return existing capacity (or requested if no quota)
		returnedCapacity := existingCapacity
		if returnedCapacity == 0 {
			returnedCapacity = requiredBytes
		}

		s.driver.Log().V(LogLevelDebug).Info("Volume already exists, returning existing volume", "volumeId", volumeID, "capacity", returnedCapacity)
		return &csi.CreateVolumeResponse{
			Volume: &csi.Volume{
				VolumeId:      volumeID,
				CapacityBytes: returnedCapacity,
				VolumeContext: parameters,
			},
		}, nil
	}

	if req.VolumeContentSource != nil {
		return s.createVolumeFromSource(ctx, req, volumeID, datasetPath, protocol, parameters)
	}

	var volInfo *VolumeInfo
	if protocol == ProtocolISCSI {
		volInfo, err = s.createISCSIVolume(ctx, volumeID, datasetPath, requiredBytes, parameters)
	} else {
		volInfo, err = s.createNFSVolume(ctx, volumeID, datasetPath, requiredBytes, parameters)
	}

	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to create volume: %v", err)
	}

	resp := &csi.CreateVolumeResponse{
		Volume: &csi.Volume{
			VolumeId:      volInfo.ID,
			CapacityBytes: volInfo.CapacityBytes,
			VolumeContext: volInfo.VolumeContext,
			ContentSource: volInfo.ContentSource,
		},
	}

	if volInfo.AccessibleTopology != nil {
		resp.Volume.AccessibleTopology = volInfo.AccessibleTopology
	}

	s.driver.Log().V(LogLevelDebug).Info("Volume created successfully", "volumeId", volumeID)
	return resp, nil
}

// createNFSVolume creates a ZFS filesystem dataset and NFS share for the volume.
func (s *ControllerServer) createNFSVolume(ctx context.Context, volumeID, datasetPath string, capacityBytes int64, parameters map[string]string) (*VolumeInfo, error) {
	compression := CompressionLZ4
	if val, ok := parameters["compression"]; ok {
		compression = strings.ToUpper(val)
	}

	sync := "STANDARD"
	if val, ok := parameters["sync"]; ok {
		sync = strings.ToUpper(val)
	}

	datasetOpts := &client.DatasetCreateOptions{
		Name:        datasetPath,
		Type:        "FILESYSTEM",
		RefQuota:    capacityBytes,
		Compression: compression,
		Sync:        sync,
		Properties:  make(map[string]any),
	}

	for key, value := range parameters {
		if propName, found := strings.CutPrefix(key, "zfs."); found {
			datasetOpts.Properties[propName] = value
		}
	}

	// Add encryption options if configured
	if encOpts := parseEncryptionOptions(parameters); encOpts != nil {
		datasetOpts.Encryption = true
		datasetOpts.EncryptionOptions = encOpts
		inheritEncryption := false
		datasetOpts.InheritEncryption = &inheritEncryption
		s.driver.Log().V(LogLevelDebug).Info("Enabling encryption for dataset", "dataset", datasetPath, "algorithm", encOpts.Algorithm)
	}

	dataset, err := s.driver.Client().CreateDataset(ctx, datasetOpts)
	if err != nil {
		return nil, fmt.Errorf("failed to create dataset: %w", err)
	}

	mountpoint := dataset.Mountpoint
	if mountpoint == "" {
		mountpoint = filepath.Join(DefaultMountpoint, datasetPath)
	}

	stringPtr := func(s string) *string { return &s }

	shareOpts := &client.NFSShareCreateOptions{
		Path:        mountpoint,
		Comment:     fmt.Sprintf("CSI volume %s", volumeID),
		Enabled:     true,
		ReadOnly:    false,
		MapAllUser:  stringPtr("root"),
		MapAllGroup: stringPtr("wheel"),
	}

	if hosts, ok := parameters["nfs.hosts"]; ok {
		shareOpts.Hosts = strings.Split(hosts, ",")
	}

	if networks, ok := parameters["nfs.networks"]; ok {
		shareOpts.Networks = strings.Split(networks, ",")
	}

	s.driver.Log().V(LogLevelDebug).Info("Creating NFS share", "mountpoint", mountpoint, "hosts", shareOpts.Hosts, "networks", shareOpts.Networks)
	share, err := s.driver.Client().CreateNFSShare(ctx, shareOpts)
	if err != nil {
		s.driver.Log().Error(err, "Failed to create NFS share", "mountpoint", mountpoint)
		s.driver.Client().DeleteDataset(ctx, datasetPath, &client.DatasetDeleteOptions{Recursive: true, Force: true})
		return nil, fmt.Errorf("failed to create NFS share: %w", err)
	}
	s.driver.Log().V(LogLevelInfo).Info("Successfully created NFS share", "shareId", share.ID, "path", mountpoint)

	// Create snapshot task if configured in parameters
	if _, err := s.createSnapshotTaskFromParameters(ctx, datasetPath, parameters); err != nil {
		s.driver.Log().Error(err, "Failed to create snapshot task for volume", "dataset", datasetPath)
		// Don't fail volume creation if snapshot task fails
	}

	pool := client.ExtractPoolFromPath(datasetPath)
	volInfo := &VolumeInfo{
		ID:            volumeID,
		Name:          volumeID,
		CapacityBytes: capacityBytes,
		DatasetPath:   datasetPath,
		PoolName:      pool,
		Protocol:      "nfs",
		NFSPath:       mountpoint,
		NFSShareID:    share.ID,
		VolumeContext: parameters,
		AccessibleTopology: []*csi.Topology{
			{
				Segments: map[string]string{
					"topology.truenas.io/pool": pool,
				},
			},
		},
	}

	if s.driver.NFSServer() != "" {
		volInfo.VolumeContext["nfsServer"] = s.driver.NFSServer()
	}
	volInfo.VolumeContext["nfsPath"] = mountpoint

	return volInfo, nil
}

// makeISCSITargetSuffix creates a valid iSCSI target name suffix from a volume ID.
// The name is converted to lowercase (TrueNAS requirement) and truncated to fit
// within TrueNAS's 120 character limit.
func makeISCSITargetSuffix(volumeID string) string {
	suffix := strings.ToLower(fmt.Sprintf("csi-%s", strings.ReplaceAll(volumeID, "/", "-")))
	if len(suffix) > maxISCSITargetNameLength {
		suffix = suffix[:maxISCSITargetNameLength]
	}
	return suffix
}

// makeISCSIExtentName creates a valid iSCSI extent name from a volume ID.
// TrueNAS limits extent names to 64 characters.
func makeISCSIExtentName(volumeID string) string {
	if len(volumeID) > maxISCSIExtentNameLength {
		return volumeID[:maxISCSIExtentNameLength]
	}
	return volumeID
}

// createISCSIVolume creates a ZVOL with iSCSI target, extent, and optional CHAP authentication.
func (s *ControllerServer) createISCSIVolume(ctx context.Context, volumeID, datasetPath string, capacityBytes int64, parameters map[string]string) (*VolumeInfo, error) {
	compression := "LZ4"
	if val, ok := parameters["compression"]; ok {
		compression = strings.ToUpper(val)
	}

	volblocksize := "16K"
	if val, ok := parameters["volblocksize"]; ok {
		volblocksize = val
	}

	datasetOpts := &client.DatasetCreateOptions{
		Name:         datasetPath,
		Type:         "VOLUME",
		Volsize:      capacityBytes,
		Volblocksize: volblocksize,
		Compression:  compression,
		Properties:   make(map[string]any),
	}

	for key, value := range parameters {
		if propName, found := strings.CutPrefix(key, "zfs."); found {
			datasetOpts.Properties[propName] = value
		}
	}

	// Add encryption options if configured
	if encOpts := parseEncryptionOptions(parameters); encOpts != nil {
		datasetOpts.Encryption = true
		datasetOpts.EncryptionOptions = encOpts
		inheritEncryption := false
		datasetOpts.InheritEncryption = &inheritEncryption
		s.driver.Log().V(LogLevelDebug).Info("Enabling encryption for ZVOL", "dataset", datasetPath, "algorithm", encOpts.Algorithm)
	}

	_, err := s.driver.Client().CreateDataset(ctx, datasetOpts)
	if err != nil {
		return nil, fmt.Errorf("failed to create ZVOL: %w", err)
	}

	// Create CHAP auth group if credentials are provided
	var authID int
	var authTag int
	if chapUser, ok := parameters["iscsi.chapUser"]; ok && chapUser != "" {
		chapSecret := parameters["iscsi.chapSecret"]
		if chapSecret == "" {
			s.driver.Client().DeleteDataset(ctx, datasetPath, &client.DatasetDeleteOptions{Recursive: true, Force: true})
			return nil, fmt.Errorf("iscsi.chapSecret is required when iscsi.chapUser is specified")
		}

		// Get next available auth tag
		nextTag, err := s.driver.Client().GetNextISCSIAuthTag(ctx)
		if err != nil {
			s.driver.Client().DeleteDataset(ctx, datasetPath, &client.DatasetDeleteOptions{Recursive: true, Force: true})
			return nil, fmt.Errorf("failed to get next auth tag: %w", err)
		}

		authOpts := &client.ISCSIAuthCreateOptions{
			Tag:    nextTag,
			User:   chapUser,
			Secret: chapSecret,
		}

		// Add mutual CHAP if provided
		if peerUser, ok := parameters["iscsi.chapPeerUser"]; ok && peerUser != "" {
			authOpts.PeerUser = peerUser
			authOpts.PeerSecret = parameters["iscsi.chapPeerSecret"]
			authOpts.DiscoveryAuth = "CHAP_MUTUAL"
		}

		auth, err := s.driver.Client().CreateISCSIAuth(ctx, authOpts)
		if err != nil {
			s.driver.Client().DeleteDataset(ctx, datasetPath, &client.DatasetDeleteOptions{Recursive: true, Force: true})
			return nil, fmt.Errorf("failed to create CHAP auth: %w", err)
		}
		authID = auth.ID
		authTag = auth.Tag
		s.driver.Log().V(LogLevelDebug).Info("Created CHAP auth for iSCSI target", "authId", authID, "tag", authTag, "user", chapUser)
	}

	// Create initiator group if specified
	var initiatorID int
	initiators := parameters["iscsi.initiators"]
	networks := parameters["iscsi.networks"]
	if initiators != "" || networks != "" {
		initOpts := &client.ISCSIInitiatorCreateOptions{
			Comment: fmt.Sprintf("CSI volume %s", volumeID),
		}
		if initiators != "" {
			initOpts.Initiators = strings.Split(initiators, ",")
		}
		if networks != "" {
			initOpts.AuthNetwork = strings.Split(networks, ",")
		}

		init, err := s.driver.Client().CreateISCSIInitiator(ctx, initOpts)
		if err != nil {
			// Cleanup auth if created
			if authID > 0 {
				s.driver.Client().DeleteISCSIAuth(ctx, authID)
			}
			s.driver.Client().DeleteDataset(ctx, datasetPath, &client.DatasetDeleteOptions{Recursive: true, Force: true})
			return nil, fmt.Errorf("failed to create initiator group: %w", err)
		}
		initiatorID = init.ID
		s.driver.Log().V(LogLevelDebug).Info("Created initiator group for iSCSI target", "initiatorId", initiatorID, "initiators", initiators, "networks", networks)
	}

	iqnBase := s.driver.GetISCSIIQNBaseFromParameters(parameters)

	targetSuffix := makeISCSITargetSuffix(volumeID)
	target, err := s.driver.Client().CreateISCSITargetWithAuth(ctx, targetSuffix, fmt.Sprintf("CSI volume %s", volumeID), authTag, initiatorID)
	if err != nil {
		// Cleanup auth and initiator if created
		if initiatorID > 0 {
			s.driver.Client().DeleteISCSIInitiator(ctx, initiatorID)
		}
		if authID > 0 {
			s.driver.Client().DeleteISCSIAuth(ctx, authID)
		}
		s.driver.Client().DeleteDataset(ctx, datasetPath, &client.DatasetDeleteOptions{Recursive: true, Force: true})
		return nil, fmt.Errorf("failed to create iSCSI target: %w", err)
	}

	zvolPath := fmt.Sprintf("zvol/%s", datasetPath)
	blocksize := 512
	if val, ok := parameters["iscsi.blocksize"]; ok {
		if bs, err := strconv.Atoi(val); err == nil {
			blocksize = bs
		}
	}

	extent, err := s.driver.Client().CreateISCSIExtent(ctx, makeISCSIExtentName(volumeID), zvolPath, blocksize)
	if err != nil {
		// Full cleanup: target, initiator, auth, dataset
		s.driver.Client().DeleteISCSITarget(ctx, target.ID, &client.ISCSITargetDeleteOptions{Force: true})
		if initiatorID > 0 {
			s.driver.Client().DeleteISCSIInitiator(ctx, initiatorID)
		}
		if authID > 0 {
			s.driver.Client().DeleteISCSIAuth(ctx, authID)
		}
		s.driver.Client().DeleteDataset(ctx, datasetPath, &client.DatasetDeleteOptions{Recursive: true, Force: true})
		return nil, fmt.Errorf("failed to create iSCSI extent: %w", err)
	}

	_, err = s.driver.Client().CreateISCSITargetExtent(ctx, target.ID, extent.ID, 0)
	if err != nil {
		// Full cleanup: extent, target, initiator, auth, dataset
		s.driver.Client().DeleteISCSIExtent(ctx, extent.ID, &client.ISCSIExtentDeleteOptions{Force: true})
		s.driver.Client().DeleteISCSITarget(ctx, target.ID, &client.ISCSITargetDeleteOptions{Force: true})
		if initiatorID > 0 {
			s.driver.Client().DeleteISCSIInitiator(ctx, initiatorID)
		}
		if authID > 0 {
			s.driver.Client().DeleteISCSIAuth(ctx, authID)
		}
		s.driver.Client().DeleteDataset(ctx, datasetPath, &client.DatasetDeleteOptions{Recursive: true, Force: true})
		return nil, fmt.Errorf("failed to associate target and extent: %w", err)
	}

	// Create snapshot task if configured in parameters
	if _, err := s.createSnapshotTaskFromParameters(ctx, datasetPath, parameters); err != nil {
		s.driver.Log().Error(err, "Failed to create snapshot task for volume", "dataset", datasetPath)
		// Don't fail volume creation if snapshot task fails
	}

	fullIQN := fmt.Sprintf("%s:%s", iqnBase, target.Name)

	pool := client.ExtractPoolFromPath(datasetPath)
	volInfo := &VolumeInfo{
		ID:               volumeID,
		Name:             volumeID,
		CapacityBytes:    capacityBytes,
		DatasetPath:      datasetPath,
		PoolName:         pool,
		Protocol:         "iscsi",
		TargetIQN:        fullIQN,
		TargetPortal:     s.driver.ISCSIPortal(),
		LUN:              0,
		ISCSITargetID:    target.ID,
		ISCSIExtentID:    extent.ID,
		ISCSIAuthID:      authID,
		ISCSIInitiatorID: initiatorID,
		VolumeContext:    parameters,
		AccessibleTopology: []*csi.Topology{
			{
				Segments: map[string]string{
					"topology.truenas.io/pool": pool,
				},
			},
		},
	}

	volInfo.VolumeContext["targetPortal"] = s.driver.ISCSIPortal()
	volInfo.VolumeContext["targetIQN"] = fullIQN
	volInfo.VolumeContext["lun"] = "0"

	return volInfo, nil
}

// createVolumeFromSource creates a volume from a snapshot or existing volume by cloning.
func (s *ControllerServer) createVolumeFromSource(ctx context.Context, req *csi.CreateVolumeRequest, volumeID, datasetPath, protocol string, parameters map[string]string) (*csi.CreateVolumeResponse, error) {
	s.driver.Log().V(LogLevelDebug).Info("Creating volume from content source", "volumeId", volumeID)
	contentSource := req.VolumeContentSource

	// Idempotency check: if the target dataset already exists, return it
	existingDataset, err := s.driver.Client().GetDataset(ctx, datasetPath)
	if err == nil && existingDataset != nil {
		s.driver.Log().V(LogLevelDebug).Info("Volume from content source already exists", "volumeId", volumeID)
		capacityBytes := existingDataset.RefQuota
		if protocol == ProtocolISCSI && existingDataset.Volsize > 0 {
			capacityBytes = existingDataset.Volsize
		}
		return &csi.CreateVolumeResponse{
			Volume: &csi.Volume{
				VolumeId:      volumeID,
				CapacityBytes: capacityBytes,
				VolumeContext: parameters,
				ContentSource: contentSource,
			},
		}, nil
	}

	switch {
	case contentSource.GetSnapshot() != nil:
		snapshot := contentSource.GetSnapshot()
		if snapshot.SnapshotId == "" {
			return nil, status.Error(codes.InvalidArgument, "snapshot ID is required")
		}

		s.driver.Log().V(LogLevelDebug).Info("Cloning snapshot", "snapshotId", snapshot.SnapshotId, "datasetPath", datasetPath)
		_, err := s.driver.Client().CloneSnapshot(ctx, snapshot.SnapshotId, datasetPath)
		if err != nil {
			if client.IsNotFoundError(err) {
				return nil, status.Errorf(codes.NotFound, "source snapshot %s not found", snapshot.SnapshotId)
			}
			return nil, status.Errorf(codes.Internal, "failed to clone snapshot: %v", err)
		}

		requiredBytes := req.CapacityRange.RequiredBytes
		if requiredBytes > 0 {
			updateOpts := &client.DatasetUpdateOptions{}
			if protocol == ProtocolISCSI {
				updateOpts.Volsize = &requiredBytes
				updateOpts.RefReservation = &requiredBytes
			} else {
				updateOpts.RefQuota = &requiredBytes
			}
			err = s.driver.Client().UpdateDataset(ctx, datasetPath, updateOpts)
			if err != nil {
				return nil, status.Errorf(codes.Internal, "failed to set capacity on restored volume: %v", err)
			}
		}

		dataset, err := s.driver.Client().GetDataset(ctx, datasetPath)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "failed to get cloned dataset: %v", err)
		}

		var volInfo *VolumeInfo
		if protocol == ProtocolISCSI {
			volInfo, err = s.createISCSITargetForClone(ctx, volumeID, datasetPath, requiredBytes, parameters)
		} else {
			volInfo, err = s.createNFSShareForClone(ctx, volumeID, datasetPath, dataset, parameters)
		}

		if err != nil {
			s.driver.Client().DeleteDataset(ctx, datasetPath, &client.DatasetDeleteOptions{Recursive: true, Force: true})
			return nil, status.Errorf(codes.Internal, "failed to create share for clone: %v", err)
		}

		volInfo.ContentSource = contentSource

		capacityBytes := dataset.RefQuota
		if protocol == ProtocolISCSI {
			capacityBytes = requiredBytes
		}

		return &csi.CreateVolumeResponse{
			Volume: &csi.Volume{
				VolumeId:      volumeID,
				CapacityBytes: capacityBytes,
				VolumeContext: parameters,
				ContentSource: contentSource,
			},
		}, nil

	case contentSource.GetVolume() != nil:
		sourceVolume := contentSource.GetVolume()
		if sourceVolume.VolumeId == "" {
			return nil, status.Error(codes.InvalidArgument, "source volume ID is required")
		}

		sourceInfo, err := s.driver.GetVolumeInfo(sourceVolume.VolumeId)
		if err != nil {
			return nil, status.Errorf(codes.NotFound, "source volume not found: %v", err)
		}

		sanitizedVolumeID := strings.ReplaceAll(volumeID, "/", "-")
		snapshotName := fmt.Sprintf("csi-clone-%s-%d", sanitizedVolumeID, time.Now().Unix())
		snapshot, err := s.driver.Client().CreateSnapshot(ctx, sourceInfo.DatasetPath, snapshotName, false)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "failed to create snapshot for clone: %v", err)
		}

		_, err = s.driver.Client().CloneSnapshot(ctx, snapshot.ID, datasetPath)
		if err != nil {
			s.driver.Client().DeleteSnapshot(ctx, snapshot.ID)
			return nil, status.Errorf(codes.Internal, "failed to clone volume: %v", err)
		}

		s.driver.Client().DeleteSnapshot(ctx, snapshot.ID)

		requiredBytes := req.CapacityRange.RequiredBytes
		if requiredBytes > 0 {
			updateOpts := &client.DatasetUpdateOptions{}
			if protocol == ProtocolISCSI {
				updateOpts.Volsize = &requiredBytes
				updateOpts.RefReservation = &requiredBytes
			} else {
				updateOpts.RefQuota = &requiredBytes
			}
			err = s.driver.Client().UpdateDataset(ctx, datasetPath, updateOpts)
			if err != nil {
				return nil, status.Errorf(codes.Internal, "failed to set capacity on cloned volume: %v", err)
			}
		}

		dataset, err := s.driver.Client().GetDataset(ctx, datasetPath)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "failed to get cloned dataset: %v", err)
		}

		var volInfo *VolumeInfo
		if protocol == ProtocolISCSI {
			volInfo, err = s.createISCSITargetForClone(ctx, volumeID, datasetPath, requiredBytes, parameters)
		} else {
			volInfo, err = s.createNFSShareForClone(ctx, volumeID, datasetPath, dataset, parameters)
		}

		if err != nil {
			s.driver.Client().DeleteDataset(ctx, datasetPath, &client.DatasetDeleteOptions{Recursive: true, Force: true})
			return nil, status.Errorf(codes.Internal, "failed to create share for clone: %v", err)
		}

		volInfo.ContentSource = contentSource

		capacityBytes := dataset.RefQuota
		if protocol == ProtocolISCSI {
			capacityBytes = requiredBytes
		}

		return &csi.CreateVolumeResponse{
			Volume: &csi.Volume{
				VolumeId:      volumeID,
				CapacityBytes: capacityBytes,
				VolumeContext: parameters,
				ContentSource: contentSource,
			},
		}, nil

	default:
		return nil, status.Error(codes.InvalidArgument, "unsupported volume content source")
	}
}

// createNFSShareForClone creates an NFS share for a cloned dataset.
func (s *ControllerServer) createNFSShareForClone(ctx context.Context, volumeID, datasetPath string, dataset *client.Dataset, parameters map[string]string) (*VolumeInfo, error) {
	mountpoint := dataset.Mountpoint
	if mountpoint == "" {
		mountpoint = fmt.Sprintf("/mnt/%s", datasetPath)
	}

	shareOpts := &client.NFSShareCreateOptions{
		Path:     mountpoint,
		Comment:  fmt.Sprintf("CSI volume clone %s", volumeID),
		Enabled:  true,
		ReadOnly: false,
	}

	share, err := s.driver.Client().CreateNFSShare(ctx, shareOpts)
	if err != nil {
		return nil, err
	}

	volInfo := &VolumeInfo{
		ID:            volumeID,
		Name:          volumeID,
		CapacityBytes: dataset.RefQuota,
		DatasetPath:   datasetPath,
		PoolName:      client.ExtractPoolFromPath(datasetPath),
		Protocol:      "nfs",
		NFSPath:       mountpoint,
		NFSShareID:    share.ID,
		VolumeContext: parameters,
	}

	if s.driver.NFSServer() != "" {
		volInfo.VolumeContext["nfsServer"] = s.driver.NFSServer()
	}
	volInfo.VolumeContext["nfsPath"] = mountpoint

	return volInfo, nil
}

// createISCSITargetForClone creates iSCSI target and extent for a cloned ZVOL.
func (s *ControllerServer) createISCSITargetForClone(ctx context.Context, volumeID, datasetPath string, capacityBytes int64, parameters map[string]string) (*VolumeInfo, error) {
	iqnBase := s.driver.GetISCSIIQNBaseFromParameters(parameters)

	targetSuffix := makeISCSITargetSuffix(volumeID)
	target, err := s.driver.Client().CreateISCSITarget(ctx, targetSuffix, fmt.Sprintf("CSI volume clone %s", volumeID))
	if err != nil {
		return nil, err
	}

	zvolPath := fmt.Sprintf("zvol/%s", datasetPath)
	extent, err := s.driver.Client().CreateISCSIExtent(ctx, makeISCSIExtentName(volumeID), zvolPath, 512)
	if err != nil {
		s.driver.Client().DeleteISCSITarget(ctx, target.ID, &client.ISCSITargetDeleteOptions{Force: true})
		return nil, err
	}

	_, err = s.driver.Client().CreateISCSITargetExtent(ctx, target.ID, extent.ID, 0)
	if err != nil {
		s.driver.Client().DeleteISCSIExtent(ctx, extent.ID, &client.ISCSIExtentDeleteOptions{Force: true})
		s.driver.Client().DeleteISCSITarget(ctx, target.ID, &client.ISCSITargetDeleteOptions{Force: true})
		return nil, err
	}

	fullIQN := fmt.Sprintf("%s:%s", iqnBase, target.Name)

	pool := client.ExtractPoolFromPath(datasetPath)
	volInfo := &VolumeInfo{
		ID:            volumeID,
		Name:          volumeID,
		CapacityBytes: capacityBytes,
		DatasetPath:   datasetPath,
		PoolName:      pool,
		Protocol:      "iscsi",
		TargetIQN:     fullIQN,
		TargetPortal:  s.driver.ISCSIPortal(),
		LUN:           0,
		ISCSITargetID: target.ID,
		ISCSIExtentID: extent.ID,
		VolumeContext: parameters,
		AccessibleTopology: []*csi.Topology{
			{
				Segments: map[string]string{
					"topology.truenas.io/pool": pool,
				},
			},
		},
	}

	volInfo.VolumeContext["targetPortal"] = s.driver.ISCSIPortal()
	volInfo.VolumeContext["targetIQN"] = fullIQN
	volInfo.VolumeContext["lun"] = "0"

	return volInfo, nil
}

// DeleteVolume deletes a volume and all its associated resources (NFS share or iSCSI target/extent).
func (s *ControllerServer) DeleteVolume(ctx context.Context, req *csi.DeleteVolumeRequest) (*csi.DeleteVolumeResponse, error) {
	ctx, cancel := withTimeout(ctx, defaultOperationTimeout)
	defer cancel()

	s.driver.Log().V(LogLevelDebug).Info("DeleteVolume called", "volumeId", req.VolumeId)

	if req.VolumeId == "" {
		return nil, status.Error(codes.InvalidArgument, "volume ID is required")
	}

	pool, name, err := s.driver.ParseVolumeID(req.VolumeId)
	if err != nil {
		// Invalid volume ID format means volume doesn't exist - return success (idempotent)
		s.driver.Log().V(LogLevelDebug).Info("Invalid volume ID format, treating as already deleted", "volumeId", req.VolumeId)
		return &csi.DeleteVolumeResponse{}, nil
	}

	datasetPath := fmt.Sprintf("%s/%s", pool, name)

	// Check if dataset exists - return success if already deleted (idempotent)
	_, err = s.driver.Client().GetDataset(ctx, datasetPath)
	if err != nil {
		if client.IsNotFoundError(err) {
			s.driver.Log().V(LogLevelDebug).Info("Volume already deleted", "volumeId", req.VolumeId)
			return &csi.DeleteVolumeResponse{}, nil
		}
	}

	// Get volume info for resource cleanup
	volInfo, _ := s.driver.GetVolumeInfo(req.VolumeId)

	// Clean up protocol-specific resources (iSCSI target/extent/auth or NFS share)
	if volInfo != nil && volInfo.Protocol == ProtocolISCSI {
		deleteOpts := s.driver.GetISCSIDeleteOptionsFromParameters(volInfo.VolumeContext)

		targetDeleteOpts := &client.ISCSITargetDeleteOptions{
			Force:         deleteOpts.ForceDelete,
			DeleteExtents: deleteOpts.DeleteExtentsWithTarget,
		}
		extentDeleteOpts := &client.ISCSIExtentDeleteOptions{
			Force: deleteOpts.ForceDelete,
		}

		if volInfo.ISCSITargetID > 0 {
			if err := s.driver.Client().DeleteISCSITarget(ctx, volInfo.ISCSITargetID, targetDeleteOpts); err != nil {
				s.driver.Log().V(LogLevelDebug).Error(err, "Failed to delete iSCSI target", "targetId", volInfo.ISCSITargetID)
			}
		}
		if volInfo.ISCSIExtentID > 0 && !deleteOpts.DeleteExtentsWithTarget {
			if err := s.driver.Client().DeleteISCSIExtent(ctx, volInfo.ISCSIExtentID, extentDeleteOpts); err != nil {
				s.driver.Log().V(LogLevelDebug).Error(err, "Failed to delete iSCSI extent", "extentId", volInfo.ISCSIExtentID)
			}
		}
		if volInfo.ISCSIAuthID > 0 {
			if err := s.driver.Client().DeleteISCSIAuth(ctx, volInfo.ISCSIAuthID); err != nil {
				s.driver.Log().V(LogLevelDebug).Error(err, "Failed to delete iSCSI auth", "authId", volInfo.ISCSIAuthID)
			}
		}
		if volInfo.ISCSIInitiatorID > 0 {
			if err := s.driver.Client().DeleteISCSIInitiator(ctx, volInfo.ISCSIInitiatorID); err != nil {
				s.driver.Log().V(LogLevelDebug).Error(err, "Failed to delete iSCSI initiator", "initiatorId", volInfo.ISCSIInitiatorID)
			}
		}
	}

	// Always try to delete NFS share by path to ensure cleanup before dataset deletion
	// This handles cases where volInfo is nil or NFSShareID wasn't stored
	nfsPath := fmt.Sprintf("%s/%s", DefaultMountpoint, datasetPath)
	if share, err := s.driver.Client().GetNFSShareByPath(ctx, nfsPath); err == nil && share != nil {
		s.driver.Log().V(LogLevelDebug).Info("Deleting NFS share before dataset", "shareId", share.ID, "path", nfsPath)
		if err := s.driver.Client().DeleteNFSShare(ctx, share.ID); err != nil {
			s.driver.Log().V(LogLevelDebug).Error(err, "Failed to delete NFS share", "shareId", share.ID)
		} else {
			// Allow NFS server time to release client state before dataset deletion
			// Similar to csi-driver-nfs's timeout-based cleanup approach
			time.Sleep(nfsShareCleanupDelay)
		}
	}

	// Delete snapshot tasks
	s.deleteSnapshotTaskForDataset(ctx, datasetPath)

	// Delete the dataset
	err = s.driver.Client().DeleteDataset(ctx, datasetPath, &client.DatasetDeleteOptions{Recursive: true, Force: true})
	if err != nil && !client.IsNotFoundError(err) {
		return nil, status.Errorf(codes.Internal, "failed to delete volume: %v", err)
	}

	s.driver.Log().V(LogLevelDebug).Info("Volume deleted successfully", "volumeId", req.VolumeId)
	return &csi.DeleteVolumeResponse{}, nil
}

// ControllerPublishVolume returns the connection info needed for node staging (portal, IQN, or NFS path).
func (s *ControllerServer) ControllerPublishVolume(ctx context.Context, req *csi.ControllerPublishVolumeRequest) (*csi.ControllerPublishVolumeResponse, error) {
	ctx, cancel := withTimeout(ctx, shortOperationTimeout)
	defer cancel()

	s.driver.Log().V(LogLevelDebug).Info("ControllerPublishVolume called", "volumeId", req.VolumeId, "nodeId", req.NodeId)

	// Validate required parameters
	if req.VolumeId == "" {
		return nil, status.Error(codes.InvalidArgument, "volume ID is required")
	}
	if req.NodeId == "" {
		return nil, status.Error(codes.InvalidArgument, "node ID is required")
	}
	if req.VolumeCapability == nil {
		return nil, status.Error(codes.InvalidArgument, "volume capability is required")
	}

	// Validate node exists - in single-node deployments, check against our node ID
	// In multi-node deployments, this should be expanded to track all registered nodes
	if req.NodeId != s.driver.NodeID() {
		return nil, status.Errorf(codes.NotFound, "node %s not found", req.NodeId)
	}

	// Parse volume ID to get dataset path
	pool, name, err := s.driver.ParseVolumeID(req.VolumeId)
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "volume not found: invalid volume ID format")
	}
	datasetPath := fmt.Sprintf("%s/%s", pool, name)

	// Check if volume exists by querying TrueNAS
	dataset, err := s.driver.Client().GetDataset(ctx, datasetPath)
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "volume %s not found: %v", req.VolumeId, err)
	}

	// Validate volume capability
	isBlockVolume := req.VolumeCapability.GetBlock() != nil
	if !isBlockVolume && req.VolumeCapability.GetMount() == nil {
		return nil, status.Error(codes.InvalidArgument, "either block or mount volume capability is required")
	}

	publishContext := make(map[string]string)

	// Try to get volume info from TrueNAS for complete publish context
	volInfo, volInfoErr := s.driver.GetVolumeInfo(req.VolumeId)
	if volInfoErr != nil {
		s.driver.Log().Info("GetVolumeInfo failed", "volumeId", req.VolumeId, "error", volInfoErr)
	} else if volInfo != nil {
		s.driver.Log().Info("GetVolumeInfo returned", "volumeId", req.VolumeId, "protocol", volInfo.Protocol, "targetIQN", volInfo.TargetIQN, "nfsPath", volInfo.NFSPath)
	}

	// Check if we got valid volume info with complete iSCSI details
	// (reconstructVolumeFromTrueNAS may return volInfo with empty TargetIQN if extent lookup failed)
	hasValidISCSIInfo := volInfo != nil && volInfo.Protocol == ProtocolISCSI && volInfo.TargetIQN != ""
	hasValidNFSInfo := volInfo != nil && volInfo.Protocol == ProtocolNFS && volInfo.NFSPath != ""

	if hasValidISCSIInfo {
		// Block volumes only supported for iSCSI
		if isBlockVolume && volInfo.Protocol != ProtocolISCSI {
			return nil, status.Error(codes.InvalidArgument, "block volume capability only supported for iSCSI")
		}
		publishContext[PublishContextProtocol] = volInfo.Protocol
		publishContext[PublishContextTargetPortal] = volInfo.TargetPortal
		publishContext[PublishContextTargetIQN] = volInfo.TargetIQN
		publishContext[PublishContextLUN] = fmt.Sprintf("%d", volInfo.LUN)
	} else if hasValidNFSInfo {
		publishContext[PublishContextProtocol] = volInfo.Protocol
		publishContext[PublishContextNFSServer] = volInfo.VolumeContext["nfsServer"]
		publishContext[PublishContextNFSPath] = volInfo.NFSPath
	} else {
		// Volume exists in TrueNAS but not in cache - determine protocol from dataset type
		if dataset.Type == "VOLUME" {
			// iSCSI ZVOL - reconstruct iSCSI info from TrueNAS
			publishContext[PublishContextProtocol] = ProtocolISCSI
			publishContext[PublishContextTargetPortal] = s.driver.ISCSIPortal()

			// Query TrueNAS for iSCSI target info
			zvolPath := fmt.Sprintf("zvol/%s", datasetPath)
			s.driver.Log().Info("Looking up iSCSI extent", "volumeId", req.VolumeId, "zvolPath", zvolPath)
			extent, err := s.driver.Client().GetISCSIExtentByDisk(ctx, zvolPath)
			if err != nil {
				s.driver.Log().Info("Could not find iSCSI extent for volume", "volumeId", req.VolumeId, "zvolPath", zvolPath, "error", err)
			} else {
				// Found extent, now find the target-extent association
				targetExtent, err := s.driver.Client().GetISCSITargetExtentByExtent(ctx, extent.ID)
				if err != nil {
					s.driver.Log().V(LogLevelDebug).Info("Could not find target-extent association", "volumeId", req.VolumeId, "extentId", extent.ID, "error", err)
				} else {
					// Found association, get the target details
					target, err := s.driver.Client().GetISCSITargetByID(ctx, targetExtent.Target)
					if err != nil {
						s.driver.Log().V(LogLevelDebug).Info("Could not find iSCSI target", "volumeId", req.VolumeId, "targetId", targetExtent.Target, "error", err)
					} else {
						// Successfully reconstructed iSCSI info
						fullIQN := fmt.Sprintf("%s:%s", s.driver.ISCSIIQNBase(), target.Name)
						publishContext[PublishContextTargetIQN] = fullIQN
						publishContext[PublishContextLUN] = fmt.Sprintf("%d", targetExtent.LunID)
						s.driver.Log().Info("Reconstructed iSCSI info from TrueNAS", "volumeId", req.VolumeId, "targetIQN", fullIQN, "lun", targetExtent.LunID)
					}
				}
			}
		} else {
			// NFS filesystem - block volumes not supported
			if isBlockVolume {
				return nil, status.Error(codes.InvalidArgument, "block volume capability only supported for iSCSI")
			}
			publishContext[PublishContextProtocol] = ProtocolNFS
			mountpoint := dataset.Mountpoint
			if mountpoint == "" {
				mountpoint = filepath.Join(DefaultMountpoint, datasetPath)
			}
			publishContext[PublishContextNFSServer] = s.driver.NFSServer()
			publishContext[PublishContextNFSPath] = mountpoint
		}
	}

	s.driver.Log().Info("ControllerPublishVolume returning", "volumeId", req.VolumeId, "publishContext", publishContext)
	return &csi.ControllerPublishVolumeResponse{
		PublishContext: publishContext,
	}, nil
}

// ControllerUnpublishVolume is a no-op for this driver since cleanup happens at node level.
func (s *ControllerServer) ControllerUnpublishVolume(ctx context.Context, req *csi.ControllerUnpublishVolumeRequest) (*csi.ControllerUnpublishVolumeResponse, error) {
	s.driver.Log().V(LogLevelDebug).Info("ControllerUnpublishVolume called", "volumeId", req.VolumeId, "nodeId", req.NodeId)

	if req.VolumeId == "" {
		return nil, status.Error(codes.InvalidArgument, "volume ID is required")
	}

	return &csi.ControllerUnpublishVolumeResponse{}, nil
}

// ValidateVolumeCapabilities checks if the requested capabilities are supported for a volume.
func (s *ControllerServer) ValidateVolumeCapabilities(ctx context.Context, req *csi.ValidateVolumeCapabilitiesRequest) (*csi.ValidateVolumeCapabilitiesResponse, error) {
	s.driver.Log().V(LogLevelDebug).Info("ValidateVolumeCapabilities called", "volumeId", req.VolumeId)

	if req.VolumeId == "" {
		return nil, status.Error(codes.InvalidArgument, "volume ID is required")
	}

	if len(req.VolumeCapabilities) == 0 {
		return nil, status.Error(codes.InvalidArgument, "volume capabilities are required")
	}

	pool, name, err := s.driver.ParseVolumeID(req.VolumeId)
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "volume not found: invalid volume ID format")
	}

	datasetPath := fmt.Sprintf("%s/%s", pool, name)
	_, err = s.driver.Client().GetDataset(ctx, datasetPath)
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "volume not found: %v", err)
	}

	// Validate the requested capabilities
	if err := s.validateVolumeCapabilities(req.VolumeCapabilities); err != nil {
		return &csi.ValidateVolumeCapabilitiesResponse{
			Message: err.Error(),
		}, nil
	}

	// All capabilities validated successfully
	return &csi.ValidateVolumeCapabilitiesResponse{
		Confirmed: &csi.ValidateVolumeCapabilitiesResponse_Confirmed{
			VolumeContext:      req.VolumeContext,
			VolumeCapabilities: req.VolumeCapabilities,
			Parameters:         req.Parameters,
		},
	}, nil
}

// ListVolumes returns all volumes in the default pool.
func (s *ControllerServer) ListVolumes(ctx context.Context, req *csi.ListVolumesRequest) (*csi.ListVolumesResponse, error) {
	ctx, cancel := withTimeout(ctx, shortOperationTimeout)
	defer cancel()

	s.driver.Log().V(LogLevelDebug).Info("ListVolumes called", "startingToken", req.StartingToken, "maxEntries", req.MaxEntries)

	// Validate starting_token - we don't use pagination, so any non-empty token is invalid
	if req.StartingToken != "" {
		return nil, status.Error(codes.Aborted, "invalid starting_token")
	}

	// Query TrueNAS for all datasets in the default pool
	pool := s.driver.DefaultPool()
	datasets, err := s.driver.Client().ListDatasets(ctx, pool)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to list volumes: %v", err)
	}

	entries := make([]*csi.ListVolumesResponse_Entry, 0, len(datasets))
	for _, dataset := range datasets {
		// Skip the pool itself
		if dataset.Name == pool {
			continue
		}

		// Only include direct children (single slash after pool name)
		if strings.Count(dataset.Name, "/") != 1 {
			continue
		}

		// Determine capacity based on dataset type
		var capacityBytes int64
		if dataset.Type == "VOLUME" {
			// iSCSI ZVOL - use volsize
			capacityBytes = dataset.Volsize
		} else {
			// NFS filesystem - use refquota
			capacityBytes = dataset.RefQuota
		}

		entry := &csi.ListVolumesResponse_Entry{
			Volume: &csi.Volume{
				VolumeId:      dataset.Name,
				CapacityBytes: capacityBytes,
			},
		}
		entries = append(entries, entry)
	}

	return &csi.ListVolumesResponse{
		Entries: entries,
	}, nil
}

// GetCapacity returns the available storage capacity for the pool.
func (s *ControllerServer) GetCapacity(ctx context.Context, req *csi.GetCapacityRequest) (*csi.GetCapacityResponse, error) {
	ctx, cancel := withTimeout(ctx, shortOperationTimeout)
	defer cancel()

	s.driver.Log().V(LogLevelDebug).Info("GetCapacity called")

	pool := s.driver.DefaultPool()

	// Check topology constraints first
	if req.AccessibleTopology != nil {
		if topologyPool, ok := req.AccessibleTopology.Segments["topology.truenas.io/pool"]; ok {
			pool = topologyPool
		}
	}

	// Parameters can override topology
	if req.Parameters != nil {
		if p, ok := req.Parameters["pool"]; ok {
			pool = p
		}
	}

	available, err := s.driver.Client().GetAvailableSpace(ctx, pool)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to get capacity: %v", err)
	}

	return &csi.GetCapacityResponse{
		AvailableCapacity: available,
	}, nil
}

// ControllerGetCapabilities returns the capabilities of the controller service.
func (s *ControllerServer) ControllerGetCapabilities(ctx context.Context, req *csi.ControllerGetCapabilitiesRequest) (*csi.ControllerGetCapabilitiesResponse, error) {
	s.driver.Log().V(LogLevelDebug).Info("ControllerGetCapabilities called")

	return &csi.ControllerGetCapabilitiesResponse{
		Capabilities: s.driver.ControllerCaps(),
	}, nil
}

// CreateSnapshot creates a ZFS snapshot of the source volume.
func (s *ControllerServer) CreateSnapshot(ctx context.Context, req *csi.CreateSnapshotRequest) (*csi.CreateSnapshotResponse, error) {
	ctx, cancel := withTimeout(ctx, defaultOperationTimeout)
	defer cancel()

	s.driver.Log().V(LogLevelDebug).Info("CreateSnapshot called", "sourceVolumeId", req.SourceVolumeId, "name", req.Name)

	if req.Name == "" {
		return nil, status.Error(codes.InvalidArgument, "snapshot name is required")
	}

	if req.SourceVolumeId == "" {
		return nil, status.Error(codes.InvalidArgument, "source volume ID is required")
	}

	volInfo, err := s.driver.GetVolumeInfo(req.SourceVolumeId)
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "volume not found: %v", err)
	}

	snapshotName := SanitizeVolumeName(req.Name)
	expectedSnapshotID := fmt.Sprintf("%s@%s", volInfo.DatasetPath, snapshotName)

	// Check if a snapshot with this name already exists on ANY volume
	// CSI requires snapshot names to be unique system-wide
	existingSnapshot, err := s.driver.Client().FindSnapshotByName(ctx, snapshotName)
	if err != nil {
		s.driver.Log().V(LogLevelDebug).Info("Error finding snapshot by name, continuing", "name", snapshotName, "error", err)
	}
	if existingSnapshot != nil {
		// A snapshot with this name exists somewhere
		if existingSnapshot.ID == expectedSnapshotID {
			// Snapshot exists on the requested source volume - return it (idempotent)
			s.driver.Log().V(LogLevelDebug).Info("Snapshot already exists on source volume", "snapshotId", existingSnapshot.ID)
			return &csi.CreateSnapshotResponse{
				Snapshot: &csi.Snapshot{
					SnapshotId:     existingSnapshot.ID,
					SourceVolumeId: req.SourceVolumeId,
					CreationTime:   timestamppb.Now(),
					ReadyToUse:     true,
				},
			}, nil
		}
		// Snapshot with this name exists on a DIFFERENT volume
		s.driver.Log().V(LogLevelDebug).Info("Snapshot name already exists on different volume",
			"requestedName", snapshotName,
			"existingSnapshotId", existingSnapshot.ID,
			"requestedSourceVolume", req.SourceVolumeId)
		return nil, status.Errorf(codes.AlreadyExists,
			"snapshot name %s already exists on a different source volume (existing: %s)",
			req.Name, existingSnapshot.Dataset)
	}

	// Create the new snapshot
	snapshot, err := s.driver.Client().CreateSnapshot(ctx, volInfo.DatasetPath, snapshotName, false)
	if err != nil {
		if strings.Contains(err.Error(), "already exists") {
			// Snapshot with this name exists but on a different volume
			return nil, status.Errorf(codes.AlreadyExists,
				"snapshot name %s already exists on a different source volume", req.Name)
		}
		return nil, status.Errorf(codes.Internal, "failed to create snapshot: %v", err)
	}

	return &csi.CreateSnapshotResponse{
		Snapshot: &csi.Snapshot{
			SnapshotId:     snapshot.ID,
			SourceVolumeId: req.SourceVolumeId,
			CreationTime:   timestamppb.Now(),
			ReadyToUse:     true,
		},
	}, nil
}

// DeleteSnapshot deletes a ZFS snapshot.
func (s *ControllerServer) DeleteSnapshot(ctx context.Context, req *csi.DeleteSnapshotRequest) (*csi.DeleteSnapshotResponse, error) {
	ctx, cancel := withTimeout(ctx, shortOperationTimeout)
	defer cancel()

	s.driver.Log().V(LogLevelDebug).Info("DeleteSnapshot called", "snapshotId", req.SnapshotId)

	if req.SnapshotId == "" {
		return nil, status.Error(codes.InvalidArgument, "snapshot ID is required")
	}

	// Validate snapshot ID format (should contain @)
	if !strings.Contains(req.SnapshotId, "@") {
		// Invalid format - treat as already deleted (idempotent)
		s.driver.Log().V(LogLevelDebug).Info("Invalid snapshot ID format, treating as already deleted", "snapshotId", req.SnapshotId)
		return &csi.DeleteSnapshotResponse{}, nil
	}

	err := s.driver.Client().DeleteSnapshot(ctx, req.SnapshotId)
	if err != nil {
		if client.IsNotFoundError(err) {
			return &csi.DeleteSnapshotResponse{}, nil
		}
		// For other errors, also return success for idempotency
		s.driver.Log().V(LogLevelDebug).Info("Snapshot delete error, treating as already deleted", "snapshotId", req.SnapshotId, "error", err)
		return &csi.DeleteSnapshotResponse{}, nil
	}

	return &csi.DeleteSnapshotResponse{}, nil
}

// ListSnapshots returns snapshots, optionally filtered by source volume or snapshot ID.
func (s *ControllerServer) ListSnapshots(ctx context.Context, req *csi.ListSnapshotsRequest) (*csi.ListSnapshotsResponse, error) {
	ctx, cancel := withTimeout(ctx, shortOperationTimeout)
	defer cancel()

	s.driver.Log().V(LogLevelDebug).Info("ListSnapshots called", "sourceVolumeId", req.SourceVolumeId, "snapshotId", req.SnapshotId,
		"maxEntries", req.MaxEntries, "startingToken", req.StartingToken)

	entries := []*csi.ListSnapshotsResponse_Entry{}

	// Parse starting token for pagination (format: "offset")
	startOffset := 0
	if req.StartingToken != "" {
		var err error
		startOffset, err = strconv.Atoi(req.StartingToken)
		if err != nil || startOffset < 0 {
			return nil, status.Error(codes.Aborted, "invalid starting_token")
		}
	}

	// If snapshot ID is specified, look up that specific snapshot
	if req.SnapshotId != "" {
		// Snapshot ID format: dataset@snapshotname (e.g., tank/pvc-123@snap-456)
		parts := strings.SplitN(req.SnapshotId, "@", 2)
		if len(parts) == 2 {
			datasetPath := parts[0]
			snapshotName := parts[1]

			snapshots, err := s.driver.Client().ListSnapshots(ctx, datasetPath)
			if err != nil {
				// Return empty list if dataset doesn't exist
				s.driver.Log().V(LogLevelDebug).Info("Failed to list snapshots for dataset", "dataset", datasetPath, "error", err)
				return &csi.ListSnapshotsResponse{Entries: entries}, nil
			}

			s.driver.Log().V(LogLevelDebug).Info("Looking for snapshot", "requestedId", req.SnapshotId, "datasetPath", datasetPath,
				"snapshotName", snapshotName, "foundSnapshots", len(snapshots))

			for _, snap := range snapshots {
				s.driver.Log().V(LogLevelDebug).Info("Comparing snapshot", "snapId", snap.ID, "snapName", snap.Name,
					"requestedId", req.SnapshotId, "requestedName", snapshotName)

				// Compare by ID or by name (TrueNAS may format ID differently)
				if snap.ID == req.SnapshotId || snap.Name == snapshotName {
					// Derive source volume ID from dataset path
					sourceVolumeId := datasetPath
					entry := &csi.ListSnapshotsResponse_Entry{
						Snapshot: &csi.Snapshot{
							SnapshotId:     snap.ID,
							SourceVolumeId: sourceVolumeId,
							CreationTime:   timestamppb.Now(),
							ReadyToUse:     true,
						},
					}
					entries = append(entries, entry)
					s.driver.Log().V(LogLevelDebug).Info("Found matching snapshot", "snapshotId", snap.ID)
					break
				}
			}
		}
		return &csi.ListSnapshotsResponse{Entries: entries}, nil
	}

	// If source volume ID is specified, list all snapshots for that volume
	if req.SourceVolumeId != "" {
		// Try cache first, then parse volume ID directly
		var datasetPath string
		volInfo, err := s.driver.GetVolumeInfo(req.SourceVolumeId)
		if err == nil {
			datasetPath = volInfo.DatasetPath
		} else {
			// Volume not in cache - parse volume ID directly
			pool, name, parseErr := s.driver.ParseVolumeID(req.SourceVolumeId)
			if parseErr != nil {
				// Invalid volume ID format - return empty (volume doesn't exist)
				return &csi.ListSnapshotsResponse{Entries: entries}, nil
			}
			datasetPath = fmt.Sprintf("%s/%s", pool, name)
		}

		snapshots, err := s.driver.Client().ListSnapshots(ctx, datasetPath)
		if err != nil {
			// Return empty list if dataset doesn't exist
			s.driver.Log().V(LogLevelDebug).Info("Failed to list snapshots for volume", "volumeId", req.SourceVolumeId, "error", err)
			return &csi.ListSnapshotsResponse{Entries: entries}, nil
		}

		// Apply pagination
		totalSnapshots := len(snapshots)
		if startOffset > totalSnapshots {
			return nil, status.Error(codes.Aborted, "invalid starting_token: offset exceeds total snapshots")
		}

		paginatedSnapshots := snapshots[startOffset:]
		maxEntries := int(req.MaxEntries)
		var nextToken string
		if maxEntries > 0 && len(paginatedSnapshots) > maxEntries {
			paginatedSnapshots = paginatedSnapshots[:maxEntries]
			nextToken = strconv.Itoa(startOffset + maxEntries)
		}

		for _, snap := range paginatedSnapshots {
			entry := &csi.ListSnapshotsResponse_Entry{
				Snapshot: &csi.Snapshot{
					SnapshotId:     snap.ID,
					SourceVolumeId: req.SourceVolumeId,
					CreationTime:   timestamppb.Now(),
					ReadyToUse:     true,
				},
			}
			entries = append(entries, entry)
		}
		return &csi.ListSnapshotsResponse{Entries: entries, NextToken: nextToken}, nil
	}

	// Neither snapshot ID nor source volume ID specified - list all snapshots
	s.driver.Log().V(LogLevelDebug).Info("Listing all snapshots")
	allSnapshots, err := s.driver.Client().ListAllSnapshots(ctx)
	if err != nil {
		s.driver.Log().V(LogLevelDebug).Info("Failed to list all snapshots", "error", err)
		return &csi.ListSnapshotsResponse{Entries: entries}, nil
	}

	// Apply pagination
	totalSnapshots := len(allSnapshots)

	// Validate starting offset
	if startOffset > totalSnapshots {
		return nil, status.Error(codes.Aborted, "invalid starting_token: offset exceeds total snapshots")
	}

	// Slice from offset
	paginatedSnapshots := allSnapshots[startOffset:]

	// Apply max_entries limit
	maxEntries := int(req.MaxEntries)
	var nextToken string
	if maxEntries > 0 && len(paginatedSnapshots) > maxEntries {
		paginatedSnapshots = paginatedSnapshots[:maxEntries]
		nextToken = strconv.Itoa(startOffset + maxEntries)
	}

	for _, snap := range paginatedSnapshots {
		// Use the dataset as the source volume ID
		sourceVolumeID := snap.Dataset
		entry := &csi.ListSnapshotsResponse_Entry{
			Snapshot: &csi.Snapshot{
				SnapshotId:     snap.ID,
				SourceVolumeId: sourceVolumeID,
				CreationTime:   timestamppb.Now(),
				ReadyToUse:     true,
			},
		}
		entries = append(entries, entry)
	}

	return &csi.ListSnapshotsResponse{
		Entries:   entries,
		NextToken: nextToken,
	}, nil
}

// ControllerExpandVolume expands the ZFS dataset or ZVOL capacity.
func (s *ControllerServer) ControllerExpandVolume(ctx context.Context, req *csi.ControllerExpandVolumeRequest) (*csi.ControllerExpandVolumeResponse, error) {
	ctx, cancel := withTimeout(ctx, defaultOperationTimeout)
	defer cancel()

	s.driver.Log().V(LogLevelDebug).Info("ControllerExpandVolume called", "volumeId", req.VolumeId)

	if req.VolumeId == "" {
		return nil, status.Error(codes.InvalidArgument, "volume ID is required")
	}

	if req.CapacityRange == nil || req.CapacityRange.RequiredBytes == 0 {
		return nil, status.Error(codes.InvalidArgument, "capacity range is required")
	}

	volInfo, err := s.driver.GetVolumeInfo(req.VolumeId)
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "volume not found: %v", err)
	}

	newSize := req.CapacityRange.RequiredBytes
	if newSize <= volInfo.CapacityBytes {
		return &csi.ControllerExpandVolumeResponse{
			CapacityBytes:         volInfo.CapacityBytes,
			NodeExpansionRequired: false,
		}, nil
	}

	updates := &client.DatasetUpdateOptions{}
	if volInfo.Protocol == ProtocolISCSI {
		updates.Volsize = &newSize
		updates.RefReservation = &newSize // Must update reservation to match volsize
	} else {
		updates.RefQuota = &newSize
	}

	err = s.driver.Client().UpdateDataset(ctx, volInfo.DatasetPath, updates)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to expand volume: %v", err)
	}

	return &csi.ControllerExpandVolumeResponse{
		CapacityBytes:         newSize,
		NodeExpansionRequired: volInfo.Protocol == ProtocolISCSI,
	}, nil
}

// ControllerGetVolume returns volume information including health status.
func (s *ControllerServer) ControllerGetVolume(ctx context.Context, req *csi.ControllerGetVolumeRequest) (*csi.ControllerGetVolumeResponse, error) {
	s.driver.Log().V(LogLevelDebug).Info("ControllerGetVolume called", "volumeId", req.VolumeId)

	if req.VolumeId == "" {
		return nil, status.Error(codes.InvalidArgument, "volume ID is required")
	}

	volInfo, err := s.driver.GetVolumeInfo(req.VolumeId)
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "volume not found: %v", err)
	}

	dataset, err := s.driver.Client().GetDataset(ctx, volInfo.DatasetPath)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to get volume info: %v", err)
	}

	abnormal := false
	message := "Volume is healthy"

	if dataset.Used > dataset.Available {
		abnormal = true
		message = "Volume is running out of space"
	}

	return &csi.ControllerGetVolumeResponse{
		Volume: &csi.Volume{
			VolumeId:      volInfo.ID,
			CapacityBytes: volInfo.CapacityBytes,
			VolumeContext: volInfo.VolumeContext,
		},
		Status: &csi.ControllerGetVolumeResponse_VolumeStatus{
			VolumeCondition: &csi.VolumeCondition{
				Abnormal: abnormal,
				Message:  message,
			},
		},
	}, nil
}

// parseEncryptionOptions extracts encryption configuration from StorageClass parameters.
// Parameters:
//   - encryption: "true" to enable encryption
//   - encryption.algorithm: AES-256-GCM (default), AES-128-CCM, AES-192-CCM, AES-256-CCM, AES-128-GCM, AES-192-GCM
//   - encryption.passphrase: Passphrase for encryption (min 8 chars)
//   - encryption.key: Hex-encoded key (64 chars) - alternative to passphrase
//   - encryption.generateKey: "true" to auto-generate key
//   - encryption.pbkdf2iters: PBKDF2 iterations (default 350000, min 100000)
//
// Returns nil if encryption is not enabled.
func parseEncryptionOptions(parameters map[string]string) *client.EncryptionOptions {
	enabled, ok := parameters["encryption"]
	if !ok || !strings.EqualFold(enabled, "true") {
		return nil
	}

	opts := &client.EncryptionOptions{
		Algorithm: defaultEncryptionAlgorithm,
	}

	// Algorithm
	if val, ok := parameters["encryption.algorithm"]; ok && val != "" {
		opts.Algorithm = strings.ToUpper(val)
	}

	// Passphrase
	if val, ok := parameters["encryption.passphrase"]; ok && val != "" {
		opts.Passphrase = &val
	}

	// Key (hex-encoded, 64 chars)
	if val, ok := parameters["encryption.key"]; ok && val != "" {
		opts.Key = &val
	}

	// Generate key automatically
	if val, ok := parameters["encryption.generateKey"]; ok && strings.EqualFold(val, "true") {
		opts.GenerateKey = true
	}

	// PBKDF2 iterations
	if val, ok := parameters["encryption.pbkdf2iters"]; ok {
		if iters, err := strconv.Atoi(val); err == nil && iters >= 100000 {
			opts.Pbkdf2iters = iters
		}
	}

	// If no key source is specified, default to generating a key
	if opts.Passphrase == nil && opts.Key == nil && !opts.GenerateKey {
		opts.GenerateKey = true
	}

	return opts
}

// createSnapshotTaskFromParameters creates a periodic snapshot task based on StorageClass parameters.
// Parameters:
//   - snapshot.schedule: Cron schedule (e.g., "0 */6 * * *" for every 6 hours)
//   - snapshot.naming: Naming schema using strftime format (default: "auto-%Y-%m-%d_%H-%M")
//   - snapshot.retention: Number of retention units (default: 2)
//   - snapshot.retentionUnit: Retention unit - HOUR, DAY, WEEK, MONTH, YEAR (default: WEEK)
//   - snapshot.recursive: Whether to snapshot child datasets (default: false)
//
// Returns nil if no snapshot.schedule is specified.
func (s *ControllerServer) createSnapshotTaskFromParameters(ctx context.Context, datasetPath string, parameters map[string]string) (*client.SnapshotTask, error) {
	schedule, ok := parameters["snapshot.schedule"]
	if !ok || schedule == "" {
		return nil, nil // No snapshot schedule configured
	}

	// Parse cron schedule (minute hour dom month dow)
	parts := strings.Fields(schedule)
	if len(parts) < cronFieldCount {
		return nil, fmt.Errorf("invalid snapshot schedule format: expected 'minute hour dom month dow'")
	}

	taskSchedule := &client.SnapshotTaskSchedule{
		Minute: parts[0],
		Hour:   parts[1],
		Dom:    parts[2],
		Month:  parts[3],
		Dow:    parts[4],
		Begin:  "00:00",
		End:    "23:59",
	}

	// Naming schema
	namingSchema := defaultSnapshotNamingSchema
	if val, ok := parameters["snapshot.naming"]; ok && val != "" {
		namingSchema = val
	}

	// Retention value
	retentionValue := defaultSnapshotRetention
	if val, ok := parameters["snapshot.retention"]; ok {
		if parsed, err := strconv.Atoi(val); err == nil && parsed > 0 {
			retentionValue = parsed
		}
	}

	// Retention unit
	retentionUnit := defaultSnapshotRetentionUnit
	if val, ok := parameters["snapshot.retentionUnit"]; ok {
		unit := strings.ToUpper(val)
		switch unit {
		case "HOUR", "DAY", "WEEK", "MONTH", "YEAR":
			retentionUnit = unit
		}
	}

	// Recursive (default: false)
	recursive := false
	if val, ok := parameters["snapshot.recursive"]; ok {
		recursive = strings.EqualFold(val, "true")
	}

	opts := &client.SnapshotTaskCreateOptions{
		Dataset:       datasetPath,
		Recursive:     recursive,
		LifetimeValue: retentionValue,
		LifetimeUnit:  retentionUnit,
		Enabled:       true,
		NamingSchema:  namingSchema,
		AllowEmpty:    true,
		Schedule:      taskSchedule,
	}

	task, err := s.driver.Client().CreateSnapshotTask(ctx, opts)
	if err != nil {
		return nil, fmt.Errorf("failed to create snapshot task: %w", err)
	}

	s.driver.Log().V(LogLevelDebug).Info("Created snapshot task for volume", "dataset", datasetPath, "taskId", task.ID, "schedule", schedule)
	return task, nil
}

// deleteSnapshotTaskForDataset deletes any snapshot tasks associated with a dataset.
func (s *ControllerServer) deleteSnapshotTaskForDataset(ctx context.Context, datasetPath string) {
	task, err := s.driver.Client().GetSnapshotTaskByDataset(ctx, datasetPath)
	if err != nil {
		// Task not found or error - nothing to delete
		return
	}

	if err := s.driver.Client().DeleteSnapshotTask(ctx, task.ID, nil); err != nil {
		s.driver.Log().V(LogLevelDebug).Error(err, "Failed to delete snapshot task", "taskId", task.ID, "dataset", datasetPath)
	} else {
		s.driver.Log().V(LogLevelDebug).Info("Deleted snapshot task for volume", "taskId", task.ID, "dataset", datasetPath)
	}
}

// ControllerModifyVolume is a no-op since volume modification is not currently supported.
func (s *ControllerServer) ControllerModifyVolume(ctx context.Context, req *csi.ControllerModifyVolumeRequest) (*csi.ControllerModifyVolumeResponse, error) {
	s.driver.Log().V(LogLevelDebug).Info("ControllerModifyVolume called", "volumeId", req.VolumeId)
	return &csi.ControllerModifyVolumeResponse{}, nil
}
