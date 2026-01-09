package driver

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/go-logr/logr"
	iscsilib "github.com/kubernetes-csi/csi-lib-iscsi/iscsi"

	"k8s.io/mount-utils"
)

// StorageClass parameter keys for iSCSI configuration
const (
	paramCHAPUsername       = "iscsi.chapUsername"
	paramCHAPPassword       = "iscsi.chapPassword"
	paramCHAPUsernameIn     = "iscsi.chapUsernameIn"
	paramCHAPPasswordIn     = "iscsi.chapPasswordIn"
	paramMultipathEnabled   = "iscsi.multipathEnabled"
	paramPersistentSessions = "iscsi.persistentSessions"

	// Directory for storing csi-lib-iscsi connector files
	connectorDir = "/var/lib/truenas-csi/connectors"

	// iSCSI connection settings
	iscsiRetryCount    = 10 // number of login attempts
	iscsiCheckInterval = 1  // seconds between retries
)

// ISCSIHandler implements the ProtocolHandler interface for iSCSI volumes
type ISCSIHandler struct {
	mounter *mount.SafeFormatAndMount
	resizer *mount.ResizeFs
	log     logr.Logger
}

// ISCSIConfig holds iSCSI-specific configuration parsed from volume/publish contexts
type ISCSIConfig struct {
	TargetPortal       string
	TargetIQN          string
	LUN                int32
	CHAPUsername       string
	CHAPPassword       string
	CHAPUsernameIn     string
	CHAPPasswordIn     string
	MultipathEnabled   bool
	PersistentSessions bool
}

// NewISCSIHandler creates a new iSCSI protocol handler
func NewISCSIHandler(mounter *mount.SafeFormatAndMount, log logr.Logger) *ISCSIHandler {
	// Ensure connector directory exists
	os.MkdirAll(connectorDir, 0750)

	return &ISCSIHandler{
		mounter: mounter,
		resizer: mount.NewResizeFs(mounter.Exec),
		log:     log,
	}
}

// Protocol returns the protocol name
func (h *ISCSIHandler) Protocol() string {
	return "iscsi"
}

// connectorPath returns the path for storing connector info for a volume
func connectorPath(volumeID string) string {
	return filepath.Join(connectorDir, fmt.Sprintf("%s.connector", sanitizeISCSIVolumeID(volumeID)))
}

// parseISCSIConfig extracts iSCSI configuration from publish and volume contexts
func parseISCSIConfig(publishContext, volumeContext map[string]string) (*ISCSIConfig, error) {
	config := &ISCSIConfig{
		TargetPortal: publishContext["targetPortal"],
		TargetIQN:    publishContext["targetIQN"],
	}

	// Parse LUN
	if lunStr := publishContext["lun"]; lunStr != "" {
		lun, err := strconv.ParseInt(lunStr, 10, 32)
		if err != nil {
			return nil, fmt.Errorf("invalid LUN value: %v", err)
		}
		config.LUN = int32(lun)
	}

	// CHAP credentials from volume context (StorageClass parameters)
	config.CHAPUsername = volumeContext[paramCHAPUsername]
	config.CHAPPassword = volumeContext[paramCHAPPassword]
	config.CHAPUsernameIn = volumeContext[paramCHAPUsernameIn]
	config.CHAPPasswordIn = volumeContext[paramCHAPPasswordIn]

	// Multipath and persistent sessions
	if val := volumeContext[paramMultipathEnabled]; strings.EqualFold(val, "true") {
		config.MultipathEnabled = true
	}
	if val := volumeContext[paramPersistentSessions]; strings.EqualFold(val, "true") {
		config.PersistentSessions = true
	}

	return config, nil
}

// buildConnector creates a csi-lib-iscsi Connector from our config
func (h *ISCSIHandler) buildConnector(volumeID string, config *ISCSIConfig) *iscsilib.Connector {
	connector := &iscsilib.Connector{
		VolumeName:    volumeID,
		TargetIqn:     config.TargetIQN,
		TargetPortals: []string{config.TargetPortal},
		Lun:           config.LUN,
		RetryCount:    iscsiRetryCount,
		CheckInterval: iscsiCheckInterval,
		DoDiscovery:   true,
	}

	// Configure CHAP authentication
	if config.CHAPUsername != "" && config.CHAPPassword != "" {
		connector.AuthType = "chap"
		connector.DiscoverySecrets = iscsilib.Secrets{
			UserName:   config.CHAPUsername,
			Password:   config.CHAPPassword,
			UserNameIn: config.CHAPUsernameIn,
			PasswordIn: config.CHAPPasswordIn,
		}
		connector.SessionSecrets = connector.DiscoverySecrets
	}

	return connector
}

// Stage implements iSCSI volume staging (login and device setup)
func (h *ISCSIHandler) Stage(ctx context.Context, req *StageRequest) (*StageResult, error) {
	h.log.V(LogLevelDebug).Info("iSCSI Stage", "volumeId", req.VolumeID, "stagingPath", req.StagingPath, "isBlock", req.IsBlockVolume)

	// Parse iSCSI configuration
	config, err := parseISCSIConfig(req.PublishContext, req.VolumeContext)
	if err != nil {
		return nil, fmt.Errorf("failed to parse iSCSI config: %w (check publish context from controller)", err)
	}

	if config.TargetPortal == "" || config.TargetIQN == "" {
		return nil, fmt.Errorf("iSCSI target portal and IQN are required (check StorageClass parameters and controller publish context)")
	}

	// Build connector for csi-lib-iscsi
	connector := h.buildConnector(req.VolumeID, config)

	// Connect to iSCSI target
	h.log.V(LogLevelDebug).Info("Connecting to iSCSI target", "portal", config.TargetPortal, "iqn", config.TargetIQN, "lun", config.LUN)
	devicePath, err := iscsilib.Connect(*connector)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to iSCSI target %s at %s: %w", config.TargetIQN, config.TargetPortal, err)
	}

	h.log.V(LogLevelDebug).Info("iSCSI connected", "device", devicePath)

	// Persist connector info for cleanup on unstage
	// Note: csi-lib-iscsi Connect() takes a value copy, so device info isn't
	// populated in our connector. We only need TargetIqn and TargetPortals for
	// disconnection, which we already have.
	cpath := connectorPath(req.VolumeID)
	if err := iscsilib.PersistConnector(connector, cpath); err != nil {
		h.log.Info("Failed to persist connector info", "error", err)
	}

	// For block volumes, skip formatting and mounting - just return the device path
	if req.IsBlockVolume {
		h.log.V(LogLevelDebug).Info("iSCSI block volume staged (no filesystem)", "volumeId", req.VolumeID, "device", devicePath)
		return &StageResult{DevicePath: devicePath}, nil
	}

	// Get filesystem type for mount volumes
	fsType := req.FSType
	if fsType == "" {
		fsType = "ext4"
	}

	// Create staging directory
	if err := os.MkdirAll(req.StagingPath, 0750); err != nil {
		return nil, fmt.Errorf("failed to create staging directory: %w", err)
	}

	// Format and mount device using SafeFormatAndMount
	h.log.V(LogLevelDebug).Info("FormatAndMount device", "device", devicePath, "stagingPath", req.StagingPath, "fsType", fsType)
	if err := h.mounter.FormatAndMount(devicePath, req.StagingPath, fsType, req.MountFlags); err != nil {
		return nil, fmt.Errorf("failed to format and mount device: %w", err)
	}

	h.log.V(LogLevelDebug).Info("iSCSI volume staged", "volumeId", req.VolumeID, "stagingPath", req.StagingPath)
	return &StageResult{DevicePath: devicePath}, nil
}

// Unstage implements iSCSI volume unstaging (logout and cleanup)
func (h *ISCSIHandler) Unstage(ctx context.Context, req *UnstageRequest) error {
	h.log.V(LogLevelDebug).Info("iSCSI Unstage", "volumeId", req.VolumeID, "stagingPath", req.StagingPath)

	// Check if mounted
	notMounted, err := h.mounter.IsLikelyNotMountPoint(req.StagingPath)
	if err != nil {
		if os.IsNotExist(err) {
			h.log.V(LogLevelDebug).Info("Staging path does not exist, considering unstaged", "stagingPath", req.StagingPath)
			// Still try to disconnect iSCSI and cleanup connector
			h.cleanupISCSISession(req.VolumeID)
			return nil
		}
		return fmt.Errorf("failed to check mount point: %w", err)
	}

	// Unmount if mounted
	if !notMounted {
		if err := h.mounter.Unmount(req.StagingPath); err != nil {
			return fmt.Errorf("failed to unmount staging path: %w", err)
		}
	}

	// Disconnect iSCSI session and cleanup
	h.cleanupISCSISession(req.VolumeID)

	// Remove staging directory
	os.Remove(req.StagingPath)

	h.log.V(LogLevelDebug).Info("iSCSI volume unstaged", "volumeId", req.VolumeID)
	return nil
}

// cleanupISCSISession disconnects the iSCSI session and removes the connector file
func (h *ISCSIHandler) cleanupISCSISession(volumeID string) {
	cpath := connectorPath(volumeID)
	if _, err := os.Stat(cpath); err != nil {
		return // No connector file, nothing to clean up
	}

	// Try to load connector - GetConnectorFromFile may fail validation if
	// mountTargetDevice is nil, but we only need TargetIqn and TargetPortals
	// for disconnect, so try to read the file directly as fallback
	connector, err := iscsilib.GetConnectorFromFile(cpath)
	if err != nil {
		h.log.V(LogLevelDebug).Info("GetConnectorFromFile failed, trying direct read", "path", cpath, "error", err)
		// Read file directly and unmarshal to get IQN and portals
		connector = h.readConnectorDirect(cpath)
	}

	if connector != nil && connector.TargetIqn != "" {
		iscsilib.Disconnect(connector.TargetIqn, connector.TargetPortals)
		h.log.V(LogLevelDebug).Info("Disconnected from iSCSI target", "targetIqn", connector.TargetIqn)
	}

	// Remove connector file
	os.Remove(cpath)
}

// readConnectorDirect reads a connector file without validation
func (h *ISCSIHandler) readConnectorDirect(path string) *iscsilib.Connector {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil
	}

	// The connector file is JSON, decode just the fields we need
	var connector iscsilib.Connector
	if err := json.Unmarshal(data, &connector); err != nil {
		h.log.V(LogLevelDebug).Info("Failed to unmarshal connector", "error", err)
		return nil
	}

	return &connector
}

// Publish implements iSCSI volume publishing (bind mount from staging)
func (h *ISCSIHandler) Publish(ctx context.Context, req *PublishRequest) error {
	h.log.V(LogLevelDebug).Info("iSCSI Publish", "volumeId", req.VolumeID, "stagingPath", req.StagingPath, "targetPath", req.TargetPath, "isBlock", req.IsBlockVolume)

	// Handle block volume publishing
	if req.IsBlockVolume {
		return h.publishBlockVolume(ctx, req)
	}

	if req.StagingPath == "" {
		return fmt.Errorf("staging path is required for iSCSI mount volumes")
	}

	// Verify staging path is mounted
	notMounted, err := h.mounter.IsLikelyNotMountPoint(req.StagingPath)
	if err != nil || notMounted {
		return fmt.Errorf("volume not staged at %s", req.StagingPath)
	}

	// Create target directory
	if err := os.MkdirAll(req.TargetPath, 0750); err != nil {
		return fmt.Errorf("failed to create target directory: %w", err)
	}

	// Bind mount from staging to target
	mountOptions := []string{"bind"}
	if req.ReadOnly {
		mountOptions = append(mountOptions, "ro")
	}

	if err := h.mounter.Mount(req.StagingPath, req.TargetPath, "", mountOptions); err != nil {
		return fmt.Errorf("failed to bind mount: %w", err)
	}

	h.log.V(LogLevelDebug).Info("iSCSI volume published", "volumeId", req.VolumeID, "targetPath", req.TargetPath)
	return nil
}

// publishBlockVolume handles publishing raw block volumes
func (h *ISCSIHandler) publishBlockVolume(ctx context.Context, req *PublishRequest) error {
	// Get device path from connector file
	cpath := connectorPath(req.VolumeID)
	connector, err := iscsilib.GetConnectorFromFile(cpath)
	if err != nil {
		return fmt.Errorf("failed to load connector for block volume: %w", err)
	}

	if len(connector.Devices) == 0 {
		return fmt.Errorf("no devices found in connector for volume %s", req.VolumeID)
	}

	// Determine device path
	var devicePath string
	if connector.MountTargetDevice != nil && connector.MountTargetDevice.Name != "" {
		// Use multipath device if available
		devicePath = fmt.Sprintf("/dev/%s", connector.MountTargetDevice.Name)
	} else {
		// Use first device
		devicePath = fmt.Sprintf("/dev/%s", connector.Devices[0].Name)
	}

	h.log.V(LogLevelDebug).Info("Publishing block volume", "volumeId", req.VolumeID, "devicePath", devicePath, "targetPath", req.TargetPath)

	// Verify device exists
	if _, err := os.Stat(devicePath); err != nil {
		return fmt.Errorf("block device %s not found: %w", devicePath, err)
	}

	// Create parent directory of target path
	targetDir := filepath.Dir(req.TargetPath)
	if err := os.MkdirAll(targetDir, 0750); err != nil {
		return fmt.Errorf("failed to create target directory: %w", err)
	}

	// Create target file for block device mount
	file, err := os.OpenFile(req.TargetPath, os.O_CREATE|os.O_RDWR, 0660)
	if err != nil {
		return fmt.Errorf("failed to create target file: %w", err)
	}
	file.Close()

	// Bind mount block device to target file
	mountOptions := []string{"bind"}
	if req.ReadOnly {
		mountOptions = append(mountOptions, "ro")
	}

	if err := h.mounter.Mount(devicePath, req.TargetPath, "", mountOptions); err != nil {
		os.Remove(req.TargetPath)
		return fmt.Errorf("failed to bind mount block device: %w", err)
	}

	h.log.V(LogLevelDebug).Info("iSCSI block volume published", "volumeId", req.VolumeID, "devicePath", devicePath, "targetPath", req.TargetPath)
	return nil
}

// Unpublish implements iSCSI volume unpublishing
func (h *ISCSIHandler) Unpublish(ctx context.Context, req *UnpublishRequest) error {
	h.log.V(LogLevelDebug).Info("iSCSI Unpublish", "volumeId", req.VolumeID, "targetPath", req.TargetPath)

	notMounted, err := h.mounter.IsLikelyNotMountPoint(req.TargetPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("failed to check mount point: %w", err)
	}

	if notMounted {
		return nil
	}

	if err := h.mounter.Unmount(req.TargetPath); err != nil {
		return fmt.Errorf("failed to unmount: %w", err)
	}

	os.Remove(req.TargetPath)

	h.log.V(LogLevelDebug).Info("iSCSI volume unpublished", "volumeId", req.VolumeID, "targetPath", req.TargetPath)
	return nil
}

// Expand implements iSCSI volume expansion
func (h *ISCSIHandler) Expand(ctx context.Context, req *ExpandRequest) (*ExpandResult, error) {
	h.log.V(LogLevelDebug).Info("iSCSI Expand", "volumeId", req.VolumeID, "volumePath", req.VolumePath)

	// Load connector to get device info
	cpath := connectorPath(req.VolumeID)
	connector, err := iscsilib.GetConnectorFromFile(cpath)
	if err != nil {
		h.log.Info("Failed to load connector for expand", "error", err)
	}

	if connector != nil {
		// Rescan the devices to pick up new size
		for i := range connector.Devices {
			if err := connector.Devices[i].Rescan(); err != nil {
				h.log.V(LogLevelTrace).Info("Failed to rescan device", "device", connector.Devices[i].Name, "error", err)
			}
		}
		// For multipath, resize the multipath device
		if connector.IsMultipathEnabled() && connector.MountTargetDevice != nil {
			if err := iscsilib.ResizeMultipathDevice(connector.MountTargetDevice); err != nil {
				h.log.V(LogLevelTrace).Info("Failed to resize multipath device", "error", err)
			}
		}
	}

	// Rescan SCSI devices to pick up new size
	h.rescanSCSIHosts()

	// Get device path from connector
	var devicePath string
	if connector != nil && len(connector.Devices) > 0 {
		devicePath = fmt.Sprintf("/dev/%s", connector.Devices[0].Name)
		// Rescan this device specifically
		rescanPath := fmt.Sprintf("/sys/block/%s/device/rescan", connector.Devices[0].Name)
		if err := os.WriteFile(rescanPath, []byte("1\n"), 0200); err != nil {
			h.log.V(LogLevelTrace).Info("Failed to rescan device", "error", err)
		}
	}

	// Resize filesystem
	if devicePath != "" && req.VolumePath != "" {
		h.log.V(LogLevelDebug).Info("Resizing filesystem", "device", devicePath, "volumePath", req.VolumePath)
		resized, err := h.resizer.Resize(devicePath, req.VolumePath)
		if err != nil {
			return nil, fmt.Errorf("failed to resize filesystem: %w", err)
		}
		if resized {
			h.log.V(LogLevelDebug).Info("Filesystem resized successfully")
		}
	}

	return &ExpandResult{CapacityBytes: req.CapacityBytes}, nil
}

// rescanSCSIHosts triggers a rescan of all SCSI hosts
func (h *ISCSIHandler) rescanSCSIHosts() {
	hostDir := "/sys/class/scsi_host"
	entries, err := os.ReadDir(hostDir)
	if err != nil {
		h.log.V(LogLevelTrace).Info("Failed to read SCSI hosts", "error", err)
		return
	}

	for _, entry := range entries {
		scanPath := filepath.Join(hostDir, entry.Name(), "scan")
		if err := os.WriteFile(scanPath, []byte("- - -"), 0200); err != nil {
			h.log.V(LogLevelTrace).Info("Failed to scan SCSI host", "host", entry.Name(), "error", err)
		}
	}
}

// sanitizeISCSIVolumeID creates a safe filename from volume ID
func sanitizeISCSIVolumeID(volumeID string) string {
	result := make([]byte, 0, len(volumeID))
	for i := 0; i < len(volumeID); i++ {
		c := volumeID[i]
		if (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '-' || c == '_' {
			result = append(result, c)
		} else {
			result = append(result, '_')
		}
	}
	return string(result)
}
