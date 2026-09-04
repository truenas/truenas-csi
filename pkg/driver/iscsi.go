package driver

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
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

	// iSCSI connection settings
	iscsiRetryCount    = 10 // number of login attempts
	iscsiCheckInterval = 1  // seconds between retries

	// iscsiadmExitNoObjsFound is iscsiadm's ISCSI_ERR_NO_OBJS_FOUND: the record or
	// session asked about does not exist.
	iscsiadmExitNoObjsFound = 21

	// Filesystem types
	fsTypeXFS = "xfs"

	// Mount options
	mountOptionNouuid = "nouuid"
	mountOptionBind   = "bind"
)

// connectorDir holds the per-volume connector files for iSCSI and NVMe-oF. It must
// be backed by a host path in the node plugin's pod spec: it records which protocol
// and target each staged volume uses, and NodeUnstageVolume gets no publish context
// to fall back on, so losing it on a container restart strands staged volumes (see
// handlerForVolume for the recovery path). Overridable in tests.
var connectorDir = "/var/lib/truenas-csi/connectors"

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
func NewISCSIHandler(mounter *mount.SafeFormatAndMount, log logr.Logger) (*ISCSIHandler, error) {
	// Ensure connector directory exists
	if err := os.MkdirAll(connectorDir, 0o750); err != nil {
		return nil, fmt.Errorf("failed to create connector directory %s: %w", connectorDir, err)
	}

	return &ISCSIHandler{
		mounter: mounter,
		resizer: mount.NewResizeFs(mounter.Exec),
		log:     log,
	}, nil
}

// Protocol returns the protocol name
func (h *ISCSIHandler) Protocol() string {
	return ProtocolISCSI
}

// connectorPath returns the path for storing connector info for a volume
func connectorPath(volumeID string) string {
	return filepath.Join(connectorDir, fmt.Sprintf("%s.connector", sanitizeISCSIVolumeID(volumeID)))
}

// parseISCSIConfig extracts iSCSI configuration from publish and volume contexts
func parseISCSIConfig(publishContext, volumeContext map[string]string) (*ISCSIConfig, error) {
	config := &ISCSIConfig{
		TargetPortal: publishContext[PublishContextTargetPortal],
		TargetIQN:    publishContext[PublishContextTargetIQN],
	}

	// Parse LUN
	if lunStr := publishContext[PublishContextLUN]; lunStr != "" {
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
		connector.AuthType = iscsiAuthTypeCHAP
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

// ensureIPv4Portal rejects IPv6 iSCSI portals with an actionable error. The pinned
// csi-lib-iscsi mis-parses IPv6 portal addresses (it splits the portal on ":"), so
// the device by-path it waits for never matches the real udev symlink and staging
// fails after many retries with an opaque "failed to find device path". Fail fast
// with a clear message instead. IPv6-only clusters should use NFS.
func ensureIPv4Portal(portal string) error {
	host := portal
	if h, _, err := net.SplitHostPort(portal); err == nil {
		host = h
	}
	host = strings.Trim(host, "[]")
	if ip := net.ParseIP(host); ip != nil && ip.To4() == nil {
		return fmt.Errorf("iSCSI portal %q is IPv6, which is not supported (csi-lib-iscsi mis-parses IPv6 portals) — use an IPv4 portal, or NFS on IPv6-only clusters", portal)
	}
	return nil
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

	if err := ensureIPv4Portal(config.TargetPortal); err != nil {
		return nil, err
	}

	// Build connector for csi-lib-iscsi
	connector := h.buildConnector(req.VolumeID, config)

	// Connect to iSCSI target
	h.log.V(LogLevelDebug).Info("Connecting to iSCSI target", "portal", config.TargetPortal, "iqn", config.TargetIQN, "lun", config.LUN)
	devicePath, err := iscsilib.Connect(*connector)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to iSCSI target %s at %s: %w", config.TargetIQN, config.TargetPortal, withCommandOutput(err))
	}

	h.log.V(LogLevelDebug).Info("iSCSI connected", "device", devicePath)

	// Persist connector info for cleanup on unstage.
	// Connect() takes a value copy so device info isn't populated in our
	// original connector. We must set it from the returned devicePath so that
	// block volume publish can find the device later.
	if devicePath != "" {
		deviceName := filepath.Base(devicePath) // e.g. "sda" from "/dev/sda"
		connector.MountTargetDevice = &iscsilib.Device{Name: deviceName}
		connector.Devices = []iscsilib.Device{{Name: deviceName}}
	}
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
		fsType = DefaultFSType
	}

	// Create staging directory
	if err := os.MkdirAll(req.StagingPath, 0o750); err != nil {
		return nil, fmt.Errorf("failed to create staging directory: %w", err)
	}

	// For XFS, add nouuid mount option to allow mounting cloned volumes
	// that share the same UUID as the source volume on the same node.
	mountFlags := req.MountFlags
	if fsType == fsTypeXFS && !slices.Contains(mountFlags, mountOptionNouuid) {
		mountFlags = append(mountFlags, mountOptionNouuid)
	}

	// Format and mount device using SafeFormatAndMount
	h.log.V(LogLevelDebug).Info("FormatAndMount device", "device", devicePath, "stagingPath", req.StagingPath, "fsType", fsType)
	if err := h.mounter.FormatAndMount(devicePath, req.StagingPath, fsType, mountFlags); err != nil {
		return nil, fmt.Errorf("failed to format and mount device: %w", err)
	}

	// Resize filesystem if the block device is larger (e.g., snapshot restored
	// to a larger PVC). FormatAndMount skips formatting when a filesystem
	// already exists, so the filesystem may be smaller than the ZVOL.
	if needsResize, err := h.resizer.NeedResize(devicePath, req.StagingPath); err == nil && needsResize {
		h.log.Info("Filesystem smaller than device, resizing", "volumeId", req.VolumeID, "device", devicePath)
		if _, err := h.resizer.Resize(devicePath, req.StagingPath); err != nil {
			return nil, fmt.Errorf("failed to resize filesystem after mount: %w", err)
		}
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
			return h.cleanupISCSISession(req.VolumeID)
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
	if err := h.cleanupISCSISession(req.VolumeID); err != nil {
		return err
	}

	// Remove staging directory
	os.Remove(req.StagingPath)

	h.log.V(LogLevelDebug).Info("iSCSI volume unstaged", "volumeId", req.VolumeID)
	return nil
}

// cleanupISCSISession logs out of the iSCSI session and removes the connector file.
//
// The connector file is the only record of the volume's protocol and target, so it
// is kept when logout fails: removing it would leave later unstage attempts unable
// to recognize the volume as iSCSI. The failure is returned rather than swallowed so
// the CSI call is retried instead of reporting a teardown that did not happen.
func (h *ISCSIHandler) cleanupISCSISession(volumeID string) error {
	cpath := connectorPath(volumeID)
	if _, err := os.Stat(cpath); err != nil {
		return nil // No connector file, nothing to clean up
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
		if err := logoutISCSITarget(connector.TargetIqn, connector.TargetPortals); err != nil {
			return fmt.Errorf("failed to log out of iSCSI target %s: %w", connector.TargetIqn, err)
		}
		h.log.V(LogLevelDebug).Info("Disconnected from iSCSI target", "targetIqn", connector.TargetIqn)
	}

	// Remove connector file
	if err := os.Remove(cpath); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to remove connector file %s: %w", cpath, err)
	}
	return nil
}

// logoutISCSITarget logs out of every portal of a target and drops its node
// database entry. csi-lib-iscsi's Disconnect helper does the same work but discards
// every error and gives the caller no way to tell a completed logout from a failed
// one, so its finer-grained calls are used directly here. Overridable in tests.
var logoutISCSITarget = func(targetIQN string, portals []string) error {
	for _, portal := range portals {
		// Node records are keyed by portal address; matching without the port
		// covers records written for a non-default port.
		host := portal
		if h, _, err := net.SplitHostPort(portal); err == nil {
			host = h
		}
		if err := iscsilib.Logout(targetIQN, host); err != nil && !isNoISCSIObjectsFound(err) {
			return fmt.Errorf("logout from portal %s failed: %w", portal, withCommandOutput(err))
		}
	}

	if err := iscsilib.DeleteDBEntry(targetIQN); err != nil && !isNoISCSIObjectsFound(err) {
		return fmt.Errorf("removing the node database entry failed: %w", withCommandOutput(err))
	}
	return nil
}

// isNoISCSIObjectsFound reports whether an iscsiadm call failed only because what it
// was asked about is not present, which makes logout idempotent.
func isNoISCSIObjectsFound(err error) bool {
	var exitErr *exec.ExitError
	return errors.As(err, &exitErr) && exitErr.ExitCode() == iscsiadmExitNoObjsFound
}

// withCommandOutput appends a failed command's stderr to its error. csi-lib-iscsi
// surfaces only the exit status, which reduces a plain diagnosis - an iscsiadm the
// host cannot run, say - to an opaque "exit status 127" with nothing to act on.
func withCommandOutput(err error) error {
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) && len(exitErr.Stderr) > 0 {
		return fmt.Errorf("%w: %s", err, strings.TrimSpace(string(exitErr.Stderr)))
	}
	return err
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
	if err := os.MkdirAll(req.TargetPath, 0o750); err != nil {
		return fmt.Errorf("failed to create target directory: %w", err)
	}

	// Bind mount from staging to target
	mountOptions := []string{mountOptionBind}
	if req.ReadOnly {
		mountOptions = append(mountOptions, mountOptionReadOnly)
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
	if err := os.MkdirAll(targetDir, 0o750); err != nil {
		return fmt.Errorf("failed to create target directory: %w", err)
	}

	// Create target file for block device mount
	file, err := os.OpenFile(req.TargetPath, os.O_CREATE|os.O_RDWR, 0o660)
	if err != nil {
		return fmt.Errorf("failed to create target file: %w", err)
	}
	file.Close()

	// Bind mount block device to target file
	mountOptions := []string{mountOptionBind}
	if req.ReadOnly {
		mountOptions = append(mountOptions, mountOptionReadOnly)
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
		// For multipath, resize the multipath device. The nil check comes first:
		// IsMultipathEnabled dereferences MountTargetDevice.
		if connector.MountTargetDevice != nil && connector.IsMultipathEnabled() {
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
		if err := os.WriteFile(rescanPath, []byte("1\n"), 0o200); err != nil {
			h.log.V(LogLevelTrace).Info("Failed to rescan device", "error", err)
		}
	}

	// Raw block volumes hold whatever the workload wrote to them, commonly a
	// partition table. The rescans above are the whole job: probing the device
	// for a filesystem to grow would only find contents the driver must not
	// touch, and the resizer rejects anything it cannot grow.
	if req.IsBlockVolume {
		h.log.V(LogLevelDebug).Info("Raw block volume, skipping filesystem resize", "volumeId", req.VolumeID, "device", devicePath)
		return &ExpandResult{CapacityBytes: req.CapacityBytes}, nil
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
		if err := os.WriteFile(scanPath, []byte("- - -"), 0o200); err != nil {
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
