package driver

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/go-logr/logr"
	"k8s.io/mount-utils"
)

// nvmeExecCommand runs nvme-cli / modprobe; overridable in tests.
var nvmeExecCommand = exec.CommandContext

const (
	// nvmeConnectorExt is the connector-file extension for NVMe-oF volumes,
	// distinct from iSCSI's ".connector".
	nvmeConnectorExt = ".nvme"

	// nvmeCtrlLossTmo keeps the connection retrying through transient target
	// outages instead of failing the mount on a brief blip (pattern from ceph-csi).
	nvmeCtrlLossTmo = "1800"

	// nvmeByIDPrefix is the udev by-id symlink prefix for a namespace UUID.
	nvmeByIDPrefix = "/dev/disk/by-id/nvme-uuid."

	nvmeSysClassDir = "/sys/class/nvme"

	nvmeDeviceWaitAttempts = 30
	nvmeDeviceWaitInterval = 200 * time.Millisecond
)

// NVMeOFHandler implements the ProtocolHandler interface for NVMe-oF/TCP volumes.
type NVMeOFHandler struct {
	mounter *mount.SafeFormatAndMount
	resizer *mount.ResizeFs
	log     logr.Logger
}

// NVMeOFConfig holds NVMe-oF configuration parsed from publish/volume contexts.
type NVMeOFConfig struct {
	SubNQN        string
	PortAddr      string
	PortSvcID     string
	Transport     string
	NamespaceUUID string
	HostNQN       string
	DHCHAPKey     string
	DHCHAPCtrlKey string
}

// nvmeConnectorInfo is persisted per-volume so unstage/expand can find the device
// and subsystem without the publish/volume contexts (NodeUnstage gets neither).
type nvmeConnectorInfo struct {
	VolumeID      string `json:"volumeID"`
	SubNQN        string `json:"subnqn"`
	NamespaceUUID string `json:"namespaceUUID"`
	DevicePath    string `json:"devicePath"`
}

// NewNVMeOFHandler creates a new NVMe-oF protocol handler.
func NewNVMeOFHandler(mounter *mount.SafeFormatAndMount, log logr.Logger) (*NVMeOFHandler, error) {
	if err := os.MkdirAll(connectorDir, 0o750); err != nil {
		return nil, fmt.Errorf("failed to create connector directory %s: %w", connectorDir, err)
	}
	return &NVMeOFHandler{
		mounter: mounter,
		resizer: mount.NewResizeFs(mounter.Exec),
		log:     log,
	}, nil
}

// Protocol returns the protocol name.
func (h *NVMeOFHandler) Protocol() string {
	return ProtocolNVMeOF
}

// nvmeConnectorPath returns the connector file path for a volume.
func nvmeConnectorPath(volumeID string) string {
	return filepath.Join(connectorDir, sanitizeISCSIVolumeID(volumeID)+nvmeConnectorExt)
}

// parseNVMeOFConfig extracts NVMe-oF configuration. Connection parameters come from
// the publish context; DH-CHAP credentials (plaintext StorageClass params) come from
// the volume context, mirroring how iSCSI CHAP is delivered.
func parseNVMeOFConfig(publishContext, volumeContext map[string]string) *NVMeOFConfig {
	transport := publishContext[PublishContextNVMeTransport]
	if transport == "" {
		transport = defaultNVMeOFTransport
	}
	return &NVMeOFConfig{
		SubNQN:        publishContext[PublishContextNVMeSubNQN],
		PortAddr:      publishContext[PublishContextNVMePortAddr],
		PortSvcID:     publishContext[PublishContextNVMePortSvcID],
		Transport:     transport,
		NamespaceUUID: publishContext[PublishContextNVMeNSUUID],
		HostNQN:       volumeContext[paramNVMeOFHostNQN],
		DHCHAPKey:     volumeContext[paramNVMeOFDHCHAPKey],
		DHCHAPCtrlKey: volumeContext[paramNVMeOFDHCHAPCtrlKey],
	}
}

// Stage connects the NVMe-oF subsystem and prepares the device (format+mount for
// filesystem volumes, or returns the device path for block volumes).
func (h *NVMeOFHandler) Stage(ctx context.Context, req *StageRequest) (*StageResult, error) {
	h.log.V(LogLevelDebug).Info("NVMe-oF Stage", "volumeId", req.VolumeID, "stagingPath", req.StagingPath, "isBlock", req.IsBlockVolume)

	config := parseNVMeOFConfig(req.PublishContext, req.VolumeContext)
	if config.SubNQN == "" || config.PortAddr == "" || config.PortSvcID == "" {
		return nil, fmt.Errorf("NVMe-oF subsystem NQN and portal are required (check controller publish context)")
	}

	if err := h.loadKernelModules(ctx); err != nil {
		h.log.V(LogLevelTrace).Info("Failed to load NVMe kernel modules (may already be loaded)", "error", err)
	}

	devicePath, err := h.connectAndDiscover(ctx, config)
	if err != nil {
		return nil, err
	}
	h.log.V(LogLevelDebug).Info("NVMe-oF connected", "device", devicePath, "subnqn", config.SubNQN)

	h.persistConnector(req.VolumeID, config, devicePath)

	// Block volumes: return the device path, no filesystem.
	if req.IsBlockVolume {
		return &StageResult{DevicePath: devicePath}, nil
	}

	fsType := req.FSType
	if fsType == "" {
		fsType = DefaultFSType
	}

	if err := os.MkdirAll(req.StagingPath, 0o750); err != nil {
		return nil, fmt.Errorf("failed to create staging directory: %w", err)
	}

	// For XFS, add nouuid so a cloned volume sharing the source UUID can mount.
	mountFlags := req.MountFlags
	if fsType == fsTypeXFS && !slices.Contains(mountFlags, mountOptionNouuid) {
		mountFlags = append(mountFlags, mountOptionNouuid)
	}

	if err := h.mounter.FormatAndMount(devicePath, req.StagingPath, fsType, mountFlags); err != nil {
		return nil, fmt.Errorf("failed to format and mount device: %w", err)
	}

	// Resize the filesystem if the device is larger (e.g. snapshot restored to a
	// larger PVC); FormatAndMount skips formatting when a filesystem already exists.
	if needsResize, err := h.resizer.NeedResize(devicePath, req.StagingPath); err == nil && needsResize {
		if _, err := h.resizer.Resize(devicePath, req.StagingPath); err != nil {
			return nil, fmt.Errorf("failed to resize filesystem after mount: %w", err)
		}
	}

	h.log.V(LogLevelDebug).Info("NVMe-oF volume staged", "volumeId", req.VolumeID, "stagingPath", req.StagingPath)
	return &StageResult{DevicePath: devicePath}, nil
}

// connectAndDiscover connects (idempotently) and resolves the namespace block device.
func (h *NVMeOFHandler) connectAndDiscover(ctx context.Context, config *NVMeOFConfig) (string, error) {
	// Already connected? Reuse the device (idempotency).
	if dev := h.findDevice(config); dev != "" {
		return dev, nil
	}

	if err := h.nvmeConnect(ctx, config); err != nil {
		// Tolerate "already connected" races: re-check for the device.
		if dev := h.waitForDevice(ctx, config); dev != "" {
			return dev, nil
		}
		return "", fmt.Errorf("failed to connect to NVMe-oF subsystem %s at %s:%s: %w", config.SubNQN, config.PortAddr, config.PortSvcID, err)
	}

	dev := h.waitForDevice(ctx, config)
	if dev == "" {
		// Roll back the half-open connection so we don't leak controllers.
		_ = h.nvmeDisconnect(ctx, config.SubNQN)
		return "", fmt.Errorf("NVMe-oF device for %s did not appear after connect", config.SubNQN)
	}
	return dev, nil
}

// nvmeConnect runs `nvme connect` with optional hostNQN and DH-CHAP secrets.
func (h *NVMeOFHandler) nvmeConnect(ctx context.Context, config *NVMeOFConfig) error {
	args := []string{
		"connect",
		"-t", config.Transport,
		"-n", config.SubNQN,
		"-a", config.PortAddr,
		"-s", config.PortSvcID,
		"-l", nvmeCtrlLossTmo,
	}
	if config.HostNQN != "" {
		args = append(args, "--hostnqn", config.HostNQN)
	}
	if config.DHCHAPKey != "" {
		args = append(args, "--dhchap-secret", config.DHCHAPKey)
	}
	if config.DHCHAPCtrlKey != "" {
		args = append(args, "--dhchap-ctrl-secret", config.DHCHAPCtrlKey)
	}

	out, err := nvmeExecCommand(ctx, "nvme", args...).CombinedOutput()
	if err != nil {
		return fmt.Errorf("nvme connect failed: %w (output: %s)", err, strings.TrimSpace(string(out)))
	}
	return nil
}

// nvmeDisconnect disconnects all controllers for a subsystem NQN. Safe because the
// driver uses one subsystem per volume (one namespace per subsystem).
func (h *NVMeOFHandler) nvmeDisconnect(ctx context.Context, subnqn string) error {
	out, err := nvmeExecCommand(ctx, "nvme", "disconnect", "-n", subnqn).CombinedOutput()
	if err != nil {
		return fmt.Errorf("nvme disconnect failed: %w (output: %s)", err, strings.TrimSpace(string(out)))
	}
	return nil
}

// nvmeRescanNamespace triggers a controller-side namespace rescan to pick up a new
// size after online expansion. ns-rescan targets the controller char device.
func (h *NVMeOFHandler) nvmeRescanNamespace(ctx context.Context, devicePath string) error {
	ctrl := nvmeControllerForDevice(devicePath)
	if ctrl == "" {
		return fmt.Errorf("could not derive controller device from %s", devicePath)
	}
	out, err := nvmeExecCommand(ctx, "nvme", "ns-rescan", ctrl).CombinedOutput()
	if err != nil {
		return fmt.Errorf("nvme ns-rescan failed: %w (output: %s)", err, strings.TrimSpace(string(out)))
	}
	return nil
}

// loadKernelModules ensures the NVMe/TCP fabrics modules are loaded (best-effort).
func (h *NVMeOFHandler) loadKernelModules(ctx context.Context) error {
	var firstErr error
	for _, mod := range []string{"nvme_tcp", "nvme_fabrics"} {
		if out, err := nvmeExecCommand(ctx, "modprobe", mod).CombinedOutput(); err != nil {
			h.log.V(LogLevelTrace).Info("modprobe failed", "module", mod, "output", strings.TrimSpace(string(out)))
			if firstErr == nil {
				firstErr = err
			}
		}
	}
	return firstErr
}

// findDevice resolves the namespace block device without waiting: first via the
// by-id UUID symlink, then via a sysfs subsysnqn match.
func (h *NVMeOFHandler) findDevice(config *NVMeOFConfig) string {
	if config.NamespaceUUID != "" {
		for _, cand := range nvmeByIDCandidates(config.NamespaceUUID) {
			if resolved, err := filepath.EvalSymlinks(cand); err == nil {
				return resolved
			}
		}
	}
	return nvmeDeviceBySubsysNQN(config.SubNQN)
}

// waitForDevice polls findDevice with backoff until the device appears or timeout.
func (h *NVMeOFHandler) waitForDevice(ctx context.Context, config *NVMeOFConfig) string {
	for i := 0; i < nvmeDeviceWaitAttempts; i++ {
		if dev := h.findDevice(config); dev != "" {
			return dev
		}
		select {
		case <-ctx.Done():
			return ""
		case <-time.After(nvmeDeviceWaitInterval):
		}
	}
	return ""
}

// nvmeByIDCandidates returns candidate by-id symlink paths for a namespace UUID,
// trying the value as-is and lowercased.
func nvmeByIDCandidates(uuid string) []string {
	candidates := []string{nvmeByIDPrefix + uuid}
	if lower := strings.ToLower(uuid); lower != uuid {
		candidates = append(candidates, nvmeByIDPrefix+lower)
	}
	return candidates
}

// nvmeDeviceBySubsysNQN scans sysfs for a controller whose subsysnqn matches and
// returns its namespace block device (e.g. /dev/nvme0n1).
func nvmeDeviceBySubsysNQN(subnqn string) string {
	if subnqn == "" {
		return ""
	}
	entries, err := os.ReadDir(nvmeSysClassDir)
	if err != nil {
		return ""
	}
	for _, e := range entries {
		ctrl := e.Name() // e.g. nvme0
		data, err := os.ReadFile(filepath.Join(nvmeSysClassDir, ctrl, "subsysnqn"))
		if err != nil || strings.TrimSpace(string(data)) != subnqn {
			continue
		}
		nsEntries, err := os.ReadDir(filepath.Join(nvmeSysClassDir, ctrl))
		if err != nil {
			continue
		}
		for _, ns := range nsEntries {
			name := ns.Name()
			if strings.HasPrefix(name, ctrl+"n") {
				devPath := "/dev/" + name
				if _, err := os.Stat(devPath); err == nil {
					return devPath
				}
			}
		}
	}
	return ""
}

// nvmeControllerForDevice maps a namespace block device to its controller char
// device, e.g. "/dev/nvme0n1" -> "/dev/nvme0". Returns "" if the path is not of
// the expected "/dev/nvme<ctrl>n<nsid>" form.
func nvmeControllerForDevice(devicePath string) string {
	const prefix = "/dev/nvme"
	if !strings.HasPrefix(devicePath, prefix) {
		return ""
	}
	rest := devicePath[len(prefix):] // e.g. "0n1"
	sep := strings.IndexByte(rest, 'n')
	if sep <= 0 {
		return ""
	}
	ctrlNum := rest[:sep] // "0"
	nsNum := rest[sep+1:] // "1"
	if !isAllDigits(ctrlNum) || !isAllDigits(nsNum) {
		return ""
	}
	return prefix + ctrlNum
}

// isAllDigits reports whether s is non-empty and consists only of ASCII digits.
func isAllDigits(s string) bool {
	if s == "" {
		return false
	}
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	return true
}

// persistConnector writes the connector file for unstage/expand.
func (h *NVMeOFHandler) persistConnector(volumeID string, config *NVMeOFConfig, devicePath string) {
	info := nvmeConnectorInfo{
		VolumeID:      volumeID,
		SubNQN:        config.SubNQN,
		NamespaceUUID: config.NamespaceUUID,
		DevicePath:    devicePath,
	}
	data, err := json.Marshal(info)
	if err != nil {
		h.log.Info("Failed to marshal NVMe-oF connector", "error", err)
		return
	}
	if err := os.WriteFile(nvmeConnectorPath(volumeID), data, 0o600); err != nil {
		h.log.Info("Failed to persist NVMe-oF connector", "error", err)
	}
}

// loadConnector reads the connector file, or returns nil if absent/invalid.
func (h *NVMeOFHandler) loadConnector(volumeID string) *nvmeConnectorInfo {
	data, err := os.ReadFile(nvmeConnectorPath(volumeID))
	if err != nil {
		return nil
	}
	var info nvmeConnectorInfo
	if err := json.Unmarshal(data, &info); err != nil {
		h.log.V(LogLevelDebug).Info("Failed to unmarshal NVMe-oF connector", "error", err)
		return nil
	}
	return &info
}

// Unstage unmounts the staging path, disconnects the subsystem, and removes the connector.
func (h *NVMeOFHandler) Unstage(ctx context.Context, req *UnstageRequest) error {
	h.log.V(LogLevelDebug).Info("NVMe-oF Unstage", "volumeId", req.VolumeID, "stagingPath", req.StagingPath)

	notMounted, err := h.mounter.IsLikelyNotMountPoint(req.StagingPath)
	if err != nil {
		if os.IsNotExist(err) {
			h.cleanupNVMeSession(ctx, req.VolumeID)
			return nil
		}
		return fmt.Errorf("failed to check mount point: %w", err)
	}

	if !notMounted {
		if err := h.mounter.Unmount(req.StagingPath); err != nil {
			return fmt.Errorf("failed to unmount staging path: %w", err)
		}
	}

	h.cleanupNVMeSession(ctx, req.VolumeID)
	os.Remove(req.StagingPath)

	h.log.V(LogLevelDebug).Info("NVMe-oF volume unstaged", "volumeId", req.VolumeID)
	return nil
}

// cleanupNVMeSession disconnects the subsystem and removes the connector file.
func (h *NVMeOFHandler) cleanupNVMeSession(ctx context.Context, volumeID string) {
	info := h.loadConnector(volumeID)
	if info != nil && info.SubNQN != "" {
		if err := h.nvmeDisconnect(ctx, info.SubNQN); err != nil {
			h.log.V(LogLevelDebug).Info("Failed to disconnect NVMe-oF subsystem", "subnqn", info.SubNQN, "error", err)
		}
	}
	os.Remove(nvmeConnectorPath(volumeID))
}

// Publish bind-mounts the staged volume (or block device) to the target path.
func (h *NVMeOFHandler) Publish(ctx context.Context, req *PublishRequest) error {
	h.log.V(LogLevelDebug).Info("NVMe-oF Publish", "volumeId", req.VolumeID, "targetPath", req.TargetPath, "isBlock", req.IsBlockVolume)

	if req.IsBlockVolume {
		return h.publishBlockVolume(req)
	}

	if req.StagingPath == "" {
		return fmt.Errorf("staging path is required for NVMe-oF mount volumes")
	}

	notMounted, err := h.mounter.IsLikelyNotMountPoint(req.StagingPath)
	if err != nil || notMounted {
		return fmt.Errorf("volume not staged at %s", req.StagingPath)
	}

	if err := os.MkdirAll(req.TargetPath, 0o750); err != nil {
		return fmt.Errorf("failed to create target directory: %w", err)
	}

	mountOptions := []string{mountOptionBind}
	if req.ReadOnly {
		mountOptions = append(mountOptions, mountOptionReadOnly)
	}
	if err := h.mounter.Mount(req.StagingPath, req.TargetPath, "", mountOptions); err != nil {
		return fmt.Errorf("failed to bind mount: %w", err)
	}
	return nil
}

// publishBlockVolume bind-mounts the raw block device to the target path.
func (h *NVMeOFHandler) publishBlockVolume(req *PublishRequest) error {
	info := h.loadConnector(req.VolumeID)
	if info == nil || info.DevicePath == "" {
		return fmt.Errorf("no NVMe-oF connector/device found for volume %s", req.VolumeID)
	}

	if _, err := os.Stat(info.DevicePath); err != nil {
		return fmt.Errorf("block device %s not found: %w", info.DevicePath, err)
	}

	if err := os.MkdirAll(filepath.Dir(req.TargetPath), 0o750); err != nil {
		return fmt.Errorf("failed to create target directory: %w", err)
	}
	file, err := os.OpenFile(req.TargetPath, os.O_CREATE|os.O_RDWR, 0o660)
	if err != nil {
		return fmt.Errorf("failed to create target file: %w", err)
	}
	file.Close()

	mountOptions := []string{mountOptionBind}
	if req.ReadOnly {
		mountOptions = append(mountOptions, mountOptionReadOnly)
	}
	if err := h.mounter.Mount(info.DevicePath, req.TargetPath, "", mountOptions); err != nil {
		os.Remove(req.TargetPath)
		return fmt.Errorf("failed to bind mount block device: %w", err)
	}
	return nil
}

// Unpublish unmounts the target path.
func (h *NVMeOFHandler) Unpublish(ctx context.Context, req *UnpublishRequest) error {
	h.log.V(LogLevelDebug).Info("NVMe-oF Unpublish", "volumeId", req.VolumeID, "targetPath", req.TargetPath)

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
	return nil
}

// Expand rescans the namespace for the new size and grows the filesystem.
func (h *NVMeOFHandler) Expand(ctx context.Context, req *ExpandRequest) (*ExpandResult, error) {
	h.log.V(LogLevelDebug).Info("NVMe-oF Expand", "volumeId", req.VolumeID, "volumePath", req.VolumePath)

	info := h.loadConnector(req.VolumeID)
	if info == nil || info.DevicePath == "" {
		return nil, fmt.Errorf("no NVMe-oF device found for volume %s", req.VolumeID)
	}

	if err := h.nvmeRescanNamespace(ctx, info.DevicePath); err != nil {
		h.log.V(LogLevelDebug).Info("Failed to rescan NVMe-oF namespace", "device", info.DevicePath, "error", err)
	}

	if req.VolumePath != "" {
		if _, err := h.resizer.Resize(info.DevicePath, req.VolumePath); err != nil {
			return nil, fmt.Errorf("failed to resize filesystem: %w", err)
		}
	}

	return &ExpandResult{CapacityBytes: req.CapacityBytes}, nil
}
