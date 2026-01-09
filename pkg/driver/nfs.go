package driver

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"slices"
	"strings"
	"time"

	"github.com/go-logr/logr"
	"k8s.io/mount-utils"
)

// Mount timeout constant (aligned with official csi-driver-nfs)
const (
	// defaultMountTimeout is the default timeout for mount operations (matches csi-driver-nfs)
	defaultMountTimeout = 110 * time.Second
)

// StorageClass parameter keys for NFS configuration
const (
	paramNFSMountOptions = "nfs.mountOptions"
)

// NFSHandler implements the ProtocolHandler interface for NFS volumes
type NFSHandler struct {
	mounter mount.Interface
	log     logr.Logger
}

// NFSConfig holds NFS-specific configuration parsed from volume/publish contexts
type NFSConfig struct {
	Server       string
	Path         string
	MountOptions []string
}

// NewNFSHandler creates a new NFS protocol handler
func NewNFSHandler(mounter mount.Interface, log logr.Logger) *NFSHandler {
	return &NFSHandler{
		mounter: mounter,
		log:     log,
	}
}

// Protocol returns the protocol name
func (h *NFSHandler) Protocol() string {
	return "nfs"
}

// parseNFSConfig extracts NFS configuration from publish and volume contexts
func parseNFSConfig(publishContext, volumeContext map[string]string) *NFSConfig {
	config := &NFSConfig{}

	// Server
	config.Server = publishContext["nfsServer"]
	if config.Server == "" {
		config.Server = volumeContext["nfsServer"]
	}

	// Path
	config.Path = publishContext["nfsPath"]
	if config.Path == "" {
		config.Path = volumeContext["nfsPath"]
	}

	// Custom mount options (includes nfsvers if user specifies it)
	if val, ok := volumeContext[paramNFSMountOptions]; ok && val != "" {
		options := strings.Split(val, ",")
		for _, opt := range options {
			opt = strings.TrimSpace(opt)
			if opt != "" {
				config.MountOptions = append(config.MountOptions, opt)
			}
		}
	}

	return config
}

// getMountOptions returns mount options based on configuration
func getMountOptions(config *NFSConfig, readonly bool) []string {
	var options []string

	// Use custom mount options if provided, otherwise use minimal defaults
	if len(config.MountOptions) > 0 {
		options = append(options, config.MountOptions...)
	} else {
		// Minimal defaults - let kernel handle rsize/wsize/timeo
		options = []string{"hard"}
	}

	if readonly {
		options = append(options, "ro")
	}

	return options
}

// mountWithTimeout executes mount with a timeout. Uses exec.CommandContext to ensure
// the mount process is killed if the timeout is reached (no goroutine leak).
func (h *NFSHandler) mountWithTimeout(ctx context.Context, source, target, fsType string, options []string) error {
	// Create a context with timeout
	mountCtx, cancel := context.WithTimeout(ctx, defaultMountTimeout)
	defer cancel()

	// Build mount command arguments
	args := []string{"-t", fsType}
	if len(options) > 0 {
		args = append(args, "-o", strings.Join(options, ","))
	}
	args = append(args, source, target)

	h.log.V(LogLevelDebug).Info("Executing mount command", "args", args, "timeout", defaultMountTimeout)

	// exec.CommandContext kills the process when context is cancelled/times out
	cmd := exec.CommandContext(mountCtx, "mount", args...)
	output, err := cmd.CombinedOutput()

	if err != nil {
		if mountCtx.Err() == context.DeadlineExceeded {
			return fmt.Errorf("mount timed out after %v", defaultMountTimeout)
		}
		return fmt.Errorf("mount failed: %v, output: %s", err, strings.TrimSpace(string(output)))
	}

	return nil
}

// Stage is a no-op for NFS (NFS doesn't need staging)
func (h *NFSHandler) Stage(ctx context.Context, req *StageRequest) (*StageResult, error) {
	h.log.V(LogLevelDebug).Info("NFS Stage (no-op)", "volumeId", req.VolumeID)

	// NFS does not support raw block volumes
	if req.IsBlockVolume {
		return nil, fmt.Errorf("NFS does not support raw block volumes")
	}

	// NFS doesn't require staging - mount happens directly in Publish
	return &StageResult{}, nil
}

// Unstage is a no-op for NFS
func (h *NFSHandler) Unstage(ctx context.Context, req *UnstageRequest) error {
	h.log.V(LogLevelDebug).Info("NFS Unstage (no-op)", "volumeId", req.VolumeID)
	return nil
}

// Publish implements NFS volume publishing (direct mount)
func (h *NFSHandler) Publish(ctx context.Context, req *PublishRequest) error {
	h.log.V(LogLevelDebug).Info("NFS Publish", "volumeId", req.VolumeID, "targetPath", req.TargetPath)

	config := parseNFSConfig(req.PublishContext, req.VolumeContext)

	if config.Server == "" || config.Path == "" {
		return fmt.Errorf("NFS server and path are required")
	}

	source := formatNFSSource(config.Server, config.Path)

	// Check if already mounted
	notMounted, err := h.mounter.IsLikelyNotMountPoint(req.TargetPath)
	if err != nil {
		if os.IsNotExist(err) {
			if err := os.MkdirAll(req.TargetPath, 0o750); err != nil {
				return fmt.Errorf("failed to create target directory: %w", err)
			}
			notMounted = true
		} else {
			return fmt.Errorf("failed to check mount point: %w", err)
		}
	}

	if !notMounted {
		h.log.V(LogLevelDebug).Info("Volume already mounted", "targetPath", req.TargetPath)
		return nil
	}

	// Determine mount options
	var mountOptions []string
	if len(req.MountFlags) > 0 {
		mountOptions = req.MountFlags
		if req.ReadOnly && !slices.Contains(mountOptions, "ro") {
			mountOptions = append(mountOptions, "ro")
		}
	} else {
		mountOptions = getMountOptions(config, req.ReadOnly)
	}

	h.log.V(LogLevelDebug).Info("Mounting NFS volume", "source", source, "target", req.TargetPath, "options", mountOptions)

	if err := h.mountWithTimeout(ctx, source, req.TargetPath, "nfs", mountOptions); err != nil {
		return fmt.Errorf("failed to mount NFS volume %s: %w", source, err)
	}

	h.log.V(LogLevelDebug).Info("NFS volume published", "volumeId", req.VolumeID, "targetPath", req.TargetPath)
	return nil
}

// Unpublish implements NFS volume unpublishing
func (h *NFSHandler) Unpublish(ctx context.Context, req *UnpublishRequest) error {
	h.log.V(LogLevelDebug).Info("NFS Unpublish", "volumeId", req.VolumeID, "targetPath", req.TargetPath)

	if err := mount.CleanupMountPoint(req.TargetPath, h.mounter, true); err != nil {
		return fmt.Errorf("failed to unmount: %w", err)
	}

	h.log.V(LogLevelDebug).Info("NFS volume unpublished", "volumeId", req.VolumeID, "targetPath", req.TargetPath)
	return nil
}

// Expand is a no-op for NFS (NFS expansion happens on the server)
func (h *NFSHandler) Expand(ctx context.Context, req *ExpandRequest) (*ExpandResult, error) {
	h.log.V(LogLevelDebug).Info("NFS Expand (no-op, expansion happens on server)", "volumeId", req.VolumeID)
	return &ExpandResult{CapacityBytes: req.CapacityBytes}, nil
}

// formatNFSSource formats an NFS source string with proper IPv6 handling.
// IPv6 addresses must be enclosed in brackets for NFS mounts (e.g., [2001:db8::1]:/path).
func formatNFSSource(server, path string) string {
	// If server already has brackets (user-provided IPv6 format), use as-is
	if strings.HasPrefix(server, "[") {
		return fmt.Sprintf("%s:%s", server, path)
	}

	// Check if server is an IPv6 address (contains colons but no brackets)
	if isIPv6Address(server) {
		return fmt.Sprintf("[%s]:%s", server, path)
	}

	// IPv4 or hostname - use standard format
	return fmt.Sprintf("%s:%s", server, path)
}

// isIPv6Address checks if a string is an IPv6 address.
// IPv6 addresses contain colons and may contain dots (for IPv4-mapped addresses).
func isIPv6Address(addr string) bool {
	// Must contain at least one colon for IPv6
	if !strings.Contains(addr, ":") {
		return false
	}

	// Parse as IP to validate
	ip := net.ParseIP(addr)
	if ip == nil {
		return false
	}

	// Check if it's IPv6 (To4() returns nil for IPv6)
	return ip.To4() == nil
}
