package driver

import (
	"context"
	"fmt"
	"net"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/go-logr/logr"
	"github.com/iXsystems/truenas_k8_driver/pkg/client"
	"google.golang.org/grpc"
	"k8s.io/mount-utils"
)

const (
	DRIVER_NAME    = "csi.truenas.io"
	DRIVER_VERSION = "1.0.0"

	DEFAULT_IQN_BASE = "iqn.2000-01.io.truenas"

	// Protocol identifiers
	ProtocolISCSI = "iscsi"
	ProtocolNFS   = "nfs"

	// Compression defaults
	CompressionLZ4 = "LZ4"

	// Mount defaults
	DefaultMountpoint = "/mnt"

	// File permissions
	DefaultDirPermission = 0o755

	// Timeouts
	GracefulShutdownTimeout = 30 * time.Second

	// Logging verbosity levels (for klog.V())
	// V(0) - Always logged (critical errors, startup/shutdown)
	// V(2) - General operational info (volume created, deleted)
	// V(4) - Detailed diagnostics (API calls, parameters)
	// V(5) - Trace level (full request/response payloads)
	LogLevelInfo  = 2
	LogLevelDebug = 4
	LogLevelTrace = 5

	// Volume defaults
	DefaultFSType         = "ext4"
	DefaultVolBlockSize   = "16K"
	DefaultISCSIBlockSize = 512
	DefaultLUN            = 0

	// IQN validation
	iqnMinParts       = 3       // iqn.YYYY-MM.domain
	iqnDateFieldLen   = 7       // YYYY-MM
	iqnDateSeparator  = 4       // position of '-' in YYYY-MM
	volumeIDSeparator = "/"     // separator in volume IDs (pool/name)

	// Publish context keys (used in ControllerPublishVolume -> NodeStageVolume)
	PublishContextProtocol     = "protocol"
	PublishContextTargetIQN    = "targetIQN"
	PublishContextTargetPortal = "targetPortal"
	PublishContextLUN          = "lun"
	PublishContextNFSServer    = "nfsServer"
	PublishContextNFSPath      = "nfsPath"
	PublishContextCHAPUser     = "chapUser"
	PublishContextCHAPSecret   = "chapSecret"
)

// VolumeInfo holds metadata about a provisioned volume
type VolumeInfo struct {
	ID                 string
	Name               string
	CapacityBytes      int64
	VolumeContext      map[string]string
	ContentSource      *csi.VolumeContentSource
	AccessibleTopology []*csi.Topology

	DatasetPath string
	PoolName    string
	Protocol    string // "nfs" or "iscsi"

	NFSPath    string
	NFSShareID int

	TargetIQN        string
	TargetPortal     string
	LUN              int
	ISCSITargetID    int
	ISCSIExtentID    int
	ISCSIAuthID      int // CHAP auth credential ID
	ISCSIInitiatorID int // Initiator group ID
}

// ISCSIDeleteOptions holds parsed delete options from StorageClass parameters.
type ISCSIDeleteOptions struct {
	ForceDelete             bool
	DeleteExtentsWithTarget bool
}

// StageRequest contains all information needed to stage a volume
type StageRequest struct {
	VolumeID         string
	StagingPath      string
	FSType           string
	MountFlags       []string
	VolumeCapability *csi.VolumeCapability
	PublishContext   map[string]string
	VolumeContext    map[string]string
	IsBlockVolume    bool // true for raw block volumes (no filesystem)
}

// UnstageRequest contains all information needed to unstage a volume
type UnstageRequest struct {
	VolumeID    string
	StagingPath string
}

// PublishRequest contains all information needed to publish a volume
type PublishRequest struct {
	VolumeID         string
	StagingPath      string
	TargetPath       string
	FSType           string
	MountFlags       []string
	ReadOnly         bool
	VolumeCapability *csi.VolumeCapability
	PublishContext   map[string]string
	VolumeContext    map[string]string
	IsBlockVolume    bool // true for raw block volumes
}

// UnpublishRequest contains all information needed to unpublish a volume
type UnpublishRequest struct {
	VolumeID   string
	TargetPath string
}

// ExpandRequest contains all information needed to expand a volume
type ExpandRequest struct {
	VolumeID      string
	VolumePath    string
	CapacityBytes int64
}

// StageResult contains the result of staging a volume
type StageResult struct {
	DevicePath string
}

// ExpandResult contains the result of expanding a volume
type ExpandResult struct {
	CapacityBytes int64
}

// ProtocolHandler defines the interface for protocol-specific volume operations
type ProtocolHandler interface {
	// Protocol returns the protocol name (e.g., "iscsi", "nfs")
	Protocol() string

	// Stage prepares a volume for use on the node (e.g., iSCSI login, device formatting)
	Stage(ctx context.Context, req *StageRequest) (*StageResult, error)

	// Unstage cleans up a staged volume (e.g., iSCSI logout)
	Unstage(ctx context.Context, req *UnstageRequest) error

	// Publish mounts a volume to the target path
	Publish(ctx context.Context, req *PublishRequest) error

	// Unpublish unmounts a volume from the target path
	Unpublish(ctx context.Context, req *UnpublishRequest) error

	// Expand grows the volume's filesystem to use newly allocated space
	Expand(ctx context.Context, req *ExpandRequest) (*ExpandResult, error)
}

type Driver struct {
	name     string
	version  string
	nodeID   string
	endpoint string

	log    logr.Logger
	client *client.Client

	defaultPool  string
	nfsServer    string
	iscsiPortal  string
	iscsiIQNBase string

	identityServer   csi.IdentityServer
	controllerServer csi.ControllerServer
	nodeServer       csi.NodeServer

	server *grpc.Server

	controllerCaps []*csi.ControllerServiceCapability
	nodeCaps       []*csi.NodeServiceCapability
	pluginCaps     []*csi.PluginCapability
	volumeCaps     []*csi.VolumeCapability_AccessMode
}

type DriverConfig struct {
	DriverName    string
	DriverVersion string
	NodeID        string
	Endpoint      string

	TrueNASURL      string
	TrueNASAPIKey   string
	TrueNASInsecure bool

	DefaultPool  string
	NFSServer    string
	ISCSIPortal  string
	ISCSIIQNBase string

	// Logger is the structured logger for the driver and client.
	// If not set, logging for the client will be disabled.
	Logger logr.Logger
}

// NewDriver creates a new TrueNAS CSI driver with the given configuration.
// It validates the configuration, establishes a connection to TrueNAS,
// and initializes the controller and node services.
func NewDriver(config *DriverConfig) (*Driver, error) {
	config.DriverName = DRIVER_NAME

	config.DriverVersion = DRIVER_VERSION

	// Initialize logger (use discard if not provided)
	log := config.Logger
	if log.GetSink() == nil {
		log = logr.Discard()
	}

	if config.NodeID == "" {
		return nil, fmt.Errorf("node ID is required")
	}
	if config.Endpoint == "" {
		return nil, fmt.Errorf("endpoint is required")
	}
	if config.TrueNASURL == "" {
		return nil, fmt.Errorf("TrueNAS URL is required")
	}
	if config.TrueNASAPIKey == "" {
		return nil, fmt.Errorf("TrueNAS API key is required")
	}
	if config.DefaultPool == "" {
		return nil, fmt.Errorf("default pool is required")
	}

	if config.ISCSIIQNBase == "" {
		config.ISCSIIQNBase = DEFAULT_IQN_BASE
	}

	if err := validateIQNFormat(config.ISCSIIQNBase); err != nil {
		return nil, fmt.Errorf("invalid iSCSI IQN base format: %w", err)
	}

	// Derive NFS server from TrueNAS URL if not explicitly set
	if config.NFSServer == "" {
		if parsedURL, err := url.Parse(config.TrueNASURL); err == nil {
			host := parsedURL.Hostname()
			if host != "" {
				config.NFSServer = host
				log.V(LogLevelInfo).Info("Derived NFS server from TrueNAS URL", "nfsServer", host)
			}
		}
	}

	// Derive iSCSI portal from TrueNAS URL if not explicitly set (default port 3260)
	if config.ISCSIPortal == "" {
		if parsedURL, err := url.Parse(config.TrueNASURL); err == nil {
			host := parsedURL.Hostname()
			if host != "" {
				config.ISCSIPortal = host + ":3260"
				log.V(LogLevelInfo).Info("Derived iSCSI portal from TrueNAS URL", "iscsiPortal", config.ISCSIPortal)
			}
		}
	}

	ctx := context.Background()

	cfg := client.Config{
		URL:                config.TrueNASURL,
		APIKey:             config.TrueNASAPIKey,
		InsecureSkipVerify: config.TrueNASInsecure,
		Logger:             config.Logger,
	}

	truenasClient := client.New(cfg)
	if err := truenasClient.Connect(ctx); err != nil {
		return nil, fmt.Errorf("failed to connect to TrueNAS: %w", err)
	}

	// Test connection
	if err := truenasClient.Ping(ctx); err != nil {
		truenasClient.Close()
		return nil, fmt.Errorf("failed to ping TrueNAS: %w", err)
	}

	// Validate that the default pool exists
	log.V(LogLevelInfo).Info("Validating pool exists in TrueNAS", "pool", config.DefaultPool)
	pool, err := truenasClient.GetPool(ctx, config.DefaultPool)
	if err != nil {
		return nil, fmt.Errorf("failed to validate pool '%s': %w\n\nPlease create the pool in TrueNAS UI (Storage → Create Pool) before using the CSI driver", config.DefaultPool, err)
	}
	log.V(LogLevelInfo).Info("Pool validated successfully", "pool", config.DefaultPool, "guid", pool.GUID)

	d := &Driver{
		name:         config.DriverName,
		version:      config.DriverVersion,
		nodeID:       config.NodeID,
		endpoint:     config.Endpoint,
		log:          log,
		client:       truenasClient,
		defaultPool:  config.DefaultPool,
		nfsServer:    config.NFSServer,
		iscsiPortal:  config.ISCSIPortal,
		iscsiIQNBase: config.ISCSIIQNBase,
	}

	d.initializeCapabilities()

	d.identityServer = NewIdentityServer(d)
	d.controllerServer = NewControllerServer(d)

	// Create node server (handlers are created internally)
	mounter := mount.New("")
	nodeServer, err := NewNodeServer(&NodeServerConfig{
		Driver:  d,
		Mounter: mounter,
	})
	if err != nil {
		truenasClient.Close()
		return nil, fmt.Errorf("failed to create node server: %w", err)
	}
	d.nodeServer = nodeServer

	return d, nil
}

// Run starts the CSI driver gRPC server on the configured endpoint.
// It blocks until the server is stopped or an error occurs.
func (d *Driver) Run(ctx context.Context) error {
	defer func() {
		if r := recover(); r != nil {
			d.log.V(LogLevelInfo).Info("Recovered from panic in CSI driver", "panic", r)
		}
	}()

	ctx, cancel := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	u, err := url.Parse(d.endpoint)
	if err != nil {
		return fmt.Errorf("failed to parse endpoint: %w", err)
	}

	var addr string
	switch u.Scheme {
	case "unix":
		addr = u.Path
		// Remove existing socket file if it exists
		if err := os.Remove(addr); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("failed to remove existing socket: %w", err)
		}

		// Create directory if needed
		if err := os.MkdirAll(filepath.Dir(addr), 0o755); err != nil {
			return fmt.Errorf("failed to create socket directory: %w", err)
		}
	case "tcp":
		addr = u.Host
	default:
		return fmt.Errorf("unsupported endpoint scheme: %s", u.Scheme)
	}

	listener, err := net.Listen(u.Scheme, addr)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", addr, err)
	}

	opts := []grpc.ServerOption{
		grpc.UnaryInterceptor(d.unaryInterceptor),
	}
	d.server = grpc.NewServer(opts...)

	csi.RegisterIdentityServer(d.server, d.identityServer)
	csi.RegisterControllerServer(d.server, d.controllerServer)
	csi.RegisterNodeServer(d.server, d.nodeServer)

	serverErr := make(chan error, 1)

	go func() {
		defer func() {
			if r := recover(); r != nil {
				d.log.V(LogLevelInfo).Info("Recovered from panic in gRPC server", "panic", r)
				serverErr <- fmt.Errorf("server panic: %v", r)
			}
		}()

		d.log.Info("TrueNAS CSI driver starting", "name", d.name, "version", d.version, "endpoint", d.endpoint)
		if err := d.server.Serve(listener); err != nil {
			serverErr <- err
		}
	}()

	select {
	case <-ctx.Done():
		d.log.V(LogLevelInfo).Info("Shutdown signal received, stopping CSI driver")
		d.Stop()
		return nil
	case err := <-serverErr:
		d.log.V(LogLevelInfo).Error(err, "Server error occurred")
		d.Stop()
		return fmt.Errorf("server error: %v", err)
	}
}

// Stop gracefully shuts down the CSI driver with a timeout.
func (d *Driver) Stop() {
	d.log.V(LogLevelInfo).Info("Stopping CSI driver server")

	// Graceful stop with timeout
	done := make(chan struct{})
	go func() {
		d.server.GracefulStop()
		close(done)
	}()

	select {
	case <-done:
		d.log.V(LogLevelInfo).Info("gRPC server stopped gracefully")
	case <-time.After(GracefulShutdownTimeout):
		d.log.V(LogLevelInfo).Info("Graceful shutdown timeout, forcing stop")
		d.server.Stop()
	}

	d.client.Close()
	d.log.Info("TrueNAS CSI driver stopped")
}

type requestIDKey struct{}

func generateRequestID() string {
	return fmt.Sprintf("%d-%04x", time.Now().UnixNano()%1000000, time.Now().Nanosecond()&0xFFFF)
}

func (d *Driver) unaryInterceptor(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	// Generate request ID for log correlation
	requestID := generateRequestID()
	ctx = context.WithValue(ctx, requestIDKey{}, requestID)

	startTime := time.Now()
	d.log.V(LogLevelDebug).Info("GRPC call started", "method", info.FullMethod, "requestId", requestID)
	d.log.V(LogLevelTrace).Info("GRPC request", "method", info.FullMethod, "requestId", requestID, "request", sanitizeRequest(req))

	resp, err := handler(ctx, req)

	duration := time.Since(startTime)
	if err != nil {
		d.log.Error(err, "GRPC call failed", "method", info.FullMethod, "requestId", requestID, "duration", duration)
	} else {
		d.log.V(LogLevelDebug).Info("GRPC call completed", "method", info.FullMethod, "requestId", requestID, "duration", duration)
		d.log.V(LogLevelTrace).Info("GRPC response", "method", info.FullMethod, "requestId", requestID, "response", resp)
	}

	return resp, err
}

// sanitizeRequest removes sensitive data from requests before logging.
func sanitizeRequest(req any) any {
	switch r := req.(type) {
	case *csi.CreateVolumeRequest:
		if r == nil {
			return nil
		}
		// Return a safe copy without potentially sensitive parameters
		safe := &struct {
			Name                string
			CapacityRange       *csi.CapacityRange
			VolumeCapabilities  int
			ParameterKeys       []string
			HasSecrets          bool
			HasVolumeSource     bool
			AccessibilityReqs   bool
		}{
			Name:               r.Name,
			CapacityRange:      r.CapacityRange,
			VolumeCapabilities: len(r.VolumeCapabilities),
			HasSecrets:         len(r.Secrets) > 0,
			HasVolumeSource:    r.VolumeContentSource != nil,
			AccessibilityReqs:  r.AccessibilityRequirements != nil,
		}
		for k := range r.Parameters {
			// Don't include values that might contain secrets
			if !strings.Contains(strings.ToLower(k), "secret") &&
				!strings.Contains(strings.ToLower(k), "password") &&
				!strings.Contains(strings.ToLower(k), "key") {
				safe.ParameterKeys = append(safe.ParameterKeys, k)
			}
		}
		return safe
	default:
		return req
	}
}

// initializeCapabilities sets up the controller, node, plugin, and volume capabilities.
func (d *Driver) initializeCapabilities() {
	d.controllerCaps = []*csi.ControllerServiceCapability{
		{
			Type: &csi.ControllerServiceCapability_Rpc{
				Rpc: &csi.ControllerServiceCapability_RPC{
					Type: csi.ControllerServiceCapability_RPC_CREATE_DELETE_VOLUME,
				},
			},
		},
		{
			Type: &csi.ControllerServiceCapability_Rpc{
				Rpc: &csi.ControllerServiceCapability_RPC{
					Type: csi.ControllerServiceCapability_RPC_PUBLISH_UNPUBLISH_VOLUME,
				},
			},
		},
		{
			Type: &csi.ControllerServiceCapability_Rpc{
				Rpc: &csi.ControllerServiceCapability_RPC{
					Type: csi.ControllerServiceCapability_RPC_CREATE_DELETE_SNAPSHOT,
				},
			},
		},
		{
			Type: &csi.ControllerServiceCapability_Rpc{
				Rpc: &csi.ControllerServiceCapability_RPC{
					Type: csi.ControllerServiceCapability_RPC_LIST_SNAPSHOTS,
				},
			},
		},
		{
			Type: &csi.ControllerServiceCapability_Rpc{
				Rpc: &csi.ControllerServiceCapability_RPC{
					Type: csi.ControllerServiceCapability_RPC_CLONE_VOLUME,
				},
			},
		},
		{
			Type: &csi.ControllerServiceCapability_Rpc{
				Rpc: &csi.ControllerServiceCapability_RPC{
					Type: csi.ControllerServiceCapability_RPC_EXPAND_VOLUME,
				},
			},
		},
		{
			Type: &csi.ControllerServiceCapability_Rpc{
				Rpc: &csi.ControllerServiceCapability_RPC{
					Type: csi.ControllerServiceCapability_RPC_LIST_VOLUMES,
				},
			},
		},
		{
			Type: &csi.ControllerServiceCapability_Rpc{
				Rpc: &csi.ControllerServiceCapability_RPC{
					Type: csi.ControllerServiceCapability_RPC_GET_CAPACITY,
				},
			},
		},
		{
			Type: &csi.ControllerServiceCapability_Rpc{
				Rpc: &csi.ControllerServiceCapability_RPC{
					Type: csi.ControllerServiceCapability_RPC_GET_VOLUME,
				},
			},
		},
	}

	// Node capabilities
	d.nodeCaps = []*csi.NodeServiceCapability{
		{
			Type: &csi.NodeServiceCapability_Rpc{
				Rpc: &csi.NodeServiceCapability_RPC{
					Type: csi.NodeServiceCapability_RPC_STAGE_UNSTAGE_VOLUME,
				},
			},
		},
		{
			Type: &csi.NodeServiceCapability_Rpc{
				Rpc: &csi.NodeServiceCapability_RPC{
					Type: csi.NodeServiceCapability_RPC_GET_VOLUME_STATS,
				},
			},
		},
		{
			Type: &csi.NodeServiceCapability_Rpc{
				Rpc: &csi.NodeServiceCapability_RPC{
					Type: csi.NodeServiceCapability_RPC_EXPAND_VOLUME,
				},
			},
		},
	}

	// Plugin capabilities
	d.pluginCaps = []*csi.PluginCapability{
		{
			Type: &csi.PluginCapability_Service_{
				Service: &csi.PluginCapability_Service{
					Type: csi.PluginCapability_Service_CONTROLLER_SERVICE,
				},
			},
		},
		{
			Type: &csi.PluginCapability_Service_{
				Service: &csi.PluginCapability_Service{
					Type: csi.PluginCapability_Service_VOLUME_ACCESSIBILITY_CONSTRAINTS,
				},
			},
		},
		{
			Type: &csi.PluginCapability_VolumeExpansion_{
				VolumeExpansion: &csi.PluginCapability_VolumeExpansion{
					Type: csi.PluginCapability_VolumeExpansion_ONLINE,
				},
			},
		},
	}

	// Volume access modes - only advertise modes we actually support
	// SINGLE_NODE_MULTI_WRITER requires cluster-aware filesystem which we don't provide
	d.volumeCaps = []*csi.VolumeCapability_AccessMode{
		{Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER},
		{Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_READER_ONLY},
		{Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_SINGLE_WRITER},
		{Mode: csi.VolumeCapability_AccessMode_MULTI_NODE_READER_ONLY},
		{Mode: csi.VolumeCapability_AccessMode_MULTI_NODE_SINGLE_WRITER},
		{Mode: csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER},
	}
}

// Client returns the TrueNAS client
func (d *Driver) Client() *client.Client {
	return d.client
}

// Log returns the driver's logger
func (d *Driver) Log() logr.Logger {
	return d.log
}

// NodeID returns the node identifier
func (d *Driver) NodeID() string {
	return d.nodeID
}

// NFSServer returns the configured NFS server address
func (d *Driver) NFSServer() string {
	return d.nfsServer
}

// ISCSIPortal returns the configured iSCSI portal address
func (d *Driver) ISCSIPortal() string {
	return d.iscsiPortal
}

// DefaultPool returns the default storage pool
func (d *Driver) DefaultPool() string {
	return d.defaultPool
}

// ISCSIIQNBase returns the base IQN for iSCSI targets
func (d *Driver) ISCSIIQNBase() string {
	return d.iscsiIQNBase
}

// ControllerCaps returns the controller capabilities
func (d *Driver) ControllerCaps() []*csi.ControllerServiceCapability {
	return d.controllerCaps
}

// NodeCaps returns the node capabilities
func (d *Driver) NodeCaps() []*csi.NodeServiceCapability {
	return d.nodeCaps
}

// VolumeCaps returns the volume capabilities
func (d *Driver) VolumeCaps() []*csi.VolumeCapability_AccessMode {
	return d.volumeCaps
}

// GenerateVolumeID creates a volume ID from pool and name
func (d *Driver) GenerateVolumeID(pool, name string) string {
	return fmt.Sprintf("%s/%s", pool, name)
}

// ParseVolumeID extracts pool and name from a volume ID
func (d *Driver) ParseVolumeID(volumeID string) (pool, name string, err error) {
	parts := strings.SplitN(volumeID, "/", 2)
	if len(parts) != 2 {
		return "", "", fmt.Errorf("invalid volume ID format: %s", volumeID)
	}
	return parts[0], parts[1], nil
}

// GetProtocolFromParameters extracts the protocol from StorageClass parameters
func (d *Driver) GetProtocolFromParameters(parameters map[string]string) string {
	if protocol, ok := parameters["protocol"]; ok {
		return strings.ToLower(protocol)
	}
	return ProtocolNFS
}

// GetPoolFromParameters extracts the pool from StorageClass parameters
func (d *Driver) GetPoolFromParameters(parameters map[string]string) string {
	if pool, ok := parameters["pool"]; ok {
		return pool
	}
	return d.defaultPool
}

// GetISCSIIQNBaseFromParameters extracts the IQN base from StorageClass parameters
func (d *Driver) GetISCSIIQNBaseFromParameters(parameters map[string]string) string {
	if iqnBase, ok := parameters["iscsi.iqn-base"]; ok {
		return iqnBase
	}
	if iqnBase, ok := parameters["iscsi.iqn-prefix"]; ok {
		return iqnBase
	}
	return d.iscsiIQNBase
}

// GetISCSIDeleteOptionsFromParameters parses iSCSI delete options from StorageClass parameters.
func (d *Driver) GetISCSIDeleteOptionsFromParameters(parameters map[string]string) *ISCSIDeleteOptions {
	opts := &ISCSIDeleteOptions{}

	if val, ok := parameters["forceDelete"]; ok {
		opts.ForceDelete = strings.ToLower(val) == "true"
	}

	if val, ok := parameters["deleteExtentsWithTarget"]; ok {
		opts.DeleteExtentsWithTarget = strings.ToLower(val) == "true"
	}

	return opts
}

// validateIQNFormat validates an iSCSI Qualified Name (IQN) format.
// Expected format: iqn.YYYY-MM.reverse.domain.name[:identifier]
func validateIQNFormat(iqn string) error {
	if !strings.HasPrefix(iqn, "iqn.") {
		return fmt.Errorf("IQN must start with 'iqn.'")
	}

	// Must have at least: iqn.YYYY-MM.domain
	parts := strings.Split(iqn, ".")
	if len(parts) < iqnMinParts {
		return fmt.Errorf("IQN format invalid: must be iqn.YYYY-MM.domain (got: %s)", iqn)
	}

	// Validate date format (YYYY-MM)
	dateField := parts[1]
	if len(dateField) != iqnDateFieldLen || dateField[iqnDateSeparator] != '-' {
		return fmt.Errorf("IQN date field must be YYYY-MM format (got: %s)", dateField)
	}

	return nil
}

// SanitizeVolumeName replaces invalid characters in volume names.
func SanitizeVolumeName(name string) string {
	name = strings.ReplaceAll(name, " ", "_")
	name = strings.ReplaceAll(name, ":", "_")
	name = strings.ReplaceAll(name, "@", "_")
	name = strings.ReplaceAll(name, "#", "_")

	// Ensure it doesn't start with a number or special character
	if len(name) > 0 && (name[0] < 'A' || (name[0] > 'Z' && name[0] < 'a') || name[0] > 'z') {
		name = "vol_" + name
	}

	return name
}

// GetVolumeInfo retrieves volume information from TrueNAS
func (d *Driver) GetVolumeInfo(volumeID string) (*VolumeInfo, error) {
	ctx := context.Background()
	return d.GetVolumeInfoWithContext(ctx, volumeID)
}

// GetVolumeInfoWithContext retrieves volume information from TrueNAS with context support
func (d *Driver) GetVolumeInfoWithContext(ctx context.Context, volumeID string) (*VolumeInfo, error) {
	return d.reconstructVolumeFromTrueNAS(ctx, volumeID)
}

// reconstructVolumeFromTrueNAS queries TrueNAS to rebuild volume metadata.
func (d *Driver) reconstructVolumeFromTrueNAS(ctx context.Context, volumeID string) (*VolumeInfo, error) {
	pool, datasetName, err := d.ParseVolumeID(volumeID)
	if err != nil {
		return nil, fmt.Errorf("invalid volume ID %s: %w", volumeID, err)
	}

	datasetPath := pool + "/" + datasetName

	// Query TrueNAS for the dataset
	dataset, err := d.client.GetDataset(ctx, datasetPath)
	if err != nil {
		return nil, fmt.Errorf("volume %s not found in TrueNAS: %w", volumeID, err)
	}

	// Reconstruct volume info based on dataset type
	volInfo := &VolumeInfo{
		ID:            volumeID,
		Name:          volumeID,
		DatasetPath:   datasetPath,
		PoolName:      pool,
		VolumeContext: make(map[string]string),
	}

	// Determine protocol and capacity based on dataset type
	if dataset.Type == "VOLUME" {
		volInfo.Protocol = ProtocolISCSI
		volInfo.CapacityBytes = dataset.Volsize
		if volInfo.CapacityBytes == 0 {
			volInfo.CapacityBytes = dataset.Used
		}

		// Query iSCSI target details from TrueNAS
		zvolPath := "zvol/" + datasetPath
		extent, err := d.client.GetISCSIExtentByDisk(ctx, zvolPath)
		if err == nil && extent != nil {
			volInfo.ISCSIExtentID = extent.ID

			// Find the target-extent association
			assoc, err := d.client.GetISCSITargetExtentByExtent(ctx, extent.ID)
			if err == nil && assoc != nil {
				volInfo.LUN = assoc.LunID

				// Get the target details
				target, err := d.client.GetISCSITargetByID(ctx, assoc.Target)
				if err == nil && target != nil {
					volInfo.ISCSITargetID = target.ID
					// Construct the full IQN
					volInfo.TargetIQN = d.iscsiIQNBase + ":" + target.Name
					volInfo.TargetPortal = d.iscsiPortal
					volInfo.VolumeContext["targetPortal"] = d.iscsiPortal
					volInfo.VolumeContext["targetIQN"] = volInfo.TargetIQN
					volInfo.VolumeContext["lun"] = fmt.Sprintf("%d", volInfo.LUN)
				}
			}
		}
		d.log.V(LogLevelDebug).Info("Reconstructed iSCSI volume", "volumeId", volumeID, "capacityBytes", volInfo.CapacityBytes,
			"targetIQN", volInfo.TargetIQN, "lun", volInfo.LUN)
	} else {
		// NFS filesystem
		volInfo.Protocol = ProtocolNFS
		volInfo.CapacityBytes = dataset.RefQuota
		volInfo.NFSPath = dataset.Mountpoint

		if d.nfsServer != "" {
			volInfo.VolumeContext["nfsServer"] = d.nfsServer
		}
		volInfo.VolumeContext["nfsPath"] = dataset.Mountpoint

		d.log.V(LogLevelDebug).Info("Reconstructed NFS volume", "volumeId", volumeID, "capacityBytes", volInfo.CapacityBytes, "path", dataset.Mountpoint)
	}

	d.log.V(LogLevelInfo).Info("Successfully reconstructed volume from TrueNAS", "volumeId", volumeID)
	return volInfo, nil
}
