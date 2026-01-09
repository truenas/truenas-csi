package client

import (
	"context"
	"fmt"
	"strconv"
	"strings"
)

// TrueNAS API method names for datasets
const (
	methodDatasetCreate = "pool.dataset.create"
	methodDatasetGet    = "pool.dataset.get_instance"
	methodDatasetQuery  = "pool.dataset.query"
	methodDatasetDelete = "pool.dataset.delete"
	methodDatasetUpdate = "pool.dataset.update"
)

// TrueNAS API method names for NFS shares
const (
	methodNFSCreate = "sharing.nfs.create"
	methodNFSGet    = "sharing.nfs.get_instance"
	methodNFSQuery  = "sharing.nfs.query"
	methodNFSDelete = "sharing.nfs.delete"
)

// TrueNAS API method names for iSCSI
const (
	methodISCSITargetCreate       = "iscsi.target.create"
	methodISCSITargetQuery        = "iscsi.target.query"
	methodISCSITargetDelete       = "iscsi.target.delete"
	methodISCSIExtentCreate       = "iscsi.extent.create"
	methodISCSIExtentQuery        = "iscsi.extent.query"
	methodISCSIExtentDelete       = "iscsi.extent.delete"
	methodISCSITargetExtentCreate = "iscsi.targetextent.create"
	methodISCSITargetExtentQuery  = "iscsi.targetextent.query"
	methodISCSITargetExtentDelete = "iscsi.targetextent.delete"
	methodISCSIAuthCreate         = "iscsi.auth.create"
	methodISCSIAuthQuery          = "iscsi.auth.query"
	methodISCSIAuthDelete         = "iscsi.auth.delete"
	methodISCSIInitiatorCreate    = "iscsi.initiator.create"
	methodISCSIInitiatorQuery     = "iscsi.initiator.query"
	methodISCSIInitiatorDelete    = "iscsi.initiator.delete"
)

// TrueNAS API method names for snapshots
const (
	methodSnapshotCreate = "pool.snapshot.create"
	methodSnapshotDelete = "pool.snapshot.delete"
	methodSnapshotClone  = "pool.snapshot.clone"
	methodSnapshotQuery  = "pool.snapshot.query"
)

// TrueNAS API method names for snapshot tasks (scheduled snapshots)
const (
	methodSnapshotTaskCreate = "pool.snapshottask.create"
	methodSnapshotTaskGet    = "pool.snapshottask.get_instance"
	methodSnapshotTaskQuery  = "pool.snapshottask.query"
	methodSnapshotTaskDelete = "pool.snapshottask.delete"
)

// TrueNAS API method names for pools
const (
	methodPoolQuery = "pool.query"
)

// TrueNAS API method names for ZFS resources
const (
	methodZFSResourceQuery = "zfs.resource.query"
)

// Default configuration values
const (
	defaultISCSIPortalID = 1
)

// Dataset represents a ZFS dataset in TrueNAS.
type Dataset struct {
	ID              string         `json:"id"`
	Name            string         `json:"name"`
	Pool            string         `json:"pool"`
	Type            string         `json:"type"`
	Mountpoint      string         `json:"mountpoint"`
	Used            int64          `json:"used"`
	Available       int64          `json:"available"`
	RefQuota        int64          `json:"refquota"`
	RefReservation  int64          `json:"refreservation"`
	Volsize         int64          `json:"volsize"`         // For ZVOLs (iSCSI volumes)
	Compression     any            `json:"compression"`     // Can be string or object in TrueNAS
	Deduplication   any            `json:"deduplication"`   // Can be string or object in TrueNAS
	Sync            any            `json:"sync"`            // Can be string or object in TrueNAS
	RecordSize      any            `json:"recordsize"`      // Can be string or object in TrueNAS
	ACLMode         any            `json:"aclmode"`         // Can be string or object in TrueNAS
	ACLType         any            `json:"acltype"`         // Can be string or object in TrueNAS
	ExtraProperties map[string]any `json:"extra_properties,omitempty"`
}

// DatasetCreateOptions specifies options for creating a dataset.
type DatasetCreateOptions struct {
	Name            string         `json:"name"`
	Type            string         `json:"type,omitempty"` // FILESYSTEM or VOLUME
	RefQuota        int64          `json:"refquota,omitempty"`
	RefReservation  int64          `json:"refreservation,omitempty"`
	Quota           int64          `json:"quota,omitempty"`
	Reservation     int64          `json:"reservation,omitempty"`
	Compression     string         `json:"compression,omitempty"`
	Deduplication   string         `json:"deduplication,omitempty"`
	Sync            string         `json:"sync,omitempty"`
	RecordSize      string         `json:"recordsize,omitempty"`
	Volsize         int64          `json:"volsize,omitempty"` // For ZVOLs
	Volblocksize    string         `json:"volblocksize,omitempty"`
	Comments        string         `json:"comments,omitempty"`
	CreateAncestors bool           `json:"create_ancestors,omitempty"`
	Properties      map[string]any `json:"properties,omitempty"`
	// Encryption options
	Encryption        bool               `json:"encryption,omitempty"`
	EncryptionOptions *EncryptionOptions `json:"encryption_options,omitempty"`
	InheritEncryption *bool              `json:"inherit_encryption,omitempty"`
}

// EncryptionOptions specifies encryption configuration for a dataset.
type EncryptionOptions struct {
	GenerateKey bool    `json:"generate_key,omitempty"`
	Pbkdf2iters int     `json:"pbkdf2iters,omitempty"` // Default: 350000, min: 100000
	Algorithm   string  `json:"algorithm,omitempty"`   // AES-256-GCM (default), AES-128-CCM, etc.
	Passphrase  *string `json:"passphrase,omitempty"`  // Min 8 chars, mutually exclusive with key
	Key         *string `json:"key,omitempty"`         // 64-char hex-encoded, mutually exclusive with passphrase
}

// DatasetUpdateOptions specifies options for updating a dataset.
type DatasetUpdateOptions struct {
	Comments         string              `json:"comments,omitempty"`
	Sync             string              `json:"sync,omitempty"`
	Compression      string              `json:"compression,omitempty"`
	Exec             string              `json:"exec,omitempty"`
	Quota            *int64              `json:"quota,omitempty"`
	RefQuota         *int64              `json:"refquota,omitempty"`
	Reservation      *int64              `json:"reservation,omitempty"`
	RefReservation   *int64              `json:"refreservation,omitempty"`
	Checksum         string              `json:"checksum,omitempty"`
	Deduplication    string              `json:"deduplication,omitempty"`
	Readonly         string              `json:"readonly,omitempty"`
	Atime            string              `json:"atime,omitempty"`
	RecordSize       string              `json:"recordsize,omitempty"`
	Volsize          *int64              `json:"volsize,omitempty"`
	QuotaWarning     *int64              `json:"quota_warning,omitempty"`
	QuotaCritical    *int64              `json:"quota_critical,omitempty"`
	RefQuotaWarning  *int64              `json:"refquota_warning,omitempty"`
	RefQuotaCritical *int64              `json:"refquota_critical,omitempty"`
	UserProperties   []map[string]string `json:"user_properties,omitempty"`
}

// QueryOptions specifies standard TrueNAS query options for .query and .get_instance methods.
type QueryOptions struct {
	Select          []string       `json:"select,omitempty"`
	OrderBy         []string       `json:"order_by,omitempty"`
	Count           bool           `json:"count,omitempty"`
	Get             bool           `json:"get,omitempty"`
	Offset          int            `json:"offset,omitempty"`
	Limit           int            `json:"limit,omitempty"`
	ForceSQLFilters bool           `json:"force_sql_filters,omitempty"`
	Extra           map[string]any `json:"extra,omitempty"`
}

// DatasetQueryOptions specifies options for querying datasets.
type DatasetQueryOptions struct {
	Extra DatasetGetExtraOptions `json:"extra"`
}

// DatasetGetExtraOptions specifies extra properties to retrieve for datasets.
type DatasetGetExtraOptions struct {
	Properties []string `json:"properties"`
}

// DatasetDeleteOptions specifies options for deleting a dataset.
type DatasetDeleteOptions struct {
	Recursive bool `json:"recursive"`
	Force     bool `json:"force"`
}

// NFSShare represents an NFS share in TrueNAS.
type NFSShare struct {
	ID       int      `json:"id"`
	Path     string   `json:"path"`
	Comment  string   `json:"comment,omitempty"`
	Hosts    []string `json:"hosts,omitempty"`
	ReadOnly bool     `json:"ro"`
	MapRoot  string   `json:"maproot,omitempty"`
	MapAll   string   `json:"mapall,omitempty"`
	Security []string `json:"security,omitempty"`
	Enabled  bool     `json:"enabled"`
	Networks []string `json:"networks,omitempty"`
}

// NFSShareCreateOptions specifies options for creating an NFS share.
type NFSShareCreateOptions struct {
	Path            string   `json:"path"`
	Comment         string   `json:"comment,omitempty"`
	Hosts           []string `json:"hosts,omitempty"`
	ReadOnly        bool     `json:"ro"`
	MapRootUser     *string  `json:"maproot_user,omitempty"`
	MapRootGroup    *string  `json:"maproot_group,omitempty"`
	MapAllUser      *string  `json:"mapall_user,omitempty"`
	MapAllGroup     *string  `json:"mapall_group,omitempty"`
	Security        []string `json:"security,omitempty"`
	Enabled         bool     `json:"enabled"`
	Networks        []string `json:"networks,omitempty"`
	ExposeSnapshots bool     `json:"expose_snapshots,omitempty"`
}

// ISCSITarget represents an iSCSI target in TrueNAS.
type ISCSITarget struct {
	ID     int                `json:"id"`
	Name   string             `json:"name"`
	Alias  string             `json:"alias,omitempty"`
	Mode   string             `json:"mode"`
	Groups []ISCSITargetGroup `json:"groups,omitempty"`
	Auth   *ISCSIAuth         `json:"auth,omitempty"`
}

// ISCSITargetCreateOptions specifies options for creating an iSCSI target.
type ISCSITargetCreateOptions struct {
	Name   string             `json:"name"`
	Alias  string             `json:"alias,omitempty"`
	Mode   string             `json:"mode"`
	Groups []ISCSITargetGroup `json:"groups,omitempty"`
}

// ISCSITargetGroup represents a portal group configuration for an iSCSI target.
type ISCSITargetGroup struct {
	Portal     int    `json:"portal"`
	Initiator  int    `json:"initiator,omitempty"`
	AuthMethod string `json:"authmethod,omitempty"`
	Auth       int    `json:"auth,omitempty"`
}

// ISCSIAuth represents CHAP authentication credentials for iSCSI.
type ISCSIAuth struct {
	ID            int    `json:"id"`
	Tag           int    `json:"tag"`
	User          string `json:"user"`
	Secret        string `json:"secret"`
	PeerUser      string `json:"peeruser,omitempty"`
	PeerSecret    string `json:"peersecret,omitempty"`
	DiscoveryAuth string `json:"discovery_auth,omitempty"` // NONE, CHAP, CHAP_MUTUAL
}

// ISCSIAuthCreateOptions specifies options for creating iSCSI authentication credentials.
type ISCSIAuthCreateOptions struct {
	Tag           int    `json:"tag"`
	User          string `json:"user"`
	Secret        string `json:"secret"`                   // 12-16 chars
	PeerUser      string `json:"peeruser,omitempty"`       // For mutual CHAP
	PeerSecret    string `json:"peersecret,omitempty"`     // 12-16 chars, must differ from secret
	DiscoveryAuth string `json:"discovery_auth,omitempty"` // NONE, CHAP, CHAP_MUTUAL (default: NONE)
}

// ISCSIExtent represents an iSCSI extent (LUN backing store) in TrueNAS.
type ISCSIExtent struct {
	ID             int    `json:"id"`
	Name           string `json:"name"`
	Type           string `json:"type"` // DISK or FILE
	Disk           string `json:"disk,omitempty"`
	Path           string `json:"path,omitempty"`
	FileSize       any    `json:"filesize,omitempty"` // Can be int64 or string in TrueNAS
	Serial         string `json:"serial,omitempty"`
	NAA            string `json:"naa,omitempty"`
	BlockSize      int    `json:"blocksize"`
	PBlockSize     bool   `json:"pblocksize"`
	AvailThreshold int    `json:"avail_threshold,omitempty"`
	Comment        string `json:"comment,omitempty"`
	InsecureTPC    bool   `json:"insecure_tpc"`
	XEN            bool   `json:"xen"`
	RPM            string `json:"rpm"`
	ReadOnly       bool   `json:"ro"`
	Enabled        bool   `json:"enabled"`
}

// ISCSIExtentCreateOptions specifies options for creating an iSCSI extent.
type ISCSIExtentCreateOptions struct {
	Name      string `json:"name"`
	Type      string `json:"type"`
	Disk      string `json:"disk,omitempty"`
	BlockSize int    `json:"blocksize"`
	Enabled   bool   `json:"enabled"`
}

// ISCSITargetExtent represents the association between an iSCSI target and extent.
type ISCSITargetExtent struct {
	ID     int `json:"id"`
	Target int `json:"target"`
	Extent int `json:"extent"`
	LunID  int `json:"lunid"`
}

// ISCSITargetExtentCreateOptions specifies options for associating an extent with a target.
type ISCSITargetExtentCreateOptions struct {
	Target int `json:"target"`
	Extent int `json:"extent"`
	LunID  int `json:"lunid"`
}

// ISCSITargetDeleteOptions specifies options for deleting an iSCSI target.
// API: iscsi.target.delete(id, force, delete_extents)
type ISCSITargetDeleteOptions struct {
	Force         bool // Force deletion even if target is in use
	DeleteExtents bool // Auto-delete associated extents (skips manual target-extent cleanup)
}

// ISCSIExtentDeleteOptions specifies options for deleting an iSCSI extent.
// API: iscsi.extent.delete(id, remove, force)
type ISCSIExtentDeleteOptions struct {
	Remove bool // For FILE type extents, delete the underlying file. No effect for DISK (zvol) type.
	Force  bool // Force deletion even if extent is in use
}

// ISCSITargetExtentDeleteOptions specifies options for deleting a target-extent association.
// API: iscsi.targetextent.delete(id, force)
type ISCSITargetExtentDeleteOptions struct {
	Force bool // Force deletion even if in use
}

// ISCSIPortal represents an iSCSI portal in TrueNAS.
type ISCSIPortal struct {
	ID                  int                 `json:"id"`
	Tag                 int                 `json:"tag"`
	Comment             string              `json:"comment,omitempty"`
	Listen              []ISCSIPortalListen `json:"listen"`
	DiscoveryAuthMethod string              `json:"discovery_authmethod,omitempty"`
	DiscoveryAuthGroup  int                 `json:"discovery_authgroup,omitempty"`
}

// ISCSIPortalListen represents a listen address for an iSCSI portal.
type ISCSIPortalListen struct {
	IP   string `json:"ip"`
	Port int    `json:"port"`
}

// ISCSIInitiator represents an iSCSI initiator configuration in TrueNAS.
type ISCSIInitiator struct {
	ID          int      `json:"id"`
	Initiators  []string `json:"initiators"`
	AuthNetwork []string `json:"auth_network,omitempty"`
	Comment     string   `json:"comment,omitempty"`
}

// ISCSIInitiatorCreateOptions specifies options for creating an iSCSI initiator group.
type ISCSIInitiatorCreateOptions struct {
	Initiators  []string `json:"initiators,omitempty"`   // List of initiator IQNs or patterns
	AuthNetwork []string `json:"auth_network,omitempty"` // List of allowed network CIDRs
	Comment     string   `json:"comment,omitempty"`
}

// Snapshot represents a ZFS snapshot in TrueNAS.
type Snapshot struct {
	ID         string         `json:"id"`
	Dataset    string         `json:"dataset"`
	Name       string         `json:"name"`
	CreateTime string         `json:"createtime"`
	Used       int64          `json:"used"`
	Referenced int64          `json:"referenced"`
	Properties map[string]any `json:"properties,omitempty"`
}

// SnapshotCreateOptions specifies options for creating a snapshot.
type SnapshotCreateOptions struct {
	Dataset   string `json:"dataset"`
	Name      string `json:"name"`
	Recursive bool   `json:"recursive"`
}

// SnapshotDeleteOptions specifies options for deleting a snapshot.
type SnapshotDeleteOptions struct {
	Defer     bool `json:"defer"`
	Recursive bool `json:"recursive"`
}

// SnapshotClone specifies parameters for cloning a snapshot.
type SnapshotClone struct {
	Snapshot   string `json:"snapshot"`
	DatasetDST string `json:"dataset_dst"`
}

// SnapshotTask represents a periodic snapshot task in TrueNAS.
type SnapshotTask struct {
	ID            int                   `json:"id"`
	Dataset       string                `json:"dataset"`
	Recursive     bool                  `json:"recursive"`
	LifetimeValue int                   `json:"lifetime_value"`
	LifetimeUnit  string                `json:"lifetime_unit"` // HOUR, DAY, WEEK, MONTH, YEAR
	Enabled       bool                  `json:"enabled"`
	Exclude       []string              `json:"exclude"`
	NamingSchema  string                `json:"naming_schema"`
	AllowEmpty    bool                  `json:"allow_empty"`
	Schedule      *SnapshotTaskSchedule `json:"schedule"`
	State         map[string]any        `json:"state,omitempty"`
	VMwareSync    bool                  `json:"vmware_sync,omitempty"`
}

// SnapshotTaskSchedule represents the cron schedule for a snapshot task.
type SnapshotTaskSchedule struct {
	Minute string `json:"minute"`
	Hour   string `json:"hour"`
	Dom    string `json:"dom"`   // Day of month
	Month  string `json:"month"`
	Dow    string `json:"dow"`   // Day of week
	Begin  string `json:"begin"` // Start time window
	End    string `json:"end"`   // End time window
}

// SnapshotTaskCreateOptions specifies options for creating a snapshot task.
type SnapshotTaskCreateOptions struct {
	Dataset       string                `json:"dataset"`
	Recursive     bool                  `json:"recursive,omitempty"`
	LifetimeValue int                   `json:"lifetime_value,omitempty"`
	LifetimeUnit  string                `json:"lifetime_unit,omitempty"` // HOUR, DAY, WEEK, MONTH, YEAR
	Enabled       bool                  `json:"enabled"`
	Exclude       []string              `json:"exclude,omitempty"`
	NamingSchema  string                `json:"naming_schema,omitempty"`
	AllowEmpty    bool                  `json:"allow_empty,omitempty"`
	Schedule      *SnapshotTaskSchedule `json:"schedule,omitempty"`
}

// SnapshotTaskDeleteOptions specifies options for deleting a snapshot task.
type SnapshotTaskDeleteOptions struct {
	FixateRemovalDate bool `json:"fixate_removal_date"`
}

// Pool represents a ZFS storage pool in TrueNAS.
type Pool struct {
	ID        int    `json:"id"`
	Name      string `json:"name"`
	GUID      string `json:"guid"`
	Status    string `json:"status"`
	Healthy   bool   `json:"healthy"`
	Size      int64  `json:"size"`
	Allocated int64  `json:"allocated"`
	Free      int64  `json:"free"`
	Path      string `json:"path"`
	Autotrim  any    `json:"autotrim"` // Can be bool or object in TrueNAS
}

// ZFSResourceQueryOptions specifies options for querying ZFS resources.
type ZFSResourceQueryOptions struct {
	Paths             []string `json:"paths"`
	Properties        []string `json:"properties,omitempty"`
	GetUserProperties bool     `json:"get_user_properties,omitempty"`
	GetSource         bool     `json:"get_source,omitempty"`
	NestResults       bool     `json:"nest_results,omitempty"`
	GetChildren       bool     `json:"get_children,omitempty"`
}

// ZFSResource represents a ZFS resource (pool, dataset, or volume) with its properties.
type ZFSResource struct {
	Name       string                 `json:"name"`
	Pool       string                 `json:"pool"`
	Type       string                 `json:"type"`
	Properties map[string]ZFSProperty `json:"properties"`
}

// ZFSProperty represents a ZFS property value with its source.
type ZFSProperty struct {
	Raw    string         `json:"raw"`
	Value  any            `json:"value"` // Can be int64, float64, string, bool, or null
	Source *ZFSPropSource `json:"source"`
}

// ZFSPropSource indicates the source of a ZFS property value.
type ZFSPropSource struct {
	Type  string `json:"type"`
	Value any    `json:"value"`
}

// GetInt64 returns the property value as int64.
func (p ZFSProperty) GetInt64() int64 {
	switch v := p.Value.(type) {
	case float64:
		return int64(v)
	case int64:
		return v
	case int:
		return int64(v)
	default:
		return 0
	}
}

// GetString returns the property value as string.
func (p ZFSProperty) GetString() string {
	switch v := p.Value.(type) {
	case string:
		return v
	default:
		return ""
	}
}

// CreateDataset creates a new ZFS dataset with the specified options.
func (c *Client) CreateDataset(ctx context.Context, options *DatasetCreateOptions) (*Dataset, error) {
	var result map[string]any
	err := c.Call(ctx, methodDatasetCreate, []any{options}, &result)
	if err != nil {
		return nil, fmt.Errorf("failed to create dataset: %w", err)
	}
	return parseDatasetResponse(result), nil
}

// GetDataset retrieves a dataset by its path.
// Returns ErrNotFound if the dataset does not exist.
func (c *Client) GetDataset(ctx context.Context, path string) (*Dataset, error) {
	// Request extra properties to include capacity fields (refquota, volsize)
	// An empty Properties list tells TrueNAS to not return extra properties
	options := &DatasetQueryOptions{
		Extra: DatasetGetExtraOptions{
			Properties: []string{"refquota", "volsize", "refreservation"},
		},
	}

	var result map[string]any
	err := c.Call(ctx, methodDatasetGet, []any{path, options}, &result)
	if err != nil {
		if IsNotFoundError(err) {
			return nil, fmt.Errorf("dataset %s: %w", path, ErrNotFound)
		}
		return nil, fmt.Errorf("failed to get dataset %s: %w", path, err)
	}

	return parseDatasetResponse(result), nil
}

// getString safely extracts a string value from a map.
func getString(m map[string]any, key string) string {
	if v, ok := m[key].(string); ok {
		return v
	}
	return ""
}

// getParsedInt64 extracts the "parsed" field from a TrueNAS property object.
// TrueNAS returns ZFS properties as objects like {"parsed": 1234, "rawvalue": "1234", ...}
// This function also handles direct numeric values and various object formats.
func getParsedInt64(m map[string]any, key string) int64 {
	v := m[key]
	if v == nil {
		return 0
	}

	// Handle direct numeric types (in case TrueNAS returns simple values)
	switch n := v.(type) {
	case float64:
		return int64(n)
	case int64:
		return n
	case int:
		return int64(n)
	}

	// Handle object format (TrueNAS property object)
	if prop, ok := v.(map[string]any); ok {
		// Try "parsed" field first (standard TrueNAS format)
		if parsed, ok := prop["parsed"].(float64); ok {
			return int64(parsed)
		}
		if parsed, ok := prop["parsed"].(int64); ok {
			return parsed
		}
		// Try "value" field as float64
		if value, ok := prop["value"].(float64); ok {
			return int64(value)
		}
		// Try "value" field as string and parse it
		if valueStr, ok := prop["value"].(string); ok {
			if i, err := strconv.ParseInt(valueStr, 10, 64); err == nil {
				return i
			}
		}
	}

	return 0
}

// getParsedString extracts the "parsed" or "value" field from a TrueNAS property object.
func getParsedString(m map[string]any, key string) string {
	if prop, ok := m[key].(map[string]any); ok {
		if parsed, ok := prop["parsed"].(string); ok {
			return parsed
		}
		if value, ok := prop["value"].(string); ok {
			return value
		}
	}
	return ""
}

// parseDatasetResponse parses a TrueNAS PoolDatasetEntry response into a Dataset.
func parseDatasetResponse(result map[string]any) *Dataset {
	refQuota := getParsedInt64(result, "refquota")
	volsize := getParsedInt64(result, "volsize")

	dataset := &Dataset{
		ID:             getString(result, "id"),
		Name:           getString(result, "name"),
		Pool:           getString(result, "pool"),
		Type:           getString(result, "type"),
		Mountpoint:     getString(result, "mountpoint"),
		Used:           getParsedInt64(result, "used"),
		Available:      getParsedInt64(result, "available"),
		RefQuota:       refQuota,
		RefReservation: getParsedInt64(result, "refreservation"),
		Volsize:        volsize,
	}

	// Store raw property objects for fields that can vary in type
	if compression, ok := result["compression"]; ok {
		dataset.Compression = compression
	}
	if dedup, ok := result["deduplication"]; ok {
		dataset.Deduplication = dedup
	}
	if sync, ok := result["sync"]; ok {
		dataset.Sync = sync
	}
	if recordsize, ok := result["recordsize"]; ok {
		dataset.RecordSize = recordsize
	}
	if aclmode, ok := result["aclmode"]; ok {
		dataset.ACLMode = aclmode
	}
	if acltype, ok := result["acltype"]; ok {
		dataset.ACLType = acltype
	}

	return dataset
}

// ListDatasets returns all datasets/volumes under a given pool.
// Uses pool.dataset.query for consistency with other dataset operations.
func (c *Client) ListDatasets(ctx context.Context, pool string) ([]Dataset, error) {
	// Filter for datasets in this pool
	filters := [][]any{
		{"pool", "=", pool},
	}

	options := map[string]any{
		"extra": map[string]any{
			"flat":              true,
			"retrieve_children": false,
			"properties":        []string{"type", "used", "available", "refquota", "volsize", "refreservation"},
		},
	}

	var results []map[string]any
	err := c.Call(ctx, methodDatasetQuery, []any{filters, options}, &results)
	if err != nil {
		return nil, fmt.Errorf("failed to list datasets in pool %s: %w", pool, err)
	}

	datasets := make([]Dataset, 0, len(results))
	for _, result := range results {
		datasets = append(datasets, *parseDatasetResponse(result))
	}

	return datasets, nil
}

// DeleteDataset deletes a dataset by its path.
func (c *Client) DeleteDataset(ctx context.Context, path string, options *DatasetDeleteOptions) error {
	err := c.Call(ctx, methodDatasetDelete, []any{path, options}, nil)
	if err != nil {
		return fmt.Errorf("failed to delete dataset %s: %w", path, err)
	}
	return nil
}

// UpdateDataset updates a dataset with the specified options.
func (c *Client) UpdateDataset(ctx context.Context, path string, updates *DatasetUpdateOptions) error {
	var result any
	err := c.Call(ctx, methodDatasetUpdate, []any{path, updates}, &result)
	if err != nil {
		return fmt.Errorf("failed to update dataset %s: %w", path, err)
	}
	return nil
}

// CreateNFSShare creates a new NFS share with the specified options.
func (c *Client) CreateNFSShare(ctx context.Context, options *NFSShareCreateOptions) (*NFSShare, error) {
	var share NFSShare
	err := c.Call(ctx, methodNFSCreate, []any{options}, &share)
	if err != nil {
		return nil, fmt.Errorf("failed to create NFS share: %w", err)
	}
	return &share, nil
}

// GetNFSShare retrieves an NFS share by its ID.
func (c *Client) GetNFSShare(ctx context.Context, id int) (*NFSShare, error) {
	options := &QueryOptions{}

	var share NFSShare
	err := c.Call(ctx, methodNFSGet, []any{id, options}, &share)
	if err != nil {
		return nil, fmt.Errorf("failed to get NFS share %d: %w", id, err)
	}
	return &share, nil
}

// GetNFSShareByPath retrieves an NFS share by its filesystem path.
func (c *Client) GetNFSShareByPath(ctx context.Context, path string) (*NFSShare, error) {
	filters := [][]any{
		{"path", "=", path},
	}
	options := &QueryOptions{}

	var shares []NFSShare
	err := c.Call(ctx, methodNFSQuery, []any{filters, options}, &shares)
	if err != nil {
		return nil, fmt.Errorf("failed to query NFS shares: %w", err)
	}

	if len(shares) == 0 {
		return nil, fmt.Errorf("NFS share not found for path %s", path)
	}

	return &shares[0], nil
}

// DeleteNFSShare deletes an NFS share by its ID.
func (c *Client) DeleteNFSShare(ctx context.Context, id int) error {
	err := c.Call(ctx, methodNFSDelete, []any{id}, nil)
	if err != nil {
		return fmt.Errorf("failed to delete NFS share %d: %w", id, err)
	}
	return nil
}

// GetISCSITargetByName retrieves an iSCSI target by its name.
// Returns ErrNotFound if the target does not exist.
func (c *Client) GetISCSITargetByName(ctx context.Context, name string) (*ISCSITarget, error) {
	filters := [][]any{
		{"name", "=", name},
	}
	options := &QueryOptions{}

	var targets []ISCSITarget
	err := c.Call(ctx, methodISCSITargetQuery, []any{filters, options}, &targets)
	if err != nil {
		return nil, fmt.Errorf("failed to query iSCSI targets: %w", err)
	}

	if len(targets) == 0 {
		return nil, ErrNotFound
	}

	return &targets[0], nil
}

// GetISCSIExtentByName retrieves an iSCSI extent by its name.
// Returns ErrNotFound if the extent does not exist.
func (c *Client) GetISCSIExtentByName(ctx context.Context, name string) (*ISCSIExtent, error) {
	filters := [][]any{
		{"name", "=", name},
	}
	options := &QueryOptions{}

	var extents []ISCSIExtent
	err := c.Call(ctx, methodISCSIExtentQuery, []any{filters, options}, &extents)
	if err != nil {
		return nil, fmt.Errorf("failed to query iSCSI extents: %w", err)
	}

	if len(extents) == 0 {
		return nil, ErrNotFound
	}

	return &extents[0], nil
}

// GetISCSIExtentByDisk retrieves an iSCSI extent by its disk path (e.g., "zvol/pool/volume").
// Returns ErrNotFound if the extent does not exist.
func (c *Client) GetISCSIExtentByDisk(ctx context.Context, disk string) (*ISCSIExtent, error) {
	filters := [][]any{
		{"disk", "=", disk},
	}
	options := &QueryOptions{}

	var extents []ISCSIExtent
	err := c.Call(ctx, methodISCSIExtentQuery, []any{filters, options}, &extents)
	if err != nil {
		return nil, fmt.Errorf("failed to query iSCSI extents by disk: %w", err)
	}

	if len(extents) == 0 {
		return nil, ErrNotFound
	}

	return &extents[0], nil
}

// GetISCSITargetExtentByExtent retrieves the target-extent association for a given extent ID.
// Returns ErrNotFound if no association exists.
func (c *Client) GetISCSITargetExtentByExtent(ctx context.Context, extentID int) (*ISCSITargetExtent, error) {
	filters := [][]any{
		{"extent", "=", extentID},
	}
	options := &QueryOptions{}

	var assocs []ISCSITargetExtent
	err := c.Call(ctx, methodISCSITargetExtentQuery, []any{filters, options}, &assocs)
	if err != nil {
		return nil, fmt.Errorf("failed to query iSCSI target-extent associations: %w", err)
	}

	if len(assocs) == 0 {
		return nil, ErrNotFound
	}

	return &assocs[0], nil
}

// GetISCSITargetByID retrieves an iSCSI target by its ID.
// Returns ErrNotFound if the target does not exist.
func (c *Client) GetISCSITargetByID(ctx context.Context, id int) (*ISCSITarget, error) {
	filters := [][]any{
		{"id", "=", id},
	}
	options := &QueryOptions{}

	var targets []ISCSITarget
	err := c.Call(ctx, methodISCSITargetQuery, []any{filters, options}, &targets)
	if err != nil {
		return nil, fmt.Errorf("failed to query iSCSI target by ID: %w", err)
	}

	if len(targets) == 0 {
		return nil, ErrNotFound
	}

	return &targets[0], nil
}

// CreateISCSITarget creates a new iSCSI target with the specified name and alias.
func (c *Client) CreateISCSITarget(ctx context.Context, name, alias string) (*ISCSITarget, error) {
	return c.CreateISCSITargetWithAuth(ctx, name, alias, 0, 0)
}

// CreateISCSITargetWithAuth creates a new iSCSI target with optional auth and initiator groups.
// authTag: CHAP authentication group tag (0 to skip)
// initiatorID: Initiator group ID (0 to skip)
func (c *Client) CreateISCSITargetWithAuth(ctx context.Context, name, alias string, authTag, initiatorID int) (*ISCSITarget, error) {
	group := ISCSITargetGroup{
		Portal: defaultISCSIPortalID,
	}

	if authTag > 0 {
		group.AuthMethod = "CHAP"
		group.Auth = authTag
	}

	if initiatorID > 0 {
		group.Initiator = initiatorID
	}

	params := &ISCSITargetCreateOptions{
		Name:   name,
		Alias:  alias,
		Mode:   "ISCSI",
		Groups: []ISCSITargetGroup{group},
	}

	var target ISCSITarget
	err := c.Call(ctx, methodISCSITargetCreate, []any{params}, &target)
	if err != nil {
		return nil, fmt.Errorf("failed to create iSCSI target: %w", err)
	}
	return &target, nil
}

// CreateISCSIExtent creates a new iSCSI extent backed by a disk.
func (c *Client) CreateISCSIExtent(ctx context.Context, name, disk string, blocksize int) (*ISCSIExtent, error) {
	params := &ISCSIExtentCreateOptions{
		Name:      name,
		Type:      "DISK",
		Disk:      disk,
		BlockSize: blocksize,
		Enabled:   true,
	}

	var extent ISCSIExtent
	err := c.Call(ctx, methodISCSIExtentCreate, []any{params}, &extent)
	if err != nil {
		return nil, fmt.Errorf("failed to create iSCSI extent: %w", err)
	}
	return &extent, nil
}

// CreateISCSITargetExtent associates an iSCSI extent with a target.
func (c *Client) CreateISCSITargetExtent(ctx context.Context, targetID, extentID, lunID int) (*ISCSITargetExtent, error) {
	params := &ISCSITargetExtentCreateOptions{
		Target: targetID,
		Extent: extentID,
		LunID:  lunID,
	}

	var targetExtent ISCSITargetExtent
	err := c.Call(ctx, methodISCSITargetExtentCreate, []any{params}, &targetExtent)
	if err != nil {
		return nil, fmt.Errorf("failed to create target-extent association: %w", err)
	}
	return &targetExtent, nil
}

// DeleteISCSITarget deletes an iSCSI target.
// If opts.DeleteExtents is false (default), this method first queries and deletes all
// target-extent associations manually before deleting the target.
// If opts.DeleteExtents is true, the API will auto-delete associated extents.
func (c *Client) DeleteISCSITarget(ctx context.Context, id int, opts *ISCSITargetDeleteOptions) error {
	if opts == nil {
		opts = &ISCSITargetDeleteOptions{}
	}

	// Only manually delete target-extent associations if not using auto-delete
	if !opts.DeleteExtents {
		var targetExtents []ISCSITargetExtent
		filters := [][]any{
			{"target", "=", id},
		}
		queryOpts := &QueryOptions{}

		err := c.Call(ctx, methodISCSITargetExtentQuery, []any{filters, queryOpts}, &targetExtents)
		if err != nil {
			return fmt.Errorf("failed to query target-extent associations: %w", err)
		}

		teDeleteOpts := &ISCSITargetExtentDeleteOptions{Force: opts.Force}
		for _, te := range targetExtents {
			if err := c.DeleteISCSITargetExtent(ctx, te.ID, teDeleteOpts); err != nil {
				return fmt.Errorf("failed to delete target-extent %d: %w", te.ID, err)
			}
		}
	}

	err := c.Call(ctx, methodISCSITargetDelete, []any{id, opts.Force, opts.DeleteExtents}, nil)
	if err != nil {
		return fmt.Errorf("failed to delete iSCSI target %d: %w", id, err)
	}
	return nil
}

// DeleteISCSIExtent deletes an iSCSI extent by its ID.
func (c *Client) DeleteISCSIExtent(ctx context.Context, id int, opts *ISCSIExtentDeleteOptions) error {
	if opts == nil {
		opts = &ISCSIExtentDeleteOptions{}
	}

	err := c.Call(ctx, methodISCSIExtentDelete, []any{id, opts.Remove, opts.Force}, nil)
	if err != nil {
		return fmt.Errorf("failed to delete iSCSI extent %d: %w", id, err)
	}
	return nil
}

// DeleteISCSITargetExtent deletes a target-extent association by its ID.
func (c *Client) DeleteISCSITargetExtent(ctx context.Context, id int, opts *ISCSITargetExtentDeleteOptions) error {
	if opts == nil {
		opts = &ISCSITargetExtentDeleteOptions{}
	}

	err := c.Call(ctx, methodISCSITargetExtentDelete, []any{id, opts.Force}, nil)
	if err != nil {
		return fmt.Errorf("failed to delete target-extent %d: %w", id, err)
	}
	return nil
}

// CreateISCSIAuth creates a new iSCSI authentication credential (CHAP).
func (c *Client) CreateISCSIAuth(ctx context.Context, opts *ISCSIAuthCreateOptions) (*ISCSIAuth, error) {
	var auth ISCSIAuth
	err := c.Call(ctx, methodISCSIAuthCreate, []any{opts}, &auth)
	if err != nil {
		return nil, fmt.Errorf("failed to create iSCSI auth: %w", err)
	}
	return &auth, nil
}

// GetISCSIAuthByTag retrieves an iSCSI authentication credential by its tag.
func (c *Client) GetISCSIAuthByTag(ctx context.Context, tag int) (*ISCSIAuth, error) {
	filters := [][]any{
		{"tag", "=", tag},
	}
	options := &QueryOptions{}

	var auths []ISCSIAuth
	err := c.Call(ctx, methodISCSIAuthQuery, []any{filters, options}, &auths)
	if err != nil {
		return nil, fmt.Errorf("failed to query iSCSI auth: %w", err)
	}

	if len(auths) == 0 {
		return nil, ErrNotFound
	}

	return &auths[0], nil
}

// DeleteISCSIAuth deletes an iSCSI authentication credential by its ID.
func (c *Client) DeleteISCSIAuth(ctx context.Context, id int) error {
	err := c.Call(ctx, methodISCSIAuthDelete, []any{id}, nil)
	if err != nil {
		return fmt.Errorf("failed to delete iSCSI auth %d: %w", id, err)
	}
	return nil
}

// GetNextISCSIAuthTag returns the next available iSCSI auth tag.
func (c *Client) GetNextISCSIAuthTag(ctx context.Context) (int, error) {
	var auths []ISCSIAuth
	err := c.Call(ctx, methodISCSIAuthQuery, []any{[][]any{}, &QueryOptions{}}, &auths)
	if err != nil {
		return 0, fmt.Errorf("failed to query iSCSI auth: %w", err)
	}

	maxTag := 0
	for _, auth := range auths {
		if auth.Tag > maxTag {
			maxTag = auth.Tag
		}
	}
	return maxTag + 1, nil
}

// CreateISCSIInitiator creates a new iSCSI initiator group.
func (c *Client) CreateISCSIInitiator(ctx context.Context, opts *ISCSIInitiatorCreateOptions) (*ISCSIInitiator, error) {
	var initiator ISCSIInitiator
	err := c.Call(ctx, methodISCSIInitiatorCreate, []any{opts}, &initiator)
	if err != nil {
		return nil, fmt.Errorf("failed to create iSCSI initiator: %w", err)
	}
	return &initiator, nil
}

// DeleteISCSIInitiator deletes an iSCSI initiator group by its ID.
func (c *Client) DeleteISCSIInitiator(ctx context.Context, id int) error {
	err := c.Call(ctx, methodISCSIInitiatorDelete, []any{id}, nil)
	if err != nil {
		return fmt.Errorf("failed to delete iSCSI initiator %d: %w", id, err)
	}
	return nil
}

// CreateSnapshot creates a new ZFS snapshot.
func (c *Client) CreateSnapshot(ctx context.Context, dataset, name string, recursive bool) (*Snapshot, error) {
	params := &SnapshotCreateOptions{
		Dataset:   dataset,
		Name:      name,
		Recursive: recursive,
	}

	var snapshot Snapshot
	err := c.Call(ctx, methodSnapshotCreate, []any{params}, &snapshot)
	if err != nil {
		return nil, fmt.Errorf("failed to create snapshot: %w", err)
	}
	return &snapshot, nil
}

// DeleteSnapshot deletes a ZFS snapshot by name.
func (c *Client) DeleteSnapshot(ctx context.Context, name string) error {
	options := &SnapshotDeleteOptions{
		Defer: false,
	}

	err := c.Call(ctx, methodSnapshotDelete, []any{name, options}, nil)
	if err != nil {
		return fmt.Errorf("failed to delete snapshot %s: %w", name, err)
	}
	return nil
}

// CloneSnapshot clones a ZFS snapshot to a new dataset.
func (c *Client) CloneSnapshot(ctx context.Context, snapshot, destination string) (*Dataset, error) {
	params := SnapshotClone{
		Snapshot:   snapshot,
		DatasetDST: destination,
	}

	err := c.Call(ctx, methodSnapshotClone, []any{params}, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to clone snapshot: %w", err)
	}

	return c.GetDataset(ctx, destination)
}

// ListSnapshots returns all snapshots for a given dataset.
func (c *Client) ListSnapshots(ctx context.Context, dataset string) ([]Snapshot, error) {
	filters := [][]any{
		{"dataset", "=", dataset},
	}
	options := &QueryOptions{}

	var snapshots []Snapshot
	err := c.Call(ctx, methodSnapshotQuery, []any{filters, options}, &snapshots)
	if err != nil {
		return nil, fmt.Errorf("failed to list snapshots: %w", err)
	}
	return snapshots, nil
}

// FindSnapshotByName searches for a snapshot by name across all datasets.
// The name parameter is the snapshot name (part after @), not the full snapshot ID.
// Returns the first matching snapshot or nil if not found.
func (c *Client) FindSnapshotByName(ctx context.Context, name string) (*Snapshot, error) {
	// Use snapshot_name filter to search by the name part
	filters := [][]any{
		{"snapshot_name", "=", name},
	}
	options := &QueryOptions{}

	var snapshots []Snapshot
	err := c.Call(ctx, methodSnapshotQuery, []any{filters, options}, &snapshots)
	if err != nil {
		return nil, fmt.Errorf("failed to find snapshot by name: %w", err)
	}

	if len(snapshots) == 0 {
		return nil, nil
	}
	return &snapshots[0], nil
}

// ListAllSnapshots returns all snapshots across all datasets.
func (c *Client) ListAllSnapshots(ctx context.Context) ([]Snapshot, error) {
	// Empty filter list means return all snapshots
	filters := [][]any{}
	options := &QueryOptions{}

	var snapshots []Snapshot
	err := c.Call(ctx, methodSnapshotQuery, []any{filters, options}, &snapshots)
	if err != nil {
		return nil, fmt.Errorf("failed to list all snapshots: %w", err)
	}
	return snapshots, nil
}

// CreateSnapshotTask creates a new periodic snapshot task.
func (c *Client) CreateSnapshotTask(ctx context.Context, opts *SnapshotTaskCreateOptions) (*SnapshotTask, error) {
	var task SnapshotTask
	err := c.Call(ctx, methodSnapshotTaskCreate, []any{opts}, &task)
	if err != nil {
		return nil, fmt.Errorf("failed to create snapshot task: %w", err)
	}
	return &task, nil
}

// GetSnapshotTask retrieves a snapshot task by its ID.
func (c *Client) GetSnapshotTask(ctx context.Context, id int) (*SnapshotTask, error) {
	var task SnapshotTask
	err := c.Call(ctx, methodSnapshotTaskGet, []any{id}, &task)
	if err != nil {
		if IsNotFoundError(err) {
			return nil, fmt.Errorf("snapshot task %d: %w", id, ErrNotFound)
		}
		return nil, fmt.Errorf("failed to get snapshot task %d: %w", id, err)
	}
	return &task, nil
}

// GetSnapshotTaskByDataset retrieves a snapshot task by its dataset path.
func (c *Client) GetSnapshotTaskByDataset(ctx context.Context, dataset string) (*SnapshotTask, error) {
	filters := [][]any{
		{"dataset", "=", dataset},
	}
	options := &QueryOptions{}

	var tasks []SnapshotTask
	err := c.Call(ctx, methodSnapshotTaskQuery, []any{filters, options}, &tasks)
	if err != nil {
		return nil, fmt.Errorf("failed to query snapshot tasks: %w", err)
	}

	if len(tasks) == 0 {
		return nil, ErrNotFound
	}

	return &tasks[0], nil
}

// ListSnapshotTasks returns all snapshot tasks for a given dataset (including child datasets if recursive).
func (c *Client) ListSnapshotTasks(ctx context.Context, dataset string) ([]SnapshotTask, error) {
	filters := [][]any{
		{"dataset", "=", dataset},
	}
	options := &QueryOptions{}

	var tasks []SnapshotTask
	err := c.Call(ctx, methodSnapshotTaskQuery, []any{filters, options}, &tasks)
	if err != nil {
		return nil, fmt.Errorf("failed to list snapshot tasks: %w", err)
	}
	return tasks, nil
}

// DeleteSnapshotTask deletes a snapshot task by its ID.
func (c *Client) DeleteSnapshotTask(ctx context.Context, id int, opts *SnapshotTaskDeleteOptions) error {
	if opts == nil {
		opts = &SnapshotTaskDeleteOptions{}
	}

	err := c.Call(ctx, methodSnapshotTaskDelete, []any{id, opts}, nil)
	if err != nil {
		return fmt.Errorf("failed to delete snapshot task %d: %w", id, err)
	}
	return nil
}

// GetPool retrieves a storage pool by name.
func (c *Client) GetPool(ctx context.Context, name string) (*Pool, error) {
	var pools []Pool
	filters := [][]any{
		{"name", "=", name},
	}
	options := &QueryOptions{}

	err := c.Call(ctx, methodPoolQuery, []any{filters, options}, &pools)
	if err != nil {
		return nil, fmt.Errorf("failed to query pool %s: %w", name, err)
	}
	if len(pools) == 0 {
		return nil, fmt.Errorf("pool '%s' not found in TrueNAS", name)
	}
	return &pools[0], nil
}

// ListPools returns all storage pools.
func (c *Client) ListPools(ctx context.Context) ([]Pool, error) {
	var pools []Pool
	filters := [][]any{}
	options := &QueryOptions{}

	err := c.Call(ctx, methodPoolQuery, []any{filters, options}, &pools)
	if err != nil {
		return nil, fmt.Errorf("failed to list pools: %w", err)
	}
	return pools, nil
}

// GetAvailableSpace returns the available space in bytes for a pool or dataset.
func (c *Client) GetAvailableSpace(ctx context.Context, poolName string) (int64, error) {
	options := &ZFSResourceQueryOptions{
		Paths:      []string{poolName},
		Properties: []string{"available"},
		GetSource:  false,
	}

	var resources []ZFSResource
	err := c.Call(ctx, methodZFSResourceQuery, []any{options}, &resources)
	if err != nil {
		return 0, fmt.Errorf("failed to query ZFS resource %s: %w", poolName, err)
	}

	if len(resources) == 0 {
		return 0, fmt.Errorf("ZFS resource '%s' not found", poolName)
	}

	resource := resources[0]
	availableProp, ok := resource.Properties["available"]
	if !ok {
		return 0, fmt.Errorf("'available' property not found for ZFS resource %s", poolName)
	}

	switch v := availableProp.Value.(type) {
	case float64:
		return int64(v), nil
	case int64:
		return v, nil
	case string:
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return 0, fmt.Errorf("unexpected string value %v for 'available' property of ZFS resource %s", v, poolName)
		}
		return n, nil
	case nil:
		return 0, fmt.Errorf("'available' property is null for ZFS resource %s", poolName)
	default:
		return 0, fmt.Errorf("unexpected type %T for 'available' property of ZFS resource %s", v, poolName)
	}
}

// ExtractPoolFromPath extracts the pool name from a dataset path.
func ExtractPoolFromPath(path string) string {
	parts := strings.Split(path, "/")
	if len(parts) > 0 {
		return parts[0]
	}
	return ""
}
