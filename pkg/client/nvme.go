package client

import (
	"context"
	"fmt"
)

// NVMe-oF (nvmet) API method names.
const (
	methodNVMetGlobalConfig = "nvmet.global.config"

	methodNVMetSubsysCreate = "nvmet.subsys.create"
	methodNVMetSubsysQuery  = "nvmet.subsys.query"
	methodNVMetSubsysUpdate = "nvmet.subsys.update"
	methodNVMetSubsysDelete = "nvmet.subsys.delete"

	methodNVMetNamespaceCreate = "nvmet.namespace.create"
	methodNVMetNamespaceQuery  = "nvmet.namespace.query"
	methodNVMetNamespaceDelete = "nvmet.namespace.delete"

	methodNVMetPortCreate = "nvmet.port.create"
	methodNVMetPortQuery  = "nvmet.port.query"
	methodNVMetPortDelete = "nvmet.port.delete"

	methodNVMetPortSubsysCreate = "nvmet.port_subsys.create"
	methodNVMetPortSubsysQuery  = "nvmet.port_subsys.query"
	methodNVMetPortSubsysDelete = "nvmet.port_subsys.delete"

	methodNVMetHostCreate = "nvmet.host.create"
	methodNVMetHostQuery  = "nvmet.host.query"
	methodNVMetHostDelete = "nvmet.host.delete"

	methodNVMetHostSubsysCreate = "nvmet.host_subsys.create"
	methodNVMetHostSubsysQuery  = "nvmet.host_subsys.query"
	methodNVMetHostSubsysDelete = "nvmet.host_subsys.delete"
)

// NVMe-oF transport / device-type values.
const (
	NVMeTransportTCP   = "TCP"
	NVMeDeviceTypeZVOL = "ZVOL"
	NVMeDeviceTypeFile = "FILE"
	// NVMeDefaultPort is the standard NVMe/TCP service port.
	NVMeDefaultPort = 4420
)

// NVMeGlobalConfig is the nvmet global configuration (nvmet.global.config).
type NVMeGlobalConfig struct {
	ID            int    `json:"id"`
	BaseNQN       string `json:"basenqn"`
	Kernel        bool   `json:"kernel"`
	ANA           bool   `json:"ana"`
	RDMA          bool   `json:"rdma"`
	XportReferral bool   `json:"xport_referral"`
}

// NVMeSubsystem represents an NVMe-oF subsystem (nvmet.subsys). SubNQN is generated
// by TrueNAS from the global basenqn when not supplied on create.
type NVMeSubsystem struct {
	ID           int    `json:"id"`
	Name         string `json:"name"`
	SubNQN       string `json:"subnqn"`
	AllowAnyHost bool   `json:"allow_any_host"`
	Serial       string `json:"serial,omitempty"`
	Hosts        []int  `json:"hosts,omitempty"`
	Namespaces   []int  `json:"namespaces,omitempty"`
	Ports        []int  `json:"ports,omitempty"`
}

// NVMeSubsysCreateOptions specifies options for creating a subsystem. subnqn is
// intentionally omitted so TrueNAS generates it from the global basenqn.
type NVMeSubsysCreateOptions struct {
	Name         string `json:"name"`
	AllowAnyHost bool   `json:"allow_any_host"`
}

// NVMeNamespace represents an NVMe-oF namespace (nvmet.namespace). DeviceUUID is
// server-generated and used for node-side device discovery; Subsys is nested in
// query responses.
type NVMeNamespace struct {
	ID          int            `json:"id"`
	NSID        int            `json:"nsid"`
	DeviceType  string         `json:"device_type"`
	DevicePath  string         `json:"device_path"`
	DeviceUUID  string         `json:"device_uuid"`
	DeviceNGUID string         `json:"device_nguid"`
	Enabled     bool           `json:"enabled"`
	Subsys      *NVMeSubsystem `json:"subsys,omitempty"`
}

// NVMeNamespaceCreateOptions specifies options for creating a namespace.
type NVMeNamespaceCreateOptions struct {
	SubsysID   int    `json:"subsys_id"`
	DeviceType string `json:"device_type"` // ZVOL or FILE
	DevicePath string `json:"device_path"` // e.g. "zvol/pool/volume"
	Enabled    bool   `json:"enabled"`
}

// NVMePort represents an NVMe-oF transport port (nvmet.port). AddrAdrFam is derived
// by the server and is read-only. AddrTrSvcID may be returned as int or string.
type NVMePort struct {
	ID          int    `json:"id"`
	Index       int    `json:"index"`
	AddrTrType  string `json:"addr_trtype"`
	AddrAdrFam  string `json:"addr_adrfam"`
	AddrTrAddr  string `json:"addr_traddr"`
	AddrTrSvcID any    `json:"addr_trsvcid"`
	Enabled     bool   `json:"enabled"`
}

// NVMePortCreateOptions specifies options for creating a TCP port. addr_adrfam is
// derived by the server and must not be sent.
type NVMePortCreateOptions struct {
	AddrTrType  string `json:"addr_trtype"`  // TCP
	AddrTrAddr  string `json:"addr_traddr"`  // listen IP
	AddrTrSvcID int    `json:"addr_trsvcid"` // port, e.g. 4420
}

// NVMePortSubsys associates a port with a subsystem (nvmet.port_subsys).
type NVMePortSubsys struct {
	ID     int            `json:"id"`
	Port   *NVMePort      `json:"port,omitempty"`
	Subsys *NVMeSubsystem `json:"subsys,omitempty"`
}

// NVMePortSubsysCreateOptions specifies options for linking a port to a subsystem.
type NVMePortSubsysCreateOptions struct {
	PortID   int `json:"port_id"`
	SubsysID int `json:"subsys_id"`
}

// NVMeHost represents an authorized NVMe-oF host (nvmet.host). DH-CHAP hash/dhgroup
// use TrueNAS's exact string forms (e.g. "SHA-256", "2048-BIT").
type NVMeHost struct {
	ID            int    `json:"id"`
	HostNQN       string `json:"hostnqn"`
	DHCHAPKey     string `json:"dhchap_key,omitempty"`
	DHCHAPCtrlKey string `json:"dhchap_ctrl_key,omitempty"`
	DHCHAPHash    string `json:"dhchap_hash,omitempty"`
	DHCHAPDHGroup string `json:"dhchap_dhgroup,omitempty"`
}

// NVMeHostCreateOptions specifies options for creating a host. dhchap_ctrl_key
// (mutual auth) requires dhchap_key.
type NVMeHostCreateOptions struct {
	HostNQN       string `json:"hostnqn"`
	DHCHAPKey     string `json:"dhchap_key,omitempty"`
	DHCHAPCtrlKey string `json:"dhchap_ctrl_key,omitempty"`
	DHCHAPHash    string `json:"dhchap_hash,omitempty"`
	DHCHAPDHGroup string `json:"dhchap_dhgroup,omitempty"`
}

// NVMeHostSubsys authorizes a host to access a subsystem (nvmet.host_subsys).
type NVMeHostSubsys struct {
	ID     int            `json:"id"`
	Host   *NVMeHost      `json:"host,omitempty"`
	Subsys *NVMeSubsystem `json:"subsys,omitempty"`
}

// NVMeHostSubsysCreateOptions specifies options for authorizing a host on a subsystem.
type NVMeHostSubsysCreateOptions struct {
	HostID   int `json:"host_id"`
	SubsysID int `json:"subsys_id"`
}

// GetNVMeGlobalConfig returns the nvmet global configuration.
func (c *Client) GetNVMeGlobalConfig(ctx context.Context) (*NVMeGlobalConfig, error) {
	var cfg NVMeGlobalConfig
	if err := c.Call(ctx, methodNVMetGlobalConfig, []any{}, &cfg); err != nil {
		return nil, fmt.Errorf("failed to get nvmet global config: %w", err)
	}
	return &cfg, nil
}

// CreateNVMeSubsystem creates a subsystem. subnqn is generated by TrueNAS from the
// global basenqn and returned in the result. Set allowAnyHost=false when DH-CHAP /
// host ACLs are used.
func (c *Client) CreateNVMeSubsystem(ctx context.Context, name string, allowAnyHost bool) (*NVMeSubsystem, error) {
	params := &NVMeSubsysCreateOptions{Name: name, AllowAnyHost: allowAnyHost}
	var subsys NVMeSubsystem
	if err := c.Call(ctx, methodNVMetSubsysCreate, []any{params}, &subsys); err != nil {
		return nil, fmt.Errorf("failed to create nvmet subsystem: %w", err)
	}
	return &subsys, nil
}

// GetNVMeSubsystemByNQN retrieves a subsystem by its generated subnqn.
func (c *Client) GetNVMeSubsystemByNQN(ctx context.Context, nqn string) (*NVMeSubsystem, error) {
	filters := [][]any{{"subnqn", "=", nqn}}
	var subsystems []NVMeSubsystem
	if err := c.Call(ctx, methodNVMetSubsysQuery, []any{filters, &QueryOptions{}}, &subsystems); err != nil {
		return nil, fmt.Errorf("failed to query nvmet subsystem by nqn: %w", err)
	}
	if len(subsystems) == 0 {
		return nil, ErrNotFound
	}
	return &subsystems[0], nil
}

// GetNVMeSubsystemByName retrieves a subsystem by its name.
func (c *Client) GetNVMeSubsystemByName(ctx context.Context, name string) (*NVMeSubsystem, error) {
	filters := [][]any{{"name", "=", name}}
	var subsystems []NVMeSubsystem
	if err := c.Call(ctx, methodNVMetSubsysQuery, []any{filters, &QueryOptions{}}, &subsystems); err != nil {
		return nil, fmt.Errorf("failed to query nvmet subsystem by name: %w", err)
	}
	if len(subsystems) == 0 {
		return nil, ErrNotFound
	}
	return &subsystems[0], nil
}

// SetNVMeSubsystemAllowAnyHost toggles allow_any_host on a subsystem.
func (c *Client) SetNVMeSubsystemAllowAnyHost(ctx context.Context, id int, allow bool) error {
	updates := map[string]any{"allow_any_host": allow}
	if err := c.Call(ctx, methodNVMetSubsysUpdate, []any{id, updates}, nil); err != nil {
		return fmt.Errorf("failed to update nvmet subsystem %d: %w", id, err)
	}
	return nil
}

// DeleteNVMeSubsystem deletes a subsystem by ID. Delete its namespaces and
// associations first.
func (c *Client) DeleteNVMeSubsystem(ctx context.Context, id int) error {
	if err := c.Call(ctx, methodNVMetSubsysDelete, []any{id}, nil); err != nil {
		return fmt.Errorf("failed to delete nvmet subsystem %d: %w", id, err)
	}
	return nil
}

// CreateNVMeNamespace creates a namespace backing devicePath (e.g. "zvol/pool/vol")
// in the given subsystem.
func (c *Client) CreateNVMeNamespace(ctx context.Context, subsysID int, deviceType, devicePath string) (*NVMeNamespace, error) {
	params := &NVMeNamespaceCreateOptions{
		SubsysID:   subsysID,
		DeviceType: deviceType,
		DevicePath: devicePath,
		Enabled:    true,
	}
	var ns NVMeNamespace
	if err := c.Call(ctx, methodNVMetNamespaceCreate, []any{params}, &ns); err != nil {
		return nil, fmt.Errorf("failed to create nvmet namespace: %w", err)
	}
	return &ns, nil
}

// GetNVMeNamespaceByDevice retrieves a namespace by its device path. The result's
// nested Subsys carries the subnqn.
func (c *Client) GetNVMeNamespaceByDevice(ctx context.Context, devicePath string) (*NVMeNamespace, error) {
	filters := [][]any{{"device_path", "=", devicePath}}
	var namespaces []NVMeNamespace
	if err := c.Call(ctx, methodNVMetNamespaceQuery, []any{filters, &QueryOptions{}}, &namespaces); err != nil {
		return nil, fmt.Errorf("failed to query nvmet namespace by device: %w", err)
	}
	if len(namespaces) == 0 {
		return nil, ErrNotFound
	}
	return &namespaces[0], nil
}

// GetNVMeNamespacesBySubsystem returns all namespaces belonging to a subsystem.
func (c *Client) GetNVMeNamespacesBySubsystem(ctx context.Context, subsysID int) ([]NVMeNamespace, error) {
	var all []NVMeNamespace
	if err := c.Call(ctx, methodNVMetNamespaceQuery, []any{[][]any{}, &QueryOptions{}}, &all); err != nil {
		return nil, fmt.Errorf("failed to query nvmet namespaces: %w", err)
	}
	var matched []NVMeNamespace
	for _, ns := range all {
		if ns.Subsys != nil && ns.Subsys.ID == subsysID {
			matched = append(matched, ns)
		}
	}
	return matched, nil
}

// DeleteNVMeNamespace deletes a namespace by ID.
func (c *Client) DeleteNVMeNamespace(ctx context.Context, id int) error {
	if err := c.Call(ctx, methodNVMetNamespaceDelete, []any{id}, nil); err != nil {
		return fmt.Errorf("failed to delete nvmet namespace %d: %w", id, err)
	}
	return nil
}

// QueryNVMePorts returns all configured nvmet ports.
func (c *Client) QueryNVMePorts(ctx context.Context) ([]NVMePort, error) {
	var ports []NVMePort
	if err := c.Call(ctx, methodNVMetPortQuery, []any{[][]any{}, &QueryOptions{}}, &ports); err != nil {
		return nil, fmt.Errorf("failed to query nvmet ports: %w", err)
	}
	return ports, nil
}

// CreateNVMePort creates a TCP transport port listening on addr:svcid.
func (c *Client) CreateNVMePort(ctx context.Context, addr string, svcID int) (*NVMePort, error) {
	params := &NVMePortCreateOptions{
		AddrTrType:  NVMeTransportTCP,
		AddrTrAddr:  addr,
		AddrTrSvcID: svcID,
	}
	var port NVMePort
	if err := c.Call(ctx, methodNVMetPortCreate, []any{params}, &port); err != nil {
		return nil, fmt.Errorf("failed to create nvmet port: %w", err)
	}
	return &port, nil
}

// GetNVMePortByAddr returns the first TCP port matching the given listen address.
func (c *Client) GetNVMePortByAddr(ctx context.Context, addr string) (*NVMePort, error) {
	filters := [][]any{{"addr_traddr", "=", addr}}
	var ports []NVMePort
	if err := c.Call(ctx, methodNVMetPortQuery, []any{filters, &QueryOptions{}}, &ports); err != nil {
		return nil, fmt.Errorf("failed to query nvmet port by addr: %w", err)
	}
	for i := range ports {
		if ports[i].AddrTrType == NVMeTransportTCP {
			return &ports[i], nil
		}
	}
	return nil, ErrNotFound
}

// CreateNVMePortSubsys links a port to a subsystem.
func (c *Client) CreateNVMePortSubsys(ctx context.Context, portID, subsysID int) (*NVMePortSubsys, error) {
	params := &NVMePortSubsysCreateOptions{PortID: portID, SubsysID: subsysID}
	var ps NVMePortSubsys
	if err := c.Call(ctx, methodNVMetPortSubsysCreate, []any{params}, &ps); err != nil {
		return nil, fmt.Errorf("failed to create nvmet port-subsys link: %w", err)
	}
	return &ps, nil
}

// GetNVMePortSubsysBySubsystem returns the port links for a subsystem.
func (c *Client) GetNVMePortSubsysBySubsystem(ctx context.Context, subsysID int) ([]NVMePortSubsys, error) {
	var all []NVMePortSubsys
	if err := c.Call(ctx, methodNVMetPortSubsysQuery, []any{[][]any{}, &QueryOptions{}}, &all); err != nil {
		return nil, fmt.Errorf("failed to query nvmet port-subsys links: %w", err)
	}
	var matched []NVMePortSubsys
	for _, ps := range all {
		if ps.Subsys != nil && ps.Subsys.ID == subsysID {
			matched = append(matched, ps)
		}
	}
	return matched, nil
}

// DeleteNVMePortSubsys deletes a port-subsystem link by ID.
func (c *Client) DeleteNVMePortSubsys(ctx context.Context, id int) error {
	if err := c.Call(ctx, methodNVMetPortSubsysDelete, []any{id}, nil); err != nil {
		return fmt.Errorf("failed to delete nvmet port-subsys link %d: %w", id, err)
	}
	return nil
}

// CreateNVMeHost creates an authorized host (with optional DH-CHAP credentials).
func (c *Client) CreateNVMeHost(ctx context.Context, opts *NVMeHostCreateOptions) (*NVMeHost, error) {
	var host NVMeHost
	if err := c.Call(ctx, methodNVMetHostCreate, []any{opts}, &host); err != nil {
		return nil, fmt.Errorf("failed to create nvmet host: %w", err)
	}
	return &host, nil
}

// GetNVMeHostByNQN retrieves a host by its host NQN.
func (c *Client) GetNVMeHostByNQN(ctx context.Context, hostNQN string) (*NVMeHost, error) {
	filters := [][]any{{"hostnqn", "=", hostNQN}}
	var hosts []NVMeHost
	if err := c.Call(ctx, methodNVMetHostQuery, []any{filters, &QueryOptions{}}, &hosts); err != nil {
		return nil, fmt.Errorf("failed to query nvmet host by nqn: %w", err)
	}
	if len(hosts) == 0 {
		return nil, ErrNotFound
	}
	return &hosts[0], nil
}

// DeleteNVMeHost deletes a host by ID.
func (c *Client) DeleteNVMeHost(ctx context.Context, id int) error {
	if err := c.Call(ctx, methodNVMetHostDelete, []any{id}, nil); err != nil {
		return fmt.Errorf("failed to delete nvmet host %d: %w", id, err)
	}
	return nil
}

// CreateNVMeHostSubsys authorizes a host to access a subsystem.
func (c *Client) CreateNVMeHostSubsys(ctx context.Context, hostID, subsysID int) (*NVMeHostSubsys, error) {
	params := &NVMeHostSubsysCreateOptions{HostID: hostID, SubsysID: subsysID}
	var hs NVMeHostSubsys
	if err := c.Call(ctx, methodNVMetHostSubsysCreate, []any{params}, &hs); err != nil {
		return nil, fmt.Errorf("failed to create nvmet host-subsys link: %w", err)
	}
	return &hs, nil
}

// GetNVMeHostSubsysByHost returns the subsystem links for a host. Used to decide
// whether a shared host can be garbage-collected.
func (c *Client) GetNVMeHostSubsysByHost(ctx context.Context, hostID int) ([]NVMeHostSubsys, error) {
	var all []NVMeHostSubsys
	if err := c.Call(ctx, methodNVMetHostSubsysQuery, []any{[][]any{}, &QueryOptions{}}, &all); err != nil {
		return nil, fmt.Errorf("failed to query nvmet host-subsys links: %w", err)
	}
	var matched []NVMeHostSubsys
	for _, hs := range all {
		if hs.Host != nil && hs.Host.ID == hostID {
			matched = append(matched, hs)
		}
	}
	return matched, nil
}

// GetNVMeHostSubsysBySubsystem returns the host links for a subsystem.
func (c *Client) GetNVMeHostSubsysBySubsystem(ctx context.Context, subsysID int) ([]NVMeHostSubsys, error) {
	var all []NVMeHostSubsys
	if err := c.Call(ctx, methodNVMetHostSubsysQuery, []any{[][]any{}, &QueryOptions{}}, &all); err != nil {
		return nil, fmt.Errorf("failed to query nvmet host-subsys links: %w", err)
	}
	var matched []NVMeHostSubsys
	for _, hs := range all {
		if hs.Subsys != nil && hs.Subsys.ID == subsysID {
			matched = append(matched, hs)
		}
	}
	return matched, nil
}

// DeleteNVMeHostSubsys deletes a host-subsystem link by ID.
func (c *Client) DeleteNVMeHostSubsys(ctx context.Context, id int) error {
	if err := c.Call(ctx, methodNVMetHostSubsysDelete, []any{id}, nil); err != nil {
		return fmt.Errorf("failed to delete nvmet host-subsys link %d: %w", id, err)
	}
	return nil
}
