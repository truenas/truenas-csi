package client

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
	"github.com/go-logr/logr"
)

// Default configuration values
const (
	defaultCallTimeout         = 30 * time.Second
	defaultPingInterval        = 30 * time.Second
	defaultPingTimeout         = 10 * time.Second
	defaultDialTimeout         = 30 * time.Second
	defaultTLSHandshakeTimeout = 10 * time.Second
	defaultReconnectMin        = 1 * time.Second
	defaultReconnectMax        = 60 * time.Second
	defaultReconnectFactor     = 2.0
	jsonRPCVersion             = "2.0"

	// WebSocket read limit - TrueNAS can return large JSON responses for list operations
	// Default is 32KB which is too small for datasets/snapshots with many entries
	defaultReadLimit = 16 * 1024 * 1024 // 16 MB

	// TrueNAS RPC error codes
	rpcErrCodeNotFound       = -6 // ENOENT - resource not found
	rpcErrCodeConnectionLost = -1 // Internal error for connection loss

	// Logging verbosity levels (for logr.Logger.V())
	// V(0) - Always logged (critical errors, startup/shutdown)
	// V(1) - General operational info (connection events, reconnection)
	// V(2) - Detailed diagnostics (ping success, dropped responses)
	logLevelInfo  = 1
	logLevelDebug = 2
)

// Sentinel errors
var (
	ErrNotConnected = errors.New("truenas: not connected")
	ErrAuthFailed   = errors.New("truenas: authentication failed")
	ErrClosed       = errors.New("truenas: client closed")
	ErrNotFound     = errors.New("truenas: resource not found")
)

// Config holds configuration for the TrueNAS client.
type Config struct {
	URL                string
	APIKey             string
	TLSConfig          *tls.Config
	InsecureSkipVerify bool
	CallTimeout        time.Duration
	PingInterval       time.Duration
	ReconnectMin       time.Duration
	ReconnectMax       time.Duration
	ReconnectFactor    float64
	// MaxReconnectAttempts limits reconnection attempts. 0 means unlimited.
	MaxReconnectAttempts int
	// Logger is an optional structured logger. If not provided, logging is disabled.
	Logger logr.Logger
}

// ConnectionError wraps connection-related errors.
type ConnectionError struct {
	Op  string // "dial", "read", "write"
	Err error
}

func (e *ConnectionError) Error() string {
	return fmt.Sprintf("truenas: %s: %v", e.Op, e.Err)
}

func (e *ConnectionError) Unwrap() error {
	return e.Err
}

// IsConnectionError reports whether err is a connection-related error.
func IsConnectionError(err error) bool {
	var connErr *ConnectionError
	return errors.As(err, &connErr)
}

// RPCError represents a JSON-RPC error from TrueNAS.
type RPCError struct {
	Code    int             `json:"code"`
	Message string          `json:"message"`
	Data    json.RawMessage `json:"data,omitempty"`
}

func (e *RPCError) Error() string {
	if len(e.Data) > 0 {
		return fmt.Sprintf("truenas: rpc error %d: %s (data: %s)", e.Code, e.Message, e.Data)
	}
	return fmt.Sprintf("truenas: rpc error %d: %s", e.Code, e.Message)
}

// IsNotFoundError checks if an error indicates a resource was not found.
// TrueNAS returns validation errors for get_instance when resource doesn't exist.
func IsNotFoundError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, ErrNotFound) {
		return true
	}
	var rpcErr *RPCError
	if errors.As(err, &rpcErr) {
		// TrueNAS returns ENOENT or validation errors for not found
		if rpcErr.Code == rpcErrCodeNotFound {
			return true
		}
		// Check for validation error messages indicating not found
		msg := strings.ToLower(rpcErr.Message)
		if strings.Contains(msg, "not found") ||
			strings.Contains(msg, "does not exist") ||
			strings.Contains(msg, "no such") {
			return true
		}
		// Also check the Data field - TrueNAS often puts error details there
		if len(rpcErr.Data) > 0 {
			data := strings.ToLower(string(rpcErr.Data))
			if strings.Contains(data, "not found") ||
				strings.Contains(data, "does not exist") ||
				strings.Contains(data, "instancenotfound") ||
				strings.Contains(data, "enoent") {
				return true
			}
		}
	}
	return false
}

// request represents a JSON-RPC request.
type request struct {
	ID      uint64 `json:"id"`
	Method  string `json:"method"`
	Params  any    `json:"params,omitempty"`
	JSONRPC string `json:"jsonrpc"`
}

// response represents a JSON-RPC response.
type response struct {
	ID      uint64          `json:"id"`
	Result  json.RawMessage `json:"result,omitempty"`
	Error   *RPCError       `json:"error,omitempty"`
	JSONRPC string          `json:"jsonrpc"`
}

// Client is a TrueNAS WebSocket API client with automatic reconnection.
type Client struct {
	config Config
	log    logr.Logger

	// Connection state (protected by connMu)
	connMu   sync.RWMutex
	conn     *websocket.Conn
	connDone chan struct{} // closed when current connection should stop

	// Request tracking
	nextID  atomic.Uint64
	pending sync.Map // map[uint64]chan response

	// Client lifecycle
	done   chan struct{} // closed on Close()
	closed atomic.Bool

	// Reconnection guard
	reconnecting atomic.Bool
}

// New creates a new TrueNAS client with the given configuration.
func New(cfg Config) *Client {
	// Apply defaults for zero values
	if cfg.CallTimeout == 0 {
		cfg.CallTimeout = defaultCallTimeout
	}
	if cfg.PingInterval == 0 {
		cfg.PingInterval = defaultPingInterval
	}
	if cfg.ReconnectMin == 0 {
		cfg.ReconnectMin = defaultReconnectMin
	}
	if cfg.ReconnectMax == 0 {
		cfg.ReconnectMax = defaultReconnectMax
	}
	if cfg.ReconnectFactor == 0 {
		cfg.ReconnectFactor = defaultReconnectFactor
	}
	if cfg.TLSConfig == nil && cfg.InsecureSkipVerify {
		cfg.TLSConfig = &tls.Config{InsecureSkipVerify: true}
	}

	// Use discard logger if none provided
	log := cfg.Logger
	if log.GetSink() == nil {
		log = logr.Discard()
	}

	return &Client{
		config: cfg,
		log:    log,
		done:   make(chan struct{}),
	}
}

// Connect establishes the initial connection to TrueNAS.
// It is safe to call multiple times; subsequent calls return nil if already connected.
func (c *Client) Connect(ctx context.Context) error {
	if c.Connected() {
		return nil
	}
	return c.dial(ctx)
}

// dial establishes a WebSocket connection to TrueNAS and authenticates.
func (c *Client) dial(ctx context.Context) error {
	if c.closed.Load() {
		return ErrClosed
	}

	// Add timeout if context has no deadline
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, defaultDialTimeout)
		defer cancel()
	}

	c.log.Info("Connecting to TrueNAS", "url", c.config.URL, "timeout", defaultDialTimeout)

	conn, _, err := websocket.Dial(ctx, c.config.URL, &websocket.DialOptions{
		HTTPClient: &http.Client{
			Transport: &http.Transport{
				TLSClientConfig:     c.config.TLSConfig,
				TLSHandshakeTimeout: defaultTLSHandshakeTimeout,
			},
		},
	})
	if err != nil {
		c.log.Error(err, "Failed to connect to TrueNAS")
		return &ConnectionError{Op: "dial", Err: err}
	}

	// Set read limit for large JSON responses (dataset/snapshot lists can be large)
	conn.SetReadLimit(defaultReadLimit)

	c.log.V(logLevelInfo).Info("WebSocket connected, authenticating")

	// Authenticate before storing connection (direct read/write, no readLoop yet)
	authCtx, authCancel := context.WithTimeout(ctx, c.config.CallTimeout)
	defer authCancel()

	authReq := request{
		ID:      c.nextID.Add(1),
		Method:  "auth.login_with_api_key",
		Params:  []string{c.config.APIKey},
		JSONRPC: jsonRPCVersion,
	}

	if err = wsjson.Write(authCtx, conn, authReq); err != nil {
		conn.Close(websocket.StatusNormalClosure, "")
		c.log.Error(err, "TrueNAS auth write error")
		return &ConnectionError{Op: "write", Err: err}
	}

	var authResp response
	if err = wsjson.Read(authCtx, conn, &authResp); err != nil {
		conn.Close(websocket.StatusNormalClosure, "")
		c.log.Error(err, "TrueNAS auth read error")
		return &ConnectionError{Op: "read", Err: err}
	}

	if authResp.Error != nil {
		conn.Close(websocket.StatusNormalClosure, "")
		c.log.Error(nil, "TrueNAS authentication error", "error", authResp.Error)
		return authResp.Error
	}

	var ok bool
	if err = json.Unmarshal(authResp.Result, &ok); err != nil || !ok {
		conn.Close(websocket.StatusNormalClosure, "")
		c.log.Error(nil, "TrueNAS authentication rejected")
		return ErrAuthFailed
	}

	connDone := make(chan struct{})

	c.connMu.Lock()
	c.conn = conn
	c.connDone = connDone
	c.connMu.Unlock()

	go c.readLoop(conn, connDone)
	go c.pingLoop(conn, connDone)

	c.log.Info("Connected to TrueNAS")
	return nil
}

// readLoop reads responses from the WebSocket and dispatches to waiting callers.
func (c *Client) readLoop(conn *websocket.Conn, done chan struct{}) {
	defer func() {
		if r := recover(); r != nil {
			c.log.V(logLevelInfo).Info("Recovered from panic in readLoop", "panic", r)
		}
	}()

	for {
		select {
		case <-done:
			return
		default:
		}

		var resp response
		if err := wsjson.Read(context.Background(), conn, &resp); err != nil {
			select {
			case <-done:
				return // Clean shutdown
			default:
				c.log.V(logLevelInfo).Info("TrueNAS read error - connection lost", "error", err)
				c.handleDisconnect(conn)
				return
			}
		}

		if ch, ok := c.pending.LoadAndDelete(resp.ID); ok {
			select {
			case ch.(chan response) <- resp:
			default:
				c.log.V(logLevelDebug).Info("Dropped response for request, no receiver", "requestId", resp.ID)
			}
		}
	}
}

// pingLoop sends periodic pings to keep the connection alive.
func (c *Client) pingLoop(conn *websocket.Conn, done chan struct{}) {
	defer func() {
		if r := recover(); r != nil {
			c.log.V(logLevelInfo).Info("Recovered from panic in pingLoop", "panic", r)
		}
	}()

	ticker := time.NewTicker(c.config.PingInterval)
	defer ticker.Stop()

	for {
		select {
		case <-done:
			return
		case <-ticker.C:
			pingCtx, cancel := context.WithTimeout(context.Background(), defaultPingTimeout)
			err := conn.Ping(pingCtx)
			cancel()

			if err != nil {
				select {
				case <-done:
					return
				default:
					c.log.V(logLevelInfo).Info("TrueNAS ping failed - connection lost", "error", err)
					c.handleDisconnect(conn)
					return
				}
			} else {
				c.log.V(logLevelDebug).Info("TrueNAS ping successful")
			}
		}
	}
}

// handleDisconnect closes the connection and initiates reconnection.
func (c *Client) handleDisconnect(conn *websocket.Conn) {
	c.connMu.Lock()
	// Check if this is still the active connection
	if c.conn != conn {
		c.connMu.Unlock()
		return
	}

	// Signal connection goroutines to stop
	if c.connDone != nil {
		close(c.connDone)
		c.connDone = nil
	}
	c.conn = nil
	c.connMu.Unlock()

	conn.Close(websocket.StatusNormalClosure, "")

	// Fail pending requests
	c.pending.Range(func(key, value any) bool {
		select {
		case value.(chan response) <- response{Error: &RPCError{Code: rpcErrCodeConnectionLost, Message: "connection lost"}}:
		default:
		}
		c.pending.Delete(key)
		return true
	})

	c.log.Info("Disconnected from TrueNAS")

	// Start reconnection if not already reconnecting and not closed
	if !c.closed.Load() && c.reconnecting.CompareAndSwap(false, true) {
		go c.reconnectLoop()
	}
}

// reconnectLoop attempts to reconnect with exponential backoff.
func (c *Client) reconnectLoop() {
	defer func() {
		if r := recover(); r != nil {
			c.log.V(logLevelInfo).Info("Recovered from panic in reconnectLoop", "panic", r)
		}
		c.reconnecting.Store(false)
	}()

	delay := c.config.ReconnectMin
	attempt := 0
	maxAttempts := c.config.MaxReconnectAttempts

	for {
		select {
		case <-c.done:
			return
		case <-time.After(delay):
		}

		if c.closed.Load() || c.Connected() {
			return
		}

		attempt++

		// Check max attempts (0 means unlimited)
		if maxAttempts > 0 && attempt > maxAttempts {
			c.log.Error(nil, "TrueNAS reconnection failed, giving up", "attempts", maxAttempts)
			return
		}

		if maxAttempts > 0 {
			c.log.V(logLevelInfo).Info("TrueNAS reconnect attempt", "attempt", attempt, "maxAttempts", maxAttempts)
		} else {
			c.log.V(logLevelInfo).Info("TrueNAS reconnect attempt", "attempt", attempt)
		}

		dialCtx, cancel := context.WithTimeout(context.Background(), defaultDialTimeout)
		err := c.dial(dialCtx)
		cancel()

		if err == nil {
			c.log.Info("Reconnected to TrueNAS", "attempts", attempt)
			return
		}

		c.log.V(logLevelInfo).Info("TrueNAS reconnect failed", "error", err)

		// Exponential backoff
		delay = time.Duration(float64(delay) * c.config.ReconnectFactor)
		delay = min(delay, c.config.ReconnectMax)
	}
}

// Connected reports whether the client has an active connection.
func (c *Client) Connected() bool {
	c.connMu.RLock()
	defer c.connMu.RUnlock()
	return c.conn != nil
}

// Call invokes a JSON-RPC method. Returns ErrNotConnected if disconnected.
func (c *Client) Call(ctx context.Context, method string, params, result any) error {
	c.connMu.RLock()
	conn := c.conn
	c.connMu.RUnlock()

	if conn == nil {
		return ErrNotConnected
	}
	return c.callOn(ctx, conn, method, params, result)
}

// callOn sends a JSON-RPC request and waits for the response.
func (c *Client) callOn(ctx context.Context, conn *websocket.Conn, method string, params, result any) error {
	// Apply default timeout if context has no deadline
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, c.config.CallTimeout)
		defer cancel()
	}

	id := c.nextID.Add(1)
	req := request{
		ID:      id,
		Method:  method,
		Params:  params,
		JSONRPC: jsonRPCVersion,
	}

	respCh := make(chan response, 1)
	c.pending.Store(id, respCh)
	defer c.pending.Delete(id)

	if err := wsjson.Write(ctx, conn, req); err != nil {
		return &ConnectionError{Op: "write", Err: err}
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case resp := <-respCh:
		if resp.Error != nil {
			return resp.Error
		}
		if result != nil && len(resp.Result) > 0 {
			if err := json.Unmarshal(resp.Result, result); err != nil {
				return fmt.Errorf("unmarshal result: %w", err)
			}
		}
		return nil
	}
}

// Close closes the client permanently.
func (c *Client) Close() error {
	if c.closed.Swap(true) {
		return nil // Already closed
	}

	c.log.V(logLevelInfo).Info("Closing TrueNAS client")

	close(c.done)

	c.connMu.Lock()
	conn := c.conn
	if c.connDone != nil {
		close(c.connDone)
	}
	c.conn = nil
	c.connMu.Unlock()

	if conn != nil {
		return conn.Close(websocket.StatusNormalClosure, "")
	}
	return nil
}

// Ping checks if the server is responsive by calling core.ping.
func (c *Client) Ping(ctx context.Context) error {
	return c.Call(ctx, "core.ping", nil, nil)
}
