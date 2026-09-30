package driver

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
	"github.com/go-logr/logr"

	"github.com/truenas/truenas-csi/pkg/client"
)

// fakeTrueNAS is a stateful stand-in for the TrueNAS JSON-RPC API, enough of it
// for the iSCSI controller paths: each "<collection>.create|query|update|delete"
// method works on an in-memory collection of records, and query understands the
// [[field, "=", value]] filters the client sends.
type fakeTrueNAS struct {
	t      *testing.T
	server *httptest.Server

	mu      sync.Mutex
	nextID  float64
	records map[string][]map[string]any
	calls   []fakeCall
	// fail makes the named method return an error, to interrupt a CreateVolume.
	fail map[string]bool
}

type fakeCall struct {
	method string
	params []any
}

type fakeRPCRequest struct {
	ID     uint64 `json:"id"`
	Method string `json:"method"`
	Params []any  `json:"params"`
}

type fakeRPCResponse struct {
	ID      uint64           `json:"id"`
	JSONRPC string           `json:"jsonrpc"`
	Result  any              `json:"result"`
	Error   *client.RPCError `json:"error,omitempty"`
}

func newFakeTrueNAS(t *testing.T) *fakeTrueNAS {
	t.Helper()
	f := &fakeTrueNAS{t: t, nextID: 1, records: map[string][]map[string]any{}, fail: map[string]bool{}}
	f.server = httptest.NewServer(http.HandlerFunc(f.serve))
	t.Cleanup(f.server.Close)
	return f
}

// controller returns a ControllerServer whose driver talks to the fake.
func (f *fakeTrueNAS) controller() *ControllerServer {
	f.t.Helper()
	c := client.New(client.Config{
		URL:    "ws" + strings.TrimPrefix(f.server.URL, "http"),
		APIKey: "test-api-key",
		Logger: logr.Discard(),
	})
	if err := c.Connect(context.Background()); err != nil {
		f.t.Fatalf("failed to connect to the fake TrueNAS: %v", err)
	}
	f.t.Cleanup(func() { _ = c.Close() })

	return &ControllerServer{driver: &Driver{
		client:        c,
		log:           logr.Discard(),
		iscsiPortal:   "10.0.0.1:3260",
		iscsiPortalID: 1,
		iscsiBasename: "iqn.2005-10.org.freenas.ctl",
	}}
}

func (f *fakeTrueNAS) serve(w http.ResponseWriter, r *http.Request) {
	if strings.HasSuffix(r.URL.Path, "/api/versions") {
		_ = json.NewEncoder(w).Encode([]string{client.MinAPIVersion})
		return
	}
	conn, err := websocket.Accept(w, r, nil)
	if err != nil {
		return
	}
	defer conn.Close(websocket.StatusNormalClosure, "")
	for {
		var req fakeRPCRequest
		if err := wsjson.Read(r.Context(), conn, &req); err != nil {
			return
		}
		result, rpcErr := f.handle(req.Method, req.Params)
		if err := wsjson.Write(r.Context(), conn, fakeRPCResponse{ID: req.ID, JSONRPC: "2.0", Result: result, Error: rpcErr}); err != nil {
			return
		}
	}
}

func (f *fakeTrueNAS) handle(method string, params []any) (any, *client.RPCError) {
	switch method {
	case "auth.login_with_api_key":
		return true, nil
	case "core.ping":
		return "pong", nil
	}

	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, fakeCall{method: method, params: params})
	if f.fail[method] {
		return nil, &client.RPCError{Code: -32001, Message: "injected failure for " + method}
	}

	if result, rpcErr, handled := f.handleSnapshot(method, params); handled {
		return result, rpcErr
	}

	dot := strings.LastIndex(method, ".")
	collection, op := method[:dot], method[dot+1:]
	switch op {
	case "create":
		record, _ := params[0].(map[string]any)
		if collection == "iscsi.auth" && record["discovery_auth"] == client.ISCSIAuthMethodCHAPMutual {
			for _, auth := range f.records[collection] {
				if auth["discovery_auth"] == client.ISCSIAuthMethodCHAPMutual {
					return nil, &client.RPCError{Code: -32602, Message: "Cannot specify CHAP_MUTUAL as only one such entry is permitted."}
				}
			}
		}
		if collection == "pool.dataset" {
			record["id"] = record["name"]
		} else {
			record["id"] = f.nextID
			f.nextID++
		}
		f.records[collection] = append(f.records[collection], record)
		return record, nil
	case "get_instance":
		if record := f.find(collection, params[0]); record != nil {
			return record, nil
		}
		return nil, &client.RPCError{Code: -32001, Message: fmt.Sprintf("%v does not exist", params[0])}
	case "query":
		var filters []any
		if len(params) > 0 {
			filters, _ = params[0].([]any)
		}
		matched := []map[string]any{}
		for _, record := range f.records[collection] {
			if matches(record, filters) {
				matched = append(matched, record)
			}
		}
		return matched, nil
	case "update":
		record := f.find(collection, params[0])
		if record == nil {
			return nil, &client.RPCError{Code: -32001, Message: "no such record"}
		}
		for k, v := range params[1].(map[string]any) {
			record[k] = v
		}
		return record, nil
	case "delete":
		if collection == "iscsi.auth" {
			if auth := f.find(collection, params[0]); auth != nil && f.tagInUse(auth["tag"]) {
				return nil, &client.RPCError{Code: -32001, Message: "Authorized access is being used by a target"}
			}
		}
		f.remove(collection, params[0])
		return true, nil
	}
	return nil, nil
}

// handleSnapshot gives snapshots the ZFS rules TrueNAS enforces: an immediate
// delete of a snapshot that clones depend on is refused, a deferred one marks it,
// and deleting its last clone destroys it.
func (f *fakeTrueNAS) handleSnapshot(method string, params []any) (any, *client.RPCError, bool) {
	switch method {
	case "pool.snapshot.create":
		opts, _ := params[0].(map[string]any)
		snapshot := newFakeSnapshot(fmt.Sprint(opts["dataset"]), fmt.Sprint(opts["name"]))
		f.records["pool.snapshot"] = append(f.records["pool.snapshot"], snapshot)
		return snapshot, nil, true
	case "pool.snapshot.clone":
		opts, _ := params[0].(map[string]any)
		dst := fmt.Sprint(opts["dataset_dst"])
		f.records["pool.dataset"] = append(f.records["pool.dataset"], map[string]any{
			"id": dst, "name": dst, "type": datasetTypeVolume, "origin": opts["snapshot"],
		})
		return true, nil, true
	case "pool.snapshot.delete":
		id := fmt.Sprint(params[0])
		opts, _ := params[1].(map[string]any)
		snapshot := f.find("pool.snapshot", id)
		if snapshot == nil {
			return nil, &client.RPCError{Code: -32001, Message: id + " does not exist"}, true
		}
		if clones := f.clonesOf(id); len(clones) > 0 {
			if opts["defer"] != true {
				return nil, &client.RPCError{Code: -32602, Message: fmt.Sprintf(
					"[EINVAL] options.defer: Please set this attribute as '%s' snapshot has dependent clones: %s", id, strings.Join(clones, ","))}, true
			}
			snapshot["properties"] = map[string]any{"defer_destroy": map[string]any{"value": "on"}}
			return true, nil, true
		}
		f.remove("pool.snapshot", id)
		return true, nil, true
	case "pool.dataset.delete":
		id := fmt.Sprint(params[0])
		f.remove("pool.dataset", id)
		// ZFS destroys a deferred snapshot once its last clone is gone, and a
		// dataset's own snapshots go with it.
		// Runs under f.mu, so it copies the records rather than calling f.all.
		for _, snapshot := range append([]map[string]any(nil), f.records["pool.snapshot"]...) {
			pending := snapshot["properties"].(map[string]any)["defer_destroy"].(map[string]any)["value"] == "on"
			if snapshot["dataset"] == id || (pending && len(f.clonesOf(fmt.Sprint(snapshot["id"]))) == 0) {
				f.remove("pool.snapshot", snapshot["id"])
			}
		}
		return true, nil, true
	}
	return nil, nil, false
}

func newFakeSnapshot(dataset, name string) map[string]any {
	return map[string]any{
		"id": dataset + "@" + name, "dataset": dataset, "name": dataset + "@" + name, "snapshot_name": name,
		"properties": map[string]any{"defer_destroy": map[string]any{"value": "off"}},
	}
}

// clonesOf returns the datasets cloned from snapshot.
func (f *fakeTrueNAS) clonesOf(snapshot string) []string {
	var clones []string
	for _, ds := range f.records["pool.dataset"] {
		if fmt.Sprint(ds["origin"]) == snapshot {
			clones = append(clones, fmt.Sprint(ds["id"]))
		}
	}
	return clones
}

func matches(record map[string]any, filters []any) bool {
	for _, raw := range filters {
		filter, _ := raw.([]any)
		if len(filter) != 3 {
			continue
		}
		value, want := fmt.Sprint(record[filter[0].(string)]), fmt.Sprint(filter[2])
		switch filter[1] {
		case "=":
			if value != want {
				return false
			}
		case "^":
			if !strings.HasPrefix(value, want) {
				return false
			}
		}
	}
	return true
}

func (f *fakeTrueNAS) find(collection string, id any) map[string]any {
	for _, record := range f.records[collection] {
		if fmt.Sprint(record["id"]) == fmt.Sprint(id) {
			return record
		}
	}
	return nil
}

func (f *fakeTrueNAS) remove(collection string, id any) {
	kept := f.records[collection][:0]
	for _, record := range f.records[collection] {
		if fmt.Sprint(record["id"]) != fmt.Sprint(id) {
			kept = append(kept, record)
		}
	}
	f.records[collection] = kept
}

func (f *fakeTrueNAS) tagInUse(tag any) bool {
	for _, target := range f.records["iscsi.target"] {
		for _, g := range groupsOf(target) {
			if fmt.Sprint(g["auth"]) == fmt.Sprint(tag) {
				return true
			}
		}
	}
	return false
}

func groupsOf(target map[string]any) []map[string]any {
	raw, _ := target["groups"].([]any)
	var groups []map[string]any
	for _, g := range raw {
		if group, ok := g.(map[string]any); ok {
			groups = append(groups, group)
		}
	}
	return groups
}

// seed stores a record as if something else had created it, and returns its id.
func (f *fakeTrueNAS) seed(collection string, record map[string]any) float64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	id := f.nextID
	f.nextID++
	record["id"] = id
	f.records[collection] = append(f.records[collection], record)
	return id
}

// all returns a snapshot of a collection.
func (f *fakeTrueNAS) all(collection string) []map[string]any {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]map[string]any(nil), f.records[collection]...)
}

// callsTo returns the params of every call to method.
func (f *fakeTrueNAS) callsTo(method string) [][]any {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out [][]any
	for _, c := range f.calls {
		if c.method == method {
			out = append(out, c.params)
		}
	}
	return out
}
