package driver

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"
)

// stubLookupHost makes host names resolve from table for the rest of the test.
func stubLookupHost(t *testing.T, table map[string][]string) {
	t.Helper()
	orig := lookupHost
	lookupHost = func(_ context.Context, host string) ([]string, error) {
		if addrs, ok := table[host]; ok {
			return addrs, nil
		}
		return nil, errors.New("no such host")
	}
	t.Cleanup(func() { lookupHost = orig })
}

func TestPortalAddresses(t *testing.T) {
	stubLookupHost(t, map[string][]string{"nas.test": {"10.0.0.1", "10.0.0.2"}})

	tests := []struct {
		host string
		want []string
	}{
		{"10.0.0.1", []string{"10.0.0.1"}},
		{"nas.test", []string{"nas.test", "10.0.0.1", "10.0.0.2"}},
		// Still matches a portal that listens on all addresses.
		{"unknown.test", []string{"unknown.test"}},
	}
	for _, tt := range tests {
		if got := portalAddresses(context.Background(), tt.host); !slices.Equal(got, tt.want) {
			t.Errorf("portalAddresses(%q) = %v, want %v", tt.host, got, tt.want)
		}
	}
}

// TrueNAS lists a portal's listen addresses as IPs. A portal bound to one address,
// rather than all of them, could never match a portal given as a host name.
func TestISCSIPortalID_HostNameAndBoundPortal(t *testing.T) {
	stubLookupHost(t, map[string][]string{"nas.test": {"10.0.0.2"}})
	f := newFakeTrueNAS(t)
	f.seed("iscsi.portal", map[string]any{"listen": []any{map[string]any{"ip": "10.0.0.9", "port": float64(3260)}}})
	bound := f.seed("iscsi.portal", map[string]any{"listen": []any{map[string]any{"ip": "10.0.0.2", "port": float64(3260)}}})

	d := f.controller().driver
	d.iscsiPortalID = 0
	d.iscsiPortal = "nas.test:3260"

	id, err := d.ISCSIPortalID(context.Background())
	if err != nil {
		t.Fatalf("ISCSIPortalID() = %v", err)
	}
	if id != int(bound) {
		t.Errorf("ISCSIPortalID() = %d, want the portal bound to the address nas.test resolves to (%v)", id, bound)
	}
}

func TestISCSIPortalID_NoMatchNamesTheAddresses(t *testing.T) {
	stubLookupHost(t, map[string][]string{"nas.test": {"10.0.0.2"}})
	f := newFakeTrueNAS(t)
	f.seed("iscsi.portal", map[string]any{"listen": []any{map[string]any{"ip": "10.0.0.9", "port": float64(3260)}}})

	d := f.controller().driver
	d.iscsiPortalID = 0
	d.iscsiPortal = "nas.test:3260"

	_, err := d.ISCSIPortalID(context.Background())
	if err == nil || !strings.Contains(err.Error(), "nas.test, 10.0.0.2") {
		t.Errorf("ISCSIPortalID() = %v, want an error listing the name and its address", err)
	}
}
