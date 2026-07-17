package driver

import "testing"

func TestEnsureIPv4Portal(t *testing.T) {
	tests := []struct {
		name    string
		portal  string
		wantErr bool
	}{
		{name: "IPv4 with port", portal: "192.168.10.126:3260", wantErr: false},
		{name: "IPv4 without port", portal: "192.168.10.126", wantErr: false},
		{name: "hostname with port", portal: "truenas.example.com:3260", wantErr: false},
		{name: "hostname without port", portal: "truenas.example.com", wantErr: false},
		{name: "IPv6 bracketed with port", portal: "[fd97:abcd::1000]:3260", wantErr: true},
		{name: "IPv6 bracketed without port", portal: "[fd97:abcd::1000]", wantErr: true},
		{name: "IPv6 bare", portal: "fd97:abcd::1000", wantErr: true},
		{name: "IPv6 loopback", portal: "[::1]:3260", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ensureIPv4Portal(tt.portal)
			if tt.wantErr && err == nil {
				t.Errorf("ensureIPv4Portal(%q) = nil, want error", tt.portal)
			}
			if !tt.wantErr && err != nil {
				t.Errorf("ensureIPv4Portal(%q) = %v, want nil", tt.portal, err)
			}
		})
	}
}
