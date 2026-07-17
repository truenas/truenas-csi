package driver

import (
	"testing"

	"github.com/truenas/truenas-csi/pkg/client"
)

func derefOr(p *string, fallback string) string {
	if p == nil {
		return fallback
	}
	return *p
}

func TestApplyNFSShareParameters(t *testing.T) {
	const unset = "<nil>"

	tests := []struct {
		name         string
		params       map[string]string
		wantMapAllU  string
		wantMapAllG  string
		wantMapRootU string
		wantMapRootG string
		wantHosts    []string
		wantNetworks []string
	}{
		{
			name:         "default squashes all to root:wheel via mapall",
			params:       map[string]string{},
			wantMapAllU:  "root",
			wantMapAllG:  "wheel",
			wantMapRootU: unset,
			wantMapRootG: unset,
		},
		{
			name:         "custom mapall user/group",
			params:       map[string]string{paramNFSMapAllUser: "postgres", paramNFSMapAllGroup: "postgres"},
			wantMapAllU:  "postgres",
			wantMapAllG:  "postgres",
			wantMapRootU: unset,
			wantMapRootG: unset,
		},
		{
			name:         "rootSquash=false uses maproot and omits mapall",
			params:       map[string]string{paramNFSRootSquash: "false"},
			wantMapAllU:  unset,
			wantMapAllG:  unset,
			wantMapRootU: "root",
			wantMapRootG: "wheel",
		},
		{
			name:         "rootSquash=false ignores mapall params",
			params:       map[string]string{paramNFSRootSquash: "false", paramNFSMapAllUser: "postgres"},
			wantMapAllU:  unset,
			wantMapAllG:  unset,
			wantMapRootU: "root",
			wantMapRootG: "wheel",
		},
		{
			name:         "rootSquash=true keeps default mapall",
			params:       map[string]string{paramNFSRootSquash: "true"},
			wantMapAllU:  "root",
			wantMapAllG:  "wheel",
			wantMapRootU: unset,
			wantMapRootG: unset,
		},
		{
			name:         "hosts and networks are split",
			params:       map[string]string{paramNFSHosts: "10.0.0.1,10.0.0.2", paramNFSNetworks: "10.0.0.0/24"},
			wantMapAllU:  "root",
			wantMapAllG:  "wheel",
			wantMapRootU: unset,
			wantMapRootG: unset,
			wantHosts:    []string{"10.0.0.1", "10.0.0.2"},
			wantNetworks: []string{"10.0.0.0/24"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opts := &client.NFSShareCreateOptions{}
			applyNFSShareParameters(opts, tt.params)

			if got := derefOr(opts.MapAllUser, unset); got != tt.wantMapAllU {
				t.Errorf("MapAllUser = %q, want %q", got, tt.wantMapAllU)
			}
			if got := derefOr(opts.MapAllGroup, unset); got != tt.wantMapAllG {
				t.Errorf("MapAllGroup = %q, want %q", got, tt.wantMapAllG)
			}
			if got := derefOr(opts.MapRootUser, unset); got != tt.wantMapRootU {
				t.Errorf("MapRootUser = %q, want %q", got, tt.wantMapRootU)
			}
			if got := derefOr(opts.MapRootGroup, unset); got != tt.wantMapRootG {
				t.Errorf("MapRootGroup = %q, want %q", got, tt.wantMapRootG)
			}

			if len(opts.Hosts) != len(tt.wantHosts) {
				t.Errorf("Hosts = %v, want %v", opts.Hosts, tt.wantHosts)
			} else {
				for i := range tt.wantHosts {
					if opts.Hosts[i] != tt.wantHosts[i] {
						t.Errorf("Hosts[%d] = %q, want %q", i, opts.Hosts[i], tt.wantHosts[i])
					}
				}
			}
			if len(opts.Networks) != len(tt.wantNetworks) {
				t.Errorf("Networks = %v, want %v", opts.Networks, tt.wantNetworks)
			} else {
				for i := range tt.wantNetworks {
					if opts.Networks[i] != tt.wantNetworks[i] {
						t.Errorf("Networks[%d] = %q, want %q", i, opts.Networks[i], tt.wantNetworks[i])
					}
				}
			}
		})
	}
}
