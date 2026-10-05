package driver

import (
	"context"
	"net"
	"time"
)

// lookupHost resolves a host name to its addresses. Tests replace it.
var lookupHost = net.DefaultResolver.LookupHost

// hostResolveTimeout bounds a portal host name lookup.
const hostResolveTimeout = 10 * time.Second

// portalAddresses returns the addresses a TrueNAS iSCSI portal may listen on for
// the configured portal host: the host itself and, for a host name, what it
// resolves to. TrueNAS lists a portal's listen addresses as IPs, so a host name
// alone matches only a portal listening on all addresses. A name that does not
// resolve still gets that match.
func portalAddresses(ctx context.Context, host string) []string {
	addrs := []string{host}
	if net.ParseIP(host) != nil {
		return addrs
	}
	ctx, cancel := context.WithTimeout(ctx, hostResolveTimeout)
	defer cancel()
	if resolved, err := lookupHost(ctx, host); err == nil {
		addrs = append(addrs, resolved...)
	}
	return addrs
}
