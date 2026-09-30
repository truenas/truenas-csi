package driver

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os/exec"
	"strings"
	"time"
)

const (
	// iscsiLoginTimeout bounds the node's login to a target. csi-lib-iscsi runs
	// every iscsiadm call with a fixed 3-second limit, which a login to a busy
	// appliance, or one of many after a node restart, can outlast.
	iscsiLoginTimeout = 30 * time.Second

	// iscsiadmTimeout bounds the iscsiadm calls that only touch the node's own
	// state: listing sessions and writing the node record.
	iscsiadmTimeout = 10 * time.Second

	// iscsiDefaultIface is the iSCSI interface csi-lib-iscsi uses when none is set.
	iscsiDefaultIface = "default"

	// iscsiDefaultPort is the port assumed for a portal given without one.
	iscsiDefaultPort = "3260"

	// iscsiadmExitSessionExists is iscsiadm's ISCSI_ERR_SESS_EXISTS: the node is
	// already logged in to the target.
	iscsiadmExitSessionExists = 15
)

// ensureISCSISession logs the node in to the volume's target, unless a session to
// it is already up. csi-lib-iscsi would log in too, but within its fixed 3 seconds,
// and a login it cuts short often completes in the background, while the next
// attempt starts over and is cut short again. Logging in here, with time to
// finish, leaves csi-lib-iscsi a session to find, so it only waits for the device.
func (h *ISCSIHandler) ensureISCSISession(ctx context.Context, config *ISCSIConfig, loginTimeout time.Duration) error {
	exists, err := iscsiSessionExists(ctx, config.TargetIQN)
	if err != nil {
		return fmt.Errorf("failed to list iSCSI sessions: %w", withCommandOutput(err))
	}
	if exists {
		h.log.V(LogLevelDebug).Info("iSCSI session already up", "iqn", config.TargetIQN)
		return nil
	}

	portal := iscsiPortalWithPort(config.TargetPortal)
	node := []string{"-m", "node", "-T", config.TargetIQN, "-p", portal}

	// -o new replaces a record an earlier attempt left, so the settings below always
	// start from scratch.
	if _, err := runISCSIAdm(ctx, iscsiadmTimeout, append(node, "-I", iscsiDefaultIface, "-o", "new")...); err != nil {
		return fmt.Errorf("failed to create the iSCSI node record for %s: %w", config.TargetIQN, withCommandOutput(err))
	}
	if chap := sessionCHAPArgs(config); chap != nil {
		if _, err := runISCSIAdm(ctx, iscsiadmTimeout, append(append(node, "-o", "update"), chap...)...); err != nil {
			// The arguments hold the CHAP secrets, so the error leaves them out.
			return fmt.Errorf("failed to set the CHAP credentials on the iSCSI node record for %s: %w", config.TargetIQN, withCommandOutput(err))
		}
	}

	start := time.Now()
	_, err = runISCSIAdm(ctx, loginTimeout, append(node, "-l")...)
	elapsed := time.Since(start).Round(time.Millisecond)
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		// The login may still finish in the background; the next attempt finds the
		// session instead of starting over.
		return fmt.Errorf("iSCSI login to %s at %s did not finish within %s", config.TargetIQN, portal, elapsed)
	case isISCSIExitCode(err, iscsiadmExitSessionExists):
		// A login still running from an earlier attempt finished in the meantime.
	case err != nil:
		return fmt.Errorf("iSCSI login to %s at %s failed after %s: %w", config.TargetIQN, portal, elapsed, withCommandOutput(err))
	}

	h.log.V(LogLevelDebug).Info("Logged in to iSCSI target", "iqn", config.TargetIQN, "portal", portal, "took", elapsed.String())
	return nil
}

// iscsiSessionExists reports whether the node has a session to the target. It
// matches on the target name alone: each volume has a target of its own, and the
// session list shows the portal's address, which need not match how the portal
// was given, a host name for example.
func iscsiSessionExists(ctx context.Context, iqn string) (bool, error) {
	out, err := runISCSIAdm(ctx, iscsiadmTimeout, "-m", "session")
	if isISCSIExitCode(err, iscsiadmExitNoObjsFound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	// Each line reads "tcp: [1] 10.0.0.1:3260,1 iqn.2005-10.org.freenas.ctl:x (non-flash)".
	for _, line := range strings.Split(string(out), "\n") {
		if fields := strings.Fields(line); len(fields) >= 4 && fields[3] == iqn {
			return true, nil
		}
	}
	return false, nil
}

// sessionCHAPArgs returns the node record settings for the volume's session CHAP
// credentials, including the ones the target presents back for mutual CHAP, or
// nil when the volume uses none. These are the settings csi-lib-iscsi writes.
func sessionCHAPArgs(config *ISCSIConfig) []string {
	if config.CHAPUsername == "" || config.CHAPPassword == "" {
		return nil
	}
	args := []string{
		"-n", "node.session.auth.authmethod", "-v", "CHAP",
		"-n", "node.session.auth.username", "-v", config.CHAPUsername,
		"-n", "node.session.auth.password", "-v", config.CHAPPassword,
	}
	if config.CHAPUsernameIn != "" {
		args = append(args, "-n", "node.session.auth.username_in", "-v", config.CHAPUsernameIn)
	}
	if config.CHAPPasswordIn != "" {
		args = append(args, "-n", "node.session.auth.password_in", "-v", config.CHAPPasswordIn)
	}
	return args
}

// iscsiPortalWithPort returns the portal as host:port, adding the default port
// when it has none.
func iscsiPortalWithPort(portal string) string {
	if _, _, err := net.SplitHostPort(portal); err == nil {
		return portal
	}
	return net.JoinHostPort(portal, iscsiDefaultPort)
}

// runISCSIAdm runs iscsiadm, giving up after timeout or when ctx ends. The
// arguments are not logged, since they can hold CHAP secrets.
func runISCSIAdm(ctx context.Context, timeout time.Duration, args ...string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	out, err := exec.CommandContext(ctx, "iscsiadm", args...).Output()
	if ctxErr := ctx.Err(); ctxErr != nil {
		return out, ctxErr
	}
	return out, err
}

// isISCSIExitCode reports whether an iscsiadm call exited with code.
func isISCSIExitCode(err error, code int) bool {
	var exitErr *exec.ExitError
	return errors.As(err, &exitErr) && exitErr.ExitCode() == code
}
