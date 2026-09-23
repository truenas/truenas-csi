package driver

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/truenas/truenas-csi/pkg/client"
)

// iscsiAccess is the access control a StorageClass asks for on a volume's iSCSI
// target: CHAP credentials, optionally mutual, and an initiator allow-list.
type iscsiAccess struct {
	chapUser, chapSecret string
	peerUser, peerSecret string
	initiators           []string
}

// iscsiAccessFromParameters reads the requested access control from the
// StorageClass parameters, rejecting combinations TrueNAS cannot enforce.
func iscsiAccessFromParameters(parameters map[string]string) (iscsiAccess, error) {
	a := iscsiAccess{
		chapUser:   parameters[paramISCSIChapUser],
		chapSecret: parameters[paramISCSIChapSecret],
		peerUser:   parameters[paramISCSIChapPeerUser],
		peerSecret: parameters[paramISCSIChapPeerSecret],
	}
	if initiators := parameters[paramISCSIInitiators]; initiators != "" {
		a.initiators = strings.Split(initiators, ",")
	}

	switch {
	case a.chapUser != "" && a.chapSecret == "":
		return a, fmt.Errorf("%s is required when %s is specified", paramISCSIChapSecret, paramISCSIChapUser)
	case a.peerUser != "" && a.chapUser == "":
		return a, fmt.Errorf("%s (mutual CHAP) requires %s", paramISCSIChapPeerUser, paramISCSIChapUser)
	case a.peerUser != "" && a.peerSecret == "":
		return a, fmt.Errorf("%s is required when %s is specified", paramISCSIChapPeerSecret, paramISCSIChapPeerUser)
	}
	return a, nil
}

func (a iscsiAccess) chap() bool   { return a.chapUser != "" }
func (a iscsiAccess) mutual() bool { return a.peerUser != "" }

// authMethod is the target group authmethod that enforces a.
func (a iscsiAccess) authMethod() string {
	switch {
	case a.mutual():
		return client.ISCSIAuthMethodCHAPMutual
	case a.chap():
		return client.ISCSIAuthMethodCHAP
	}
	return client.ISCSIAuthMethodNone
}

// satisfiedBy reports whether a target group already enforces a. A group that
// enforces more than a asks for is left alone rather than weakened.
func (a iscsiAccess) satisfiedBy(g client.ISCSITargetGroup) bool {
	if a.chap() && (g.Auth == 0 || g.AuthMethod != a.authMethod()) {
		return false
	}
	if len(a.initiators) > 0 && g.Initiator == 0 {
		return false
	}
	return true
}

// createISCSIAccess creates the auth entry and initiator group a asks for and
// returns the target group that references them. undo deletes what was created,
// for a caller whose next step fails.
func (s *ControllerServer) createISCSIAccess(ctx context.Context, volumeID string, portalID int, a iscsiAccess) (group client.ISCSITargetGroup, undo func(), err error) {
	group = client.ISCSITargetGroup{Portal: portalID, AuthMethod: a.authMethod()}
	var authID, initiatorID int
	undo = func() {
		if initiatorID > 0 {
			if err := s.driver.Client().DeleteISCSIInitiator(ctx, initiatorID); err != nil {
				s.driver.Log().V(LogLevelDebug).Error(err, "Failed to delete iSCSI initiator group", "initiatorId", initiatorID)
			}
		}
		if authID > 0 {
			if err := s.driver.Client().DeleteISCSIAuth(ctx, authID); err != nil {
				s.driver.Log().V(LogLevelDebug).Error(err, "Failed to delete iSCSI auth", "authId", authID)
			}
		}
	}

	if a.chap() {
		auth, err := s.createISCSIAuth(ctx, a)
		if err != nil {
			return group, nil, fmt.Errorf("failed to create CHAP auth: %w", err)
		}
		authID, group.Auth = auth.ID, auth.Tag
		s.driver.Log().V(LogLevelDebug).Info("Created CHAP auth for iSCSI target", "authId", auth.ID, "tag", auth.Tag,
			"user", a.chapUser, "mutual", a.mutual())
	}

	if len(a.initiators) > 0 {
		initiator, err := s.driver.Client().CreateISCSIInitiator(ctx, &client.ISCSIInitiatorCreateOptions{
			Initiators: a.initiators,
			Comment:    fmt.Sprintf("CSI volume %s", volumeID),
		})
		if err != nil {
			undo()
			return group, nil, fmt.Errorf("failed to create initiator group: %w", err)
		}
		initiatorID, group.Initiator = initiator.ID, initiator.ID
		s.driver.Log().V(LogLevelDebug).Info("Created initiator group for iSCSI target", "initiatorId", initiator.ID, "initiators", a.initiators)
	}

	return group, undo, nil
}

// createISCSIAuth creates the auth entry for a under a tag of its own.
func (s *ControllerServer) createISCSIAuth(ctx context.Context, a iscsiAccess) (*client.ISCSIAuth, error) {
	// The next tag is derived from the existing ones, so two volumes created at
	// once could otherwise pick the same tag and accept each other's credentials.
	s.iscsiAuthMu.Lock()
	defer s.iscsiAuthMu.Unlock()

	tag, err := s.driver.Client().GetNextISCSIAuthTag(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get next auth tag: %w", err)
	}
	return s.driver.Client().CreateISCSIAuth(ctx, &client.ISCSIAuthCreateOptions{
		Tag:        tag,
		User:       a.chapUser,
		Secret:     a.chapSecret,
		PeerUser:   a.peerUser,
		PeerSecret: a.peerSecret,
		// Discovery authentication is appliance-wide: TrueNAS permits a single
		// mutual entry, and any entry that enables it makes every discovery on the
		// appliance require it. A volume's credentials belong to its target alone.
		DiscoveryAuth: client.ISCSIAuthMethodNone,
	})
}

// ensureISCSITarget returns the volume's target, creating it with the requested
// access control or bringing an existing one up to it. A target left behind by an
// interrupted CreateVolume must not end up weaker than one created in one pass.
func (s *ControllerServer) ensureISCSITarget(ctx context.Context, volumeID, name string, portalID int, a iscsiAccess) (*client.ISCSITarget, error) {
	target, err := s.driver.Client().GetISCSITargetByName(ctx, name)
	if err == nil {
		return target, s.enforceISCSIAccess(ctx, volumeID, target, portalID, a)
	}
	if !errors.Is(err, client.ErrNotFound) {
		return nil, fmt.Errorf("failed to look up iSCSI target: %w", err)
	}

	group, undo, err := s.createISCSIAccess(ctx, volumeID, portalID, a)
	if err != nil {
		return nil, err
	}
	target, err = s.driver.Client().CreateISCSITargetWithGroup(ctx, name, fmt.Sprintf("CSI volume %s", volumeID), group)
	if err != nil {
		undo()
		return nil, fmt.Errorf("failed to create iSCSI target: %w", err)
	}
	return target, nil
}

// enforceISCSIAccess brings an existing target up to the requested access control.
// Earlier versions of the driver completed an interrupted CreateVolume with none,
// so a target can exist that anyone can log into.
func (s *ControllerServer) enforceISCSIAccess(ctx context.Context, volumeID string, target *client.ISCSITarget, portalID int, a iscsiAccess) error {
	var previous client.ISCSITargetGroup
	if len(target.Groups) > 0 {
		previous = target.Groups[0]
		if a.satisfiedBy(previous) {
			return nil
		}
	}

	s.driver.Log().Info("iSCSI target lacks the access control its StorageClass asks for, applying it",
		"volumeId", volumeID, "target", target.Name, "authMethod", previous.AuthMethod, "want", a.authMethod())
	group, undo, err := s.createISCSIAccess(ctx, volumeID, portalID, a)
	if err != nil {
		return err
	}
	groups := []client.ISCSITargetGroup{group}
	if _, err := s.driver.Client().UpdateISCSITargetGroups(ctx, target.ID, groups); err != nil {
		undo()
		return err
	}
	target.Groups = groups

	// What the target referenced before may now be unused.
	s.releaseISCSIAccess(ctx, []client.ISCSITargetGroup{previous})
	return nil
}

// releaseISCSIAccess deletes the auth entries and initiator groups that groups
// reference, once no remaining target references them. Call it after the target
// holding groups has been deleted or given new ones. TrueNAS itself refuses to
// delete an auth entry a target uses, so a failed target delete leaves them be.
func (s *ControllerServer) releaseISCSIAccess(ctx context.Context, groups []client.ISCSITargetGroup) {
	authTags, initiatorIDs := map[int]bool{}, map[int]bool{}
	for _, g := range groups {
		if g.Auth > 0 {
			authTags[g.Auth] = true
		}
		if g.Initiator > 0 {
			initiatorIDs[g.Initiator] = true
		}
	}
	if len(authTags) == 0 && len(initiatorIDs) == 0 {
		return
	}

	targets, err := s.driver.Client().ListISCSITargets(ctx)
	if err != nil {
		s.driver.Log().Error(err, "Failed to list iSCSI targets, leaving their auth and initiator groups in place")
		return
	}
	for _, t := range targets {
		for _, g := range t.Groups {
			delete(authTags, g.Auth)
			delete(initiatorIDs, g.Initiator)
		}
	}

	for tag := range authTags {
		auths, err := s.driver.Client().ListISCSIAuthByTag(ctx, tag)
		if err != nil {
			s.driver.Log().Error(err, "Failed to list iSCSI auth", "tag", tag)
			continue
		}
		for _, auth := range auths {
			if err := s.driver.Client().DeleteISCSIAuth(ctx, auth.ID); err != nil {
				s.driver.Log().Error(err, "Failed to delete iSCSI auth", "authId", auth.ID, "tag", tag)
			}
		}
	}
	for id := range initiatorIDs {
		if err := s.driver.Client().DeleteISCSIInitiator(ctx, id); err != nil {
			s.driver.Log().Error(err, "Failed to delete iSCSI initiator group", "initiatorId", id)
		}
	}
}

// iscsiVolumeContext returns the volume context for an iSCSI volume: the
// StorageClass parameters, the CHAP credentials again under the names the node
// reads, and the target the node logs into.
func (s *ControllerServer) iscsiVolumeContext(parameters map[string]string, iqn string, lun int) map[string]string {
	volCtx := copyParameters(parameters)
	bridgeISCSICHAPParams(volCtx)
	volCtx[PublishContextTargetPortal] = s.driver.ISCSIPortal()
	volCtx[PublishContextTargetIQN] = iqn
	volCtx[PublishContextLUN] = strconv.Itoa(lun)
	return volCtx
}
