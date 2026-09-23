package driver

import (
	"context"
	"fmt"
	"maps"
	"testing"

	"github.com/container-storage-interface/spec/lib/go/csi"

	"github.com/truenas/truenas-csi/pkg/client"
)

func mutualCHAPParameters() map[string]string {
	return map[string]string{
		paramProtocol:            ProtocolISCSI,
		paramISCSIChapUser:       "openshift",
		paramISCSIChapSecret:     "testtesttest1",
		paramISCSIChapPeerUser:   "openshift",
		paramISCSIChapPeerSecret: "testtesttest2",
	}
}

func TestISCSIAccessFromParameters(t *testing.T) {
	tests := []struct {
		name       string
		parameters map[string]string
		wantErr    bool
		wantMethod string
	}{
		{"no CHAP", map[string]string{}, false, client.ISCSIAuthMethodNone},
		{"one-way CHAP", map[string]string{paramISCSIChapUser: "u", paramISCSIChapSecret: "testtesttest1"}, false, client.ISCSIAuthMethodCHAP},
		{"mutual CHAP", mutualCHAPParameters(), false, client.ISCSIAuthMethodCHAPMutual},
		{"user without secret", map[string]string{paramISCSIChapUser: "u"}, true, ""},
		{"peer without user", map[string]string{paramISCSIChapPeerUser: "p", paramISCSIChapPeerSecret: "testtesttest2"}, true, ""},
		{"peer without peer secret", map[string]string{paramISCSIChapUser: "u", paramISCSIChapSecret: "testtesttest1", paramISCSIChapPeerUser: "p"}, true, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			access, err := iscsiAccessFromParameters(tt.parameters)
			if (err != nil) != tt.wantErr {
				t.Fatalf("iscsiAccessFromParameters() error = %v, wantErr %v", err, tt.wantErr)
			}
			if err == nil && access.authMethod() != tt.wantMethod {
				t.Errorf("authMethod() = %q, want %q", access.authMethod(), tt.wantMethod)
			}
		})
	}
}

// targetGroup returns the single portal group of the fake's target with the given name.
func targetGroup(t *testing.T, f *fakeTrueNAS, name string) map[string]any {
	t.Helper()
	for _, target := range f.all("iscsi.target") {
		if target["name"] == name {
			groups := groupsOf(target)
			if len(groups) != 1 {
				t.Fatalf("target %s has %d portal groups, want 1", name, len(groups))
			}
			return groups[0]
		}
	}
	t.Fatalf("no target named %s", name)
	return nil
}

// assertMutualCHAPTarget checks that the named target requires mutual CHAP with an
// auth entry that exists and leaves discovery alone.
func assertMutualCHAPTarget(t *testing.T, f *fakeTrueNAS, name string) {
	t.Helper()
	group := targetGroup(t, f, name)
	if group["authmethod"] != client.ISCSIAuthMethodCHAPMutual {
		t.Errorf("target %s authmethod = %v, want %s", name, group["authmethod"], client.ISCSIAuthMethodCHAPMutual)
	}
	var found bool
	for _, auth := range f.all("iscsi.auth") {
		if fmt.Sprint(auth["tag"]) != fmt.Sprint(group["auth"]) {
			continue
		}
		found = true
		if auth["discovery_auth"] != client.ISCSIAuthMethodNone {
			t.Errorf("auth tag %v discovery_auth = %v, want %s: discovery is appliance-wide", auth["tag"], auth["discovery_auth"], client.ISCSIAuthMethodNone)
		}
		if auth["peeruser"] != "openshift" {
			t.Errorf("auth tag %v peeruser = %v, want openshift", auth["tag"], auth["peeruser"])
		}
	}
	if !found {
		t.Errorf("target %s references auth tag %v, which has no entry", name, group["auth"])
	}
}

// assertNodeCanAuthenticate checks that a volume context carries the credentials
// under the names the node reads.
func assertNodeCanAuthenticate(t *testing.T, volCtx map[string]string) {
	t.Helper()
	want := map[string]string{
		paramCHAPUsername:   "openshift",
		paramCHAPPassword:   "testtesttest1",
		paramCHAPUsernameIn: "openshift",
		paramCHAPPasswordIn: "testtesttest2",
	}
	for key, value := range want {
		if volCtx[key] != value {
			t.Errorf("volume context %s = %q, want %q", key, volCtx[key], value)
		}
	}
	if volCtx[PublishContextTargetIQN] == "" || volCtx[PublishContextTargetPortal] == "" {
		t.Errorf("volume context has no target: %v", volCtx)
	}
}

// TrueNAS permits one auth entry with mutual discovery authentication, so claiming
// it per volume made every mutual-CHAP volume after the first fail.
func TestCreateISCSIVolume_MutualCHAPForEveryVolume(t *testing.T) {
	f := newFakeTrueNAS(t)
	s := f.controller()
	ctx := context.Background()

	for _, volumeID := range []string{"tank/pvc-a", "tank/pvc-b"} {
		volInfo, err := s.createISCSIVolume(ctx, volumeID, volumeID, 1*GiB, mutualCHAPParameters())
		if err != nil {
			t.Fatalf("createISCSIVolume(%s) = %v", volumeID, err)
		}
		assertMutualCHAPTarget(t, f, makeISCSITargetSuffix(volumeID))
		assertNodeCanAuthenticate(t, volInfo.VolumeContext)
	}

	a := targetGroup(t, f, makeISCSITargetSuffix("tank/pvc-a"))["auth"]
	b := targetGroup(t, f, makeISCSITargetSuffix("tank/pvc-b"))["auth"]
	if fmt.Sprint(a) == fmt.Sprint(b) {
		t.Errorf("both volumes share auth tag %v, so each would accept the other's credentials", a)
	}
}

// A CreateVolume interrupted after the ZVOL is retried through ensureISCSIChain,
// which used to finish the target with no auth and no initiator group at all.
func TestEnsureISCSIChain_CompletesTargetWithAccess(t *testing.T) {
	f := newFakeTrueNAS(t)
	s := f.controller()
	const volumeID = "tank/pvc-a"

	parameters := mutualCHAPParameters()
	parameters[paramISCSIInitiators] = "iqn.1993-08.org.debian:01:node1"

	volCtx, err := s.ensureISCSIChain(context.Background(), volumeID, volumeID, parameters)
	if err != nil {
		t.Fatalf("ensureISCSIChain() = %v", err)
	}

	assertMutualCHAPTarget(t, f, makeISCSITargetSuffix(volumeID))
	assertNodeCanAuthenticate(t, volCtx)
	group := targetGroup(t, f, makeISCSITargetSuffix(volumeID))
	if initiators := f.all("iscsi.initiator"); len(initiators) != 1 || fmt.Sprint(group["initiator"]) != fmt.Sprint(initiators[0]["id"]) {
		t.Errorf("target initiator = %v with initiator groups %v, want the volume's own", group["initiator"], initiators)
	}
}

// seedISCSIVolume stores a target, extent and association for volumeID as an
// earlier CreateVolume would have, with the given portal group.
func seedISCSIVolume(f *fakeTrueNAS, volumeID string, group map[string]any) {
	targetID := f.seed("iscsi.target", map[string]any{"name": makeISCSITargetSuffix(volumeID), "groups": []any{group}})
	extentID := f.seed("iscsi.extent", map[string]any{"name": makeISCSIExtentName(volumeID), "disk": "zvol/" + volumeID})
	f.seed("iscsi.targetextent", map[string]any{"target": targetID, "extent": extentID, "lunid": float64(0)})
}

// Earlier versions left targets like this behind. Retrying CreateVolume must not
// hand one back as if it were protected.
func TestEnsureISCSIChain_RepairsAnUnauthenticatedTarget(t *testing.T) {
	f := newFakeTrueNAS(t)
	s := f.controller()
	const volumeID = "tank/pvc-a"
	seedISCSIVolume(f, volumeID, map[string]any{"portal": float64(1), "authmethod": client.ISCSIAuthMethodNone})

	volCtx, err := s.ensureISCSIChain(context.Background(), volumeID, volumeID, mutualCHAPParameters())
	if err != nil {
		t.Fatalf("ensureISCSIChain() = %v", err)
	}

	assertMutualCHAPTarget(t, f, makeISCSITargetSuffix(volumeID))
	assertNodeCanAuthenticate(t, volCtx)
}

func TestEnsureISCSIChain_LeavesAProtectedTargetAlone(t *testing.T) {
	f := newFakeTrueNAS(t)
	s := f.controller()
	const volumeID = "tank/pvc-a"
	f.seed("iscsi.auth", map[string]any{"tag": float64(7), "user": "openshift", "peeruser": "openshift", "discovery_auth": client.ISCSIAuthMethodNone})
	seedISCSIVolume(f, volumeID, map[string]any{"portal": float64(1), "authmethod": client.ISCSIAuthMethodCHAPMutual, "auth": float64(7)})

	volCtx, err := s.ensureISCSIChain(context.Background(), volumeID, volumeID, mutualCHAPParameters())
	if err != nil {
		t.Fatalf("ensureISCSIChain() = %v", err)
	}

	if n := len(f.callsTo("iscsi.target.update")) + len(f.callsTo("iscsi.auth.create")); n != 0 {
		t.Errorf("a target that already enforces the StorageClass was changed (%d calls)", n)
	}
	assertNodeCanAuthenticate(t, volCtx)
}

// Clones got a target with no access control at all, whatever the StorageClass said.
func TestCreateISCSITargetForClone_AppliesAccess(t *testing.T) {
	f := newFakeTrueNAS(t)
	s := f.controller()
	const volumeID = "tank/pvc-clone"

	volInfo, err := s.createISCSITargetForClone(context.Background(), volumeID, volumeID, 1*GiB, mutualCHAPParameters())
	if err != nil {
		t.Fatalf("createISCSITargetForClone() = %v", err)
	}

	assertMutualCHAPTarget(t, f, makeISCSITargetSuffix(volumeID))
	assertNodeCanAuthenticate(t, volInfo.VolumeContext)
}

// A failed step after the auth entry is created must not leave the entry behind.
func TestCreateISCSIVolume_UndoesAccessWhenTargetFails(t *testing.T) {
	f := newFakeTrueNAS(t)
	s := f.controller()
	f.fail["iscsi.target.create"] = true

	parameters := mutualCHAPParameters()
	parameters[paramISCSIInitiators] = "iqn.1993-08.org.debian:01:node1"
	if _, err := s.createISCSIVolume(context.Background(), "tank/pvc-a", "tank/pvc-a", 1*GiB, parameters); err == nil {
		t.Fatal("createISCSIVolume() succeeded with target creation failing")
	}

	if auths := f.all("iscsi.auth"); len(auths) != 0 {
		t.Errorf("auth entries left behind: %v", auths)
	}
	if initiators := f.all("iscsi.initiator"); len(initiators) != 0 {
		t.Errorf("initiator groups left behind: %v", initiators)
	}
}

// Deleting a volume used to leave its auth entry and initiator group forever,
// since the volume information rebuilt from TrueNAS never recorded them.
func TestDeleteVolume_ReleasesISCSIAccess(t *testing.T) {
	f := newFakeTrueNAS(t)
	s := f.controller()
	ctx := context.Background()

	parameters := mutualCHAPParameters()
	parameters[paramISCSIInitiators] = "iqn.1993-08.org.debian:01:node1"
	if _, err := s.createISCSIVolume(ctx, "tank/pvc-a", "tank/pvc-a", 1*GiB, parameters); err != nil {
		t.Fatalf("createISCSIVolume() = %v", err)
	}
	// The fake's dataset create stores only what was sent; give the ZVOL the type
	// DeleteVolume looks for.
	for _, ds := range f.all("pool.dataset") {
		ds["type"] = datasetTypeVolume
	}

	if _, err := s.DeleteVolume(ctx, &csi.DeleteVolumeRequest{VolumeId: "tank/pvc-a"}); err != nil {
		t.Fatalf("DeleteVolume() = %v", err)
	}

	if targets := f.all("iscsi.target"); len(targets) != 0 {
		t.Fatalf("targets left behind: %v", targets)
	}
	if auths := f.all("iscsi.auth"); len(auths) != 0 {
		t.Errorf("auth entries left behind: %v", auths)
	}
	if initiators := f.all("iscsi.initiator"); len(initiators) != 0 {
		t.Errorf("initiator groups left behind: %v", initiators)
	}
}

func TestReleaseISCSIAccess_KeepsWhatAnotherTargetUses(t *testing.T) {
	f := newFakeTrueNAS(t)
	s := f.controller()

	f.seed("iscsi.auth", map[string]any{"tag": float64(5), "user": "shared"})
	f.seed("iscsi.auth", map[string]any{"tag": float64(5), "user": "shared-2"})
	unusedAuth := f.seed("iscsi.auth", map[string]any{"tag": float64(6), "user": "unused"})
	shared := f.seed("iscsi.initiator", map[string]any{})
	unused := f.seed("iscsi.initiator", map[string]any{})
	f.seed("iscsi.target", map[string]any{"name": "other", "groups": []any{
		map[string]any{"portal": float64(1), "auth": float64(5), "initiator": shared},
	}})

	s.releaseISCSIAccess(context.Background(), []client.ISCSITargetGroup{
		{Auth: 5, Initiator: int(shared)},
		{Auth: 6, Initiator: int(unused)},
	})

	tags := map[string]int{}
	for _, auth := range f.all("iscsi.auth") {
		tags[fmt.Sprint(auth["tag"])]++
	}
	if want := map[string]int{"5": 2}; !maps.Equal(tags, want) {
		t.Errorf("auth entries by tag = %v, want %v", tags, want)
	}
	initiators := f.all("iscsi.initiator")
	if len(initiators) != 1 || initiators[0]["id"] != shared {
		t.Errorf("initiator groups = %v, want only %v", initiators, shared)
	}

	// TrueNAS refuses to delete an auth entry a target uses, so the outcome above
	// would hold even if the driver tried. It must not try.
	for _, call := range f.callsTo("iscsi.auth.delete") {
		if call[0] != unusedAuth {
			t.Errorf("tried to delete auth entry %v, which another target uses", call[0])
		}
	}
	for _, call := range f.callsTo("iscsi.initiator.delete") {
		if call[0] != unused {
			t.Errorf("tried to delete initiator group %v, which another target uses", call[0])
		}
	}
}
