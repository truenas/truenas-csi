package driver

import "testing"

func TestBridgeISCSICHAPParams(t *testing.T) {
	t.Run("no CHAP params leaves node-side keys empty", func(t *testing.T) {
		p := map[string]string{paramProtocol: "iscsi"}
		bridgeISCSICHAPParams(p)
		for _, k := range []string{paramCHAPUsername, paramCHAPPassword, paramCHAPUsernameIn, paramCHAPPasswordIn} {
			if p[k] != "" {
				t.Errorf("%s = %q, want empty", k, p[k])
			}
		}
	})

	t.Run("standard CHAP maps to outgoing node credentials", func(t *testing.T) {
		p := map[string]string{
			paramISCSIChapUser:   "chapuser",
			paramISCSIChapSecret: "chapsecret12",
		}
		bridgeISCSICHAPParams(p)
		if p[paramCHAPUsername] != "chapuser" {
			t.Errorf("%s = %q, want chapuser", paramCHAPUsername, p[paramCHAPUsername])
		}
		if p[paramCHAPPassword] != "chapsecret12" {
			t.Errorf("%s = %q, want chapsecret12", paramCHAPPassword, p[paramCHAPPassword])
		}
		if p[paramCHAPUsernameIn] != "" || p[paramCHAPPasswordIn] != "" {
			t.Errorf("mutual keys should be empty, got in=%q/%q", p[paramCHAPUsernameIn], p[paramCHAPPasswordIn])
		}
	})

	t.Run("mutual CHAP maps peer credentials to incoming node credentials", func(t *testing.T) {
		p := map[string]string{
			paramISCSIChapUser:       "chapuser",
			paramISCSIChapSecret:     "chapsecret12",
			paramISCSIChapPeerUser:   "peeruser",
			paramISCSIChapPeerSecret: "peersecret12",
		}
		bridgeISCSICHAPParams(p)
		if p[paramCHAPUsername] != "chapuser" || p[paramCHAPPassword] != "chapsecret12" {
			t.Errorf("outgoing = %q/%q", p[paramCHAPUsername], p[paramCHAPPassword])
		}
		if p[paramCHAPUsernameIn] != "peeruser" || p[paramCHAPPasswordIn] != "peersecret12" {
			t.Errorf("incoming = %q/%q, want peeruser/peersecret12", p[paramCHAPUsernameIn], p[paramCHAPPasswordIn])
		}
	})

	t.Run("explicit node-side value is not overwritten", func(t *testing.T) {
		p := map[string]string{
			paramISCSIChapUser: "chapuser",
			paramCHAPUsername:  "explicit",
		}
		bridgeISCSICHAPParams(p)
		if p[paramCHAPUsername] != "explicit" {
			t.Errorf("%s = %q, want explicit (not overwritten)", paramCHAPUsername, p[paramCHAPUsername])
		}
	})
}
