package driver

import "testing"

// For CHAP volumes, buildConnector must skip sendtargets discovery
// (DoDiscovery=false) and configure CHAP at the session scope only: the library
// applies session CHAP via CreateDBEntry, gated on DoCHAPDiscovery, and only when
// SessionSecrets.SecretsType == "chap". No discovery secrets are set.
func TestBuildConnector_CHAPEnabled(t *testing.T) {
	h := &ISCSIHandler{}
	config := &ISCSIConfig{
		TargetPortal:   "10.0.0.1:3260",
		TargetIQN:      "iqn.2000-01.io.truenas:vol",
		CHAPUsername:   "k8s",
		CHAPPassword:   "pass1234abcd",
		CHAPUsernameIn: "truenas",
		CHAPPasswordIn: "peerpass1234",
	}

	c := h.buildConnector("vol1", config)

	if c.DoDiscovery {
		t.Error("DoDiscovery must be false for CHAP volumes (log in directly to the known target)")
	}
	if !c.DoCHAPDiscovery {
		t.Error("DoCHAPDiscovery must be true so CreateDBEntry creates the node record and applies session CHAP")
	}
	if c.SessionSecrets.SecretsType != "chap" {
		t.Errorf("SessionSecrets.SecretsType must be \"chap\", got %q", c.SessionSecrets.SecretsType)
	}
	if c.DiscoverySecrets.SecretsType != "" {
		t.Errorf("DiscoverySecrets must be unset (no discovery), got SecretsType %q", c.DiscoverySecrets.SecretsType)
	}
	if c.SessionSecrets.UserName != "k8s" || c.SessionSecrets.Password != "pass1234abcd" {
		t.Errorf("session credentials not propagated: %+v", c.SessionSecrets)
	}
	if c.SessionSecrets.UserNameIn != "truenas" || c.SessionSecrets.PasswordIn != "peerpass1234" {
		t.Errorf("mutual (incoming) credentials not propagated: %+v", c.SessionSecrets)
	}
}

func TestBuildConnector_NoCHAP(t *testing.T) {
	h := &ISCSIHandler{}
	c := h.buildConnector("vol1", &ISCSIConfig{
		TargetPortal: "10.0.0.1:3260",
		TargetIQN:    "iqn.2000-01.io.truenas:vol",
	})

	// Non-CHAP volumes are unchanged: discovery on, no CHAP.
	if !c.DoDiscovery {
		t.Error("DoDiscovery must remain true for non-CHAP volumes")
	}
	if c.DoCHAPDiscovery {
		t.Error("DoCHAPDiscovery must be false when no CHAP credentials are set")
	}
	if c.SessionSecrets.SecretsType != "" || c.DiscoverySecrets.SecretsType != "" {
		t.Errorf("no secrets should be set without CHAP: session=%q discovery=%q",
			c.SessionSecrets.SecretsType, c.DiscoverySecrets.SecretsType)
	}
}
