package driver

import "testing"

func TestSecretOrParam(t *testing.T) {
	tests := []struct {
		name       string
		secrets    map[string]string
		parameters map[string]string
		key        string
		want       string
	}{
		{
			name:       "secret takes precedence over parameter",
			secrets:    map[string]string{"k": "fromSecret"},
			parameters: map[string]string{"k": "fromParam"},
			key:        "k",
			want:       "fromSecret",
		},
		{
			name:       "falls back to parameter when secret absent",
			secrets:    map[string]string{},
			parameters: map[string]string{"k": "fromParam"},
			key:        "k",
			want:       "fromParam",
		},
		{
			name:       "empty secret value falls back to parameter",
			secrets:    map[string]string{"k": ""},
			parameters: map[string]string{"k": "fromParam"},
			key:        "k",
			want:       "fromParam",
		},
		{
			name:       "nil secrets falls back to parameter",
			secrets:    nil,
			parameters: map[string]string{"k": "fromParam"},
			key:        "k",
			want:       "fromParam",
		},
		{
			name:       "missing in both returns empty",
			secrets:    map[string]string{},
			parameters: map[string]string{},
			key:        "k",
			want:       "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := secretOrParam(tt.secrets, tt.parameters, tt.key); got != tt.want {
				t.Errorf("secretOrParam() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestParseEncryptionOptions_KeyFromSecret(t *testing.T) {
	const key = "0000000000000000000000000000000000000000000000000000000000000000"
	parameters := map[string]string{paramEncryption: "true"}
	secrets := map[string]string{paramEncryptionKey: key}

	opts := parseEncryptionOptions(parameters, secrets)
	if opts == nil {
		t.Fatal("expected encryption options, got nil")
	}
	if opts.Key == nil || *opts.Key != key {
		t.Errorf("Key = %v, want %q", opts.Key, key)
	}
	if opts.GenerateKey {
		t.Error("GenerateKey should be false when a key is supplied via secret")
	}
}

func TestParseEncryptionOptions_SecretOverridesParameter(t *testing.T) {
	parameters := map[string]string{
		paramEncryption:    "true",
		paramEncryptionKey: "inlineKeyFromStorageClass",
	}
	secrets := map[string]string{paramEncryptionKey: "keyFromSecret"}

	opts := parseEncryptionOptions(parameters, secrets)
	if opts == nil || opts.Key == nil {
		t.Fatal("expected encryption options with key")
	}
	if *opts.Key != "keyFromSecret" {
		t.Errorf("Key = %q, want secret value", *opts.Key)
	}
}

func TestParseEncryptionOptions_PassphraseFallsBackToParameter(t *testing.T) {
	parameters := map[string]string{
		paramEncryption:           "true",
		paramEncryptionPassphrase: "inlinePassphrase",
	}
	opts := parseEncryptionOptions(parameters, nil)
	if opts == nil || opts.Passphrase == nil {
		t.Fatal("expected encryption options with passphrase")
	}
	if *opts.Passphrase != "inlinePassphrase" {
		t.Errorf("Passphrase = %q, want inline parameter value", *opts.Passphrase)
	}
}

func TestParseEncryptionOptions_DefaultsToGenerateKey(t *testing.T) {
	opts := parseEncryptionOptions(map[string]string{paramEncryption: "true"}, nil)
	if opts == nil {
		t.Fatal("expected encryption options, got nil")
	}
	if !opts.GenerateKey {
		t.Error("expected GenerateKey=true when no key/passphrase supplied")
	}
}

func TestParseEncryptionOptions_DisabledReturnsNil(t *testing.T) {
	if opts := parseEncryptionOptions(map[string]string{}, map[string]string{paramEncryptionKey: "x"}); opts != nil {
		t.Errorf("expected nil when encryption not enabled, got %+v", opts)
	}
}

func TestParseISCSIConfig_CHAPFromSecret(t *testing.T) {
	publishContext := map[string]string{
		PublishContextTargetPortal: "10.0.0.1:3260",
		PublishContextTargetIQN:    "iqn.2000-01.io.truenas:test",
	}
	secrets := map[string]string{
		paramCHAPUsername:   "userFromSecret",
		paramCHAPPassword:   "passFromSecret",
		paramCHAPUsernameIn: "peerUserFromSecret",
		paramCHAPPasswordIn: "peerPassFromSecret",
	}

	config, err := parseISCSIConfig(publishContext, nil, secrets)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if config.CHAPUsername != "userFromSecret" || config.CHAPPassword != "passFromSecret" {
		t.Errorf("CHAP creds not sourced from secret: %+v", config)
	}
	if config.CHAPUsernameIn != "peerUserFromSecret" || config.CHAPPasswordIn != "peerPassFromSecret" {
		t.Errorf("mutual CHAP creds not sourced from secret: %+v", config)
	}
}

func TestParseISCSIConfig_CHAPSecretOverridesVolumeContext(t *testing.T) {
	publishContext := map[string]string{
		PublishContextTargetPortal: "10.0.0.1:3260",
		PublishContextTargetIQN:    "iqn.2000-01.io.truenas:test",
	}
	volumeContext := map[string]string{
		paramCHAPUsername: "userFromVolCtx",
		paramCHAPPassword: "passFromVolCtx",
	}
	secrets := map[string]string{
		paramCHAPUsername: "userFromSecret",
		paramCHAPPassword: "passFromSecret",
	}

	config, err := parseISCSIConfig(publishContext, volumeContext, secrets)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if config.CHAPUsername != "userFromSecret" || config.CHAPPassword != "passFromSecret" {
		t.Errorf("secret should override volume context: %+v", config)
	}
}

func TestParseISCSIConfig_CHAPFallsBackToVolumeContext(t *testing.T) {
	publishContext := map[string]string{
		PublishContextTargetPortal: "10.0.0.1:3260",
		PublishContextTargetIQN:    "iqn.2000-01.io.truenas:test",
	}
	volumeContext := map[string]string{
		paramCHAPUsername: "userFromVolCtx",
		paramCHAPPassword: "passFromVolCtx",
	}

	config, err := parseISCSIConfig(publishContext, volumeContext, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if config.CHAPUsername != "userFromVolCtx" || config.CHAPPassword != "passFromVolCtx" {
		t.Errorf("expected fallback to volume context: %+v", config)
	}
}

func TestParseNVMeOFConfig_DHCHAPFromSecret(t *testing.T) {
	publishContext := map[string]string{
		PublishContextNVMeSubNQN:    "nqn.2011-06.com.truenas:csi",
		PublishContextNVMePortAddr:  "10.0.0.1",
		PublishContextNVMePortSvcID: "4420",
	}
	secrets := map[string]string{
		paramNVMeOFDHCHAPKey:     "DHHC-1:00:keyFromSecret:",
		paramNVMeOFDHCHAPCtrlKey: "DHHC-1:00:ctrlFromSecret:",
	}

	cfg := parseNVMeOFConfig(publishContext, nil, secrets)
	if cfg.DHCHAPKey != "DHHC-1:00:keyFromSecret:" || cfg.DHCHAPCtrlKey != "DHHC-1:00:ctrlFromSecret:" {
		t.Errorf("DH-CHAP keys not sourced from secret: %q / %q", cfg.DHCHAPKey, cfg.DHCHAPCtrlKey)
	}
}

func TestParseNVMeOFConfig_DHCHAPSecretOverridesVolumeContext(t *testing.T) {
	publishContext := map[string]string{
		PublishContextNVMeSubNQN:    "nqn.2011-06.com.truenas:csi",
		PublishContextNVMePortAddr:  "10.0.0.1",
		PublishContextNVMePortSvcID: "4420",
	}
	volumeContext := map[string]string{paramNVMeOFDHCHAPKey: "DHHC-1:00:keyFromVolCtx:"}
	secrets := map[string]string{paramNVMeOFDHCHAPKey: "DHHC-1:00:keyFromSecret:"}

	cfg := parseNVMeOFConfig(publishContext, volumeContext, secrets)
	if cfg.DHCHAPKey != "DHHC-1:00:keyFromSecret:" {
		t.Errorf("secret should override volume context: %q", cfg.DHCHAPKey)
	}
}

func TestValidateNVMeOFParameters_KeyFromSecretSatisfiesValidation(t *testing.T) {
	// hostNQN inline, DH-CHAP key supplied via secret: validation must see the
	// effective key (from the secret) and not error.
	parameters := map[string]string{paramNVMeOFHostNQN: "nqn.2014-08.org.nvmexpress:uuid:node-a"}
	secrets := map[string]string{paramNVMeOFDHCHAPKey: "DHHC-1:00:key:"}

	if err := validateNVMeOFParameters(parameters, secrets); err != nil {
		t.Errorf("unexpected validation error: %v", err)
	}
}

func TestValidateNVMeOFParameters_KeyFromSecretRequiresHostNQN(t *testing.T) {
	// DH-CHAP key via secret but no hostNQN: validation must catch it using the
	// effective (secret-sourced) key.
	secrets := map[string]string{paramNVMeOFDHCHAPKey: "DHHC-1:00:key:"}

	if err := validateNVMeOFParameters(map[string]string{}, secrets); err == nil {
		t.Error("expected error when DH-CHAP key set (via secret) without hostNQN")
	}
}
