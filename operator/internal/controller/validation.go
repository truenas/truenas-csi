package controller

import (
	"context"
	"fmt"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	csiv1alpha1 "github.com/truenas/truenas-csi/operator/api/v1alpha1"
)

// Validator performs pre-flight validation of TrueNASCSI resources
type Validator struct {
	client    client.Client
	namespace string
}

// NewValidator creates a new Validator instance
func NewValidator(c client.Client, namespace string) *Validator {
	return &Validator{client: c, namespace: namespace}
}

// Validate performs all validation checks on the TrueNASCSI resource
func (v *Validator) Validate(ctx context.Context, csi *csiv1alpha1.TrueNASCSI) error {
	if err := v.ValidateURL(csi.Spec.TrueNASURL); err != nil {
		return err
	}
	if err := v.ValidateCredentials(ctx, csi.Spec.CredentialsSecret); err != nil {
		return err
	}
	return nil
}

// ValidateURL checks that the TrueNAS URL is valid
func (v *Validator) ValidateURL(url string) error {
	if url == "" {
		return fmt.Errorf("%w: URL is empty", ErrInvalidURL)
	}
	if !strings.HasPrefix(url, "ws://") && !strings.HasPrefix(url, "wss://") {
		return fmt.Errorf("%w: must start with ws:// or wss://", ErrInvalidURL)
	}
	return nil
}

// ValidateCredentials checks that the credentials secret exists and has the required key
func (v *Validator) ValidateCredentials(ctx context.Context, secretName string) error {
	if secretName == "" {
		return fmt.Errorf("%w: secret name is empty", ErrSecretNotFound)
	}

	secret := &corev1.Secret{}
	key := types.NamespacedName{Name: secretName, Namespace: v.namespace}

	if err := v.client.Get(ctx, key, secret); err != nil {
		return fmt.Errorf("%w: %s in namespace %s: %v", ErrSecretNotFound, secretName, v.namespace, err)
	}

	if _, exists := secret.Data[CredentialsSecretKey]; !exists {
		return fmt.Errorf("%w: %s", ErrSecretMissingKey, secretName)
	}

	return nil
}
