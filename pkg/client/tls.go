package client

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"os"
)

// ErrCertificateVerification is returned by Connect when the server's TLS
// certificate fails verification.
var ErrCertificateVerification = errors.New("truenas: TLS certificate verification failed")

// NewTLSConfig returns the TLS configuration for connecting to TrueNAS. With
// insecureSkipVerify the server certificate is not checked at all. Otherwise it is
// verified against the system's CAs and, when caBundleFile is set, the PEM
// certificates in that file as well, for a TrueNAS certificate issued by a
// private CA. The system's CAs stay trusted either way.
func NewTLSConfig(insecureSkipVerify bool, caBundleFile string) (*tls.Config, error) {
	if insecureSkipVerify {
		return &tls.Config{InsecureSkipVerify: true}, nil
	}
	if caBundleFile == "" {
		return &tls.Config{}, nil
	}

	pem, err := os.ReadFile(caBundleFile)
	if err != nil {
		return nil, fmt.Errorf("failed to read CA bundle: %w", err)
	}
	pool, err := x509.SystemCertPool()
	if err != nil {
		pool = x509.NewCertPool()
	}
	if !pool.AppendCertsFromPEM(pem) {
		return nil, fmt.Errorf("CA bundle %s holds no PEM certificates", caBundleFile)
	}
	return &tls.Config{RootCAs: pool}, nil
}

// IsCertificateError reports whether err is a failure to verify the server's TLS
// certificate. Retrying never fixes one: the certificate, or what the client
// trusts, has to change.
func IsCertificateError(err error) bool {
	var verifyErr *tls.CertificateVerificationError
	var unknownAuthority x509.UnknownAuthorityError
	var hostname x509.HostnameError
	var invalid x509.CertificateInvalidError
	return errors.As(err, &verifyErr) || errors.As(err, &unknownAuthority) ||
		errors.As(err, &hostname) || errors.As(err, &invalid)
}
