# TLS and Private CAs

The driver connects to the TrueNAS API over `wss://` and verifies the TrueNAS
certificate like any TLS client. Which setup you need depends on the certificate:

| TrueNAS certificate | What to set |
|---------------------|-------------|
| Issued by a public CA | Nothing |
| Issued by a private CA | Give the driver that CA, as below |
| The self-signed certificate TrueNAS ships with | Skip verification: `insecureSkipTLS` (operator), `truenas.insecureSkipTLS` (Helm), `truenasInsecure: "true"` (manifest). Not recommended for production |

Whatever CA signed it, the certificate must be issued for the host in the TrueNAS
URL, as a DNS name or IP address in its subject alternative names. The self-signed
certificate TrueNAS ships with is issued for `localhost` only, so trusting it
does not help. Replace it in TrueNAS under Credentials > Certificates with one
issued for the address the driver uses.

Behind a proxy that inspects TLS, the driver sees the proxy's certificate instead,
signed by the proxy's CA; trust that CA the same way. See
[Outbound Proxy](proxy.md).

## Trusting a private CA

The driver reads a PEM file of CA certificates from the path in
`TRUENAS_CA_BUNDLE` and trusts them in addition to the ones its image already
trusts. Each deployment method sets that up for you from a ConfigMap. Both the
controller and the node pods connect to TrueNAS, so both get it.

### Operator

Put the certificates in a ConfigMap in the driver namespace and point
`trustedCA` at it:

```yaml
spec:
  trustedCA:
    name: truenas-trusted-ca
    key: ca-bundle.crt   # the default
```

On OpenShift, the cluster can fill the ConfigMap with every CA it trusts. Create
it empty with the injection label, and OpenShift adds the bundle under
`ca-bundle.crt`:

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: trusted-cabundle
  namespace: truenas-csi
  labels:
    config.openshift.io/inject-trusted-cabundle: "true"
data: {}
```

```yaml
spec:
  trustedCA:
    name: trusted-cabundle
```

The operator watches the ConfigMap. Changing the certificates in it, including
OpenShift updating the cluster bundle, rolls the controller and node pods, since
the driver reads the bundle only at startup. If the ConfigMap or its key is
missing, the TrueNASCSI resource reports it as `Degraded`.

### Helm

Give the certificates inline, and the chart creates the ConfigMap:

```yaml
truenas:
  caBundle: |
    -----BEGIN CERTIFICATE-----
    ...
    -----END CERTIFICATE-----
```

Or name a ConfigMap you manage:

```yaml
truenas:
  existingCABundleConfigMap: truenas-trusted-ca
  existingCABundleKey: ca-bundle.crt   # the default
```

An upgrade that changes `truenas.caBundle` rolls the pods. The chart cannot see
inside a ConfigMap it did not create, so after changing that one, restart the
pods yourself.

### Manifest

Create the ConfigMap:

```bash
kubectl -n truenas-csi create configmap truenas-trusted-ca --from-file=ca-bundle.crt=my-ca.pem
```

Then, in `deploy/truenas-csi-driver.yaml`, mount it into the `csi-controller`
container of the controller Deployment and the `csi-node` container of the node
DaemonSet, and point `TRUENAS_CA_BUNDLE` at the file:

```yaml
          env:
            - name: TRUENAS_CA_BUNDLE
              value: /etc/truenas-csi/trusted-ca/ca-bundle.crt
          volumeMounts:
            - name: trusted-ca
              mountPath: /etc/truenas-csi/trusted-ca
              readOnly: true
      volumes:
        - name: trusted-ca
          configMap:
            name: truenas-trusted-ca
```

After changing the certificates, restart both workloads:

```bash
kubectl -n truenas-csi rollout restart deployment/truenas-csi-controller
kubectl -n truenas-csi rollout restart daemonset/truenas-csi-node
```

## When verification fails

If the driver cannot verify the TrueNAS certificate, it stops at startup with a
certificate error that names the settings above, and Kubernetes restarts it until
the certificate or the trust is fixed. The error says which check failed:

- `certificate signed by unknown authority`: the driver does not trust the CA.
  Add it as above.
- `certificate is valid for ..., not ...`: the certificate is not issued for the
  host in the TrueNAS URL. Reissue it, or connect by a name it covers.

A connection that verified at startup and drops later is retried without the
driver stopping, so a running driver is not taken down by a certificate change on
TrueNAS.
