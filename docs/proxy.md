# Outbound Proxy

On clusters whose egress has to go through an HTTP proxy, the driver can reach the
TrueNAS API through one. It reads the standard `HTTP_PROXY`, `HTTPS_PROXY` and
`NO_PROXY` variables, in upper or lower case, like any Go program:

- A `wss://` TrueNAS URL goes through `HTTPS_PROXY`, and a `ws://` one through
  `HTTP_PROXY`. For `wss://` the proxy has to support `CONNECT`.
- `NO_PROXY` is a comma-separated list of hosts, domains (`.example.internal`),
  IP addresses and CIDRs (`10.0.0.0/8`) to reach directly.

Only the API connection is affected: the WebSocket and the supported-versions check
the driver makes when it starts. Storage traffic, meaning NFS mounts, iSCSI logins
and NVMe-oF connections, is made by the node's kernel and never goes through the
proxy.

Both the controller and the node pods connect to the API, so both get the settings.
They are read when a pod starts. The driver logs the route it takes at startup,
which shows whether `NO_PROXY` applied:

```
"Connecting to TrueNAS" url="wss://10.0.0.100/api/current" proxy="http://proxy.example:3128"
```

`proxy="direct"` means no proxy is used. A password in the proxy URL is masked in
the log; a user name is not.

## Operator

Set `useClusterProxy` on the TrueNASCSI resource:

```yaml
spec:
  useClusterProxy: true
```

The operator then passes its own `HTTP_PROXY`, `HTTPS_PROXY` and `NO_PROXY` to the
driver. On OpenShift, OLM gives the operator the cluster-wide proxy settings from
the `cluster` Proxy object. Elsewhere, set the variables on the operator's
Deployment. A change to them rolls the controller and node pods once the operator
restarts with the new values.

It is off by default, so a cluster-wide proxy does not start carrying TrueNAS
traffic without anyone asking for it. With it on, TrueNAS is reached through the
proxy unless its host is in `NO_PROXY`: to keep reaching it directly, add it to
the cluster Proxy's `noProxy`.

## Helm

```yaml
proxy:
  httpsProxy: http://proxy.example:3128
  httpProxy: http://proxy.example:3128
  noProxy: 10.0.0.0/8,.example.internal
```

An upgrade that changes them rolls the pods.

## Manifest

Uncomment the proxy keys in the `truenas-csi-config` ConfigMap in
`deploy/truenas-csi-driver.yaml`:

```yaml
  httpsProxy: "http://proxy.example:3128"
  httpProxy: "http://proxy.example:3128"
  noProxy: "10.0.0.0/8,.example.internal"
```

Then restart both workloads:

```bash
kubectl -n truenas-csi rollout restart deployment/truenas-csi-controller
kubectl -n truenas-csi rollout restart daemonset/truenas-csi-node
```

## TLS-inspecting proxies

A proxy that inspects TLS presents its own certificate for TrueNAS, signed by the
proxy's CA. Give the driver that CA as described in
[TLS and Private CAs](tls.md). On OpenShift, the cluster's trusted CA bundle
usually already holds it.
