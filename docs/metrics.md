# Prometheus Metrics

The controller pod exposes two kinds of metrics: the CSI sidecars' operation
metrics, which are enabled in the shipped manifest, and the driver's own
endpoint, which is disabled until you configure an address.

## Ports

Every container in the controller pod needs its own port, so they are allocated
as follows. All of them are pod-network ports with no `hostPort`.

| Port | Container | Enabled by |
|------|-----------|-----------|
| 8080 | csi-controller (the driver) | `metricsAddr` in the ConfigMap |
| 8081 | csi-provisioner | shipped `--http-endpoint` |
| 8082 | csi-attacher | shipped `--http-endpoint` |
| 8083 | csi-snapshotter | shipped `--http-endpoint` |
| 8084 | csi-resizer | shipped `--http-endpoint` |

Port 9808 is the liveness probe sidecar's health endpoint and is not a metrics
port.

## Enabling the driver endpoint

Add `metricsAddr` to the `truenas-csi-config` ConfigMap and restart the
controller:

```bash
kubectl -n truenas-csi patch configmap truenas-csi-config \
  --type merge -p '{"data":{"metricsAddr":":8080"}}'
kubectl -n truenas-csi rollout restart deployment/truenas-csi-controller
```

The sidecar ports need no configuration. To scrape any of it, create the metrics
Service, and the ServiceMonitor if you run the Prometheus Operator:

```bash
kubectl apply -f deploy/monitoring/metrics-service.yaml
kubectl apply -f deploy/monitoring/servicemonitor.yaml   # requires the Prometheus Operator CRDs
```

Check it directly:

```bash
kubectl -n truenas-csi port-forward deployment/truenas-csi-controller 8080:8080
curl -s localhost:8080/metrics | grep truenas_csi
```

The equivalent `--metrics-addr` flag exists on the driver binary and takes
precedence over the environment variable. Prefer the ConfigMap in manifests: a
driver image older than this feature exits on an unknown flag, but ignores an
unknown environment variable, so the ConfigMap keeps a newer manifest working
against a pinned older image.

### Node plugin

The node DaemonSet reads a separate `nodeMetricsAddr` key. It is separate because
the node pods run with `hostNetwork: true`, so the address they bind is a port on
every node in the cluster. Pick a port that is free cluster-wide, and scrape it
with a Prometheus node-role job rather than through a Service.

If the port is already taken, the driver logs the bind failure and keeps serving
volumes without metrics. A monitoring endpoint never blocks storage operations.

## Metrics

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `truenas_csi_grpc_requests_total` | counter | `method`, `code` | CSI gRPC requests served, by method and resulting gRPC status code |
| `truenas_csi_grpc_request_duration_seconds` | histogram | `method` | Time taken to serve each CSI gRPC request |
| `truenas_csi_volume_operations_total` | counter | `protocol`, `operation`, `status` | Volume operations attempted, by storage protocol and outcome |
| `truenas_csi_api_calls_total` | counter | `method`, `status` | Calls made to the TrueNAS API, by JSON-RPC method and outcome |
| `truenas_csi_api_call_duration_seconds` | histogram | `method` | Time each TrueNAS API call took, including any wait for a reconnect |
| `truenas_csi_api_connected` | gauge | none | Whether the TrueNAS API WebSocket is connected (1) or not (0) |
| `truenas_csi_api_reconnects_total` | counter | none | Successful reconnections to the TrueNAS API after a failed or lost connection |

Label values:

- `method` on the gRPC metrics is the full method name, for example
  `/csi.v1.Controller/CreateVolume`; on the API metrics it is the JSON-RPC
  method, for example `pool.dataset.create`.
- `code` is the gRPC status name: `OK`, `NotFound`, `Aborted`, and so on. A
  steady trickle of `Aborted` is normal, since that is how CSI reports an
  operation that is already in progress.
- `protocol` is `nfs`, `iscsi`, or `nvmeof`. Deleting a volume TrueNAS no longer
  has yields `unknown`, because the protocol is read from the dataset.
- `operation` is `create`, `delete`, `expand`, or `snapshot`. Snapshot deletion
  is absent: a snapshot ID does not say which protocol backs its volume, and
  resolving that would cost an API call on every delete. Its outcome is still in
  `truenas_csi_grpc_requests_total`.
- `status` is `success` or `error`. The gRPC metrics already carry the exact
  status code, so it is not repeated here.

Requests rejected before a protocol is resolved, such as a StorageClass with
invalid parameters, appear only in `truenas_csi_grpc_requests_total`.

Standard Go runtime and process collectors (`go_*`, `process_*`) are exposed
alongside these. Histogram buckets run from 10ms to 300s, because volume
creation and deletion wait on ZFS and can legitimately take minutes.

### Which metric answers which question

The three layers overlap on purpose, and the differences are where the useful
diagnosis lives:

- `truenas_csi_grpc_*` is what Kubernetes asked the driver to do and what it
  answered.
- `truenas_csi_api_*` is what the driver asked TrueNAS to do. A CreateVolume that
  is slow but succeeds shows up here and nowhere else, which is the usual shape
  of a PVC that takes minutes to bind.
- `csi_sidecar_operations_seconds` is what the sidecars asked the driver to do,
  so it also counts calls that never reached the driver at all.

## Example queries

Provisioning error rate, by method:

```promql
sum by (method) (rate(truenas_csi_grpc_requests_total{code!="OK"}[5m]))
```

95th percentile CreateVolume latency:

```promql
histogram_quantile(0.95, sum by (le) (
  rate(truenas_csi_grpc_request_duration_seconds_bucket{method="/csi.v1.Controller/CreateVolume"}[10m])
))
```

TrueNAS API currently unreachable:

```promql
truenas_csi_api_connected == 0
```

Connection flapping over the last hour:

```promql
increase(truenas_csi_api_reconnects_total[1h]) > 3
```

Failed CSI operations as the sidecars saw them, which includes calls that never
reached the driver:

```promql
sum by (method_name, grpc_status_code) (
  rate(csi_sidecar_operations_seconds_count{grpc_status_code!="OK"}[5m])
)
```

Volume operation failures broken down by protocol, to tell a broken iSCSI portal
from a broken appliance:

```promql
sum by (protocol, operation) (
  rate(truenas_csi_volume_operations_total{status="error"}[15m])
)
```

Slowest TrueNAS API calls, the usual reason a PVC is slow rather than failed:

```promql
topk(5, histogram_quantile(0.95, sum by (method, le) (
  rate(truenas_csi_api_call_duration_seconds_bucket[10m])
)))
```

## Grafana dashboard

`deploy/monitoring/grafana-dashboard.json` covers all of the above plus the
sidecar metrics: connection state, CSI call and failure rates, p95 latency for
both CSI and TrueNAS API calls, and volume operations by protocol. It has a data
source picker and a namespace filter, so it needs no editing before import.

Import it through the Grafana UI (Dashboards, New, Import, Upload JSON file), or
hand it to the Grafana sidecar that kube-prometheus-stack runs:

```bash
kubectl -n monitoring create configmap truenas-csi-dashboard \
  --from-file=truenas-csi.json=deploy/monitoring/grafana-dashboard.json
kubectl -n monitoring label configmap truenas-csi-dashboard grafana_dashboard=1
```

Check the sidecar's own label and namespace first; `grafana_dashboard=1` in the
monitoring namespace is the kube-prometheus-stack default, not a rule.

## Metrics from other components

Several things worth monitoring are already published elsewhere and need no
driver configuration:

- **Per-volume usage** comes from kubelet, which calls the driver's
  `NodeGetVolumeStats`: `kubelet_volume_stats_capacity_bytes`,
  `kubelet_volume_stats_available_bytes`, `kubelet_volume_stats_used_bytes`, and
  the matching `_inodes*` series. These cover filesystem volumes; raw block
  volumes have no filesystem to measure.
- **PVC and PV state** comes from kube-state-metrics
  (`kube_persistentvolumeclaim_*`, `kube_persistentvolume_*`), including
  requested and bound capacity by StorageClass.
- **CSI sidecar operations** come from the sidecars themselves on ports
  8081-8084, chiefly `csi_sidecar_operations_seconds`, which records the
  duration and gRPC status of every call each sidecar made to the driver. It
  overlaps with `truenas_csi_grpc_*` but measures from the caller's side, so it
  also captures calls that never reached the driver.

  These flags travel with the sidecar image versions the manifest pins. If you
  override a sidecar image with one older than `--http-endpoint` support, remove
  the flag as well or that container will fail to start.

## Stability

Metric names are treated as an interface. New metrics may be added in later
releases; the names and labels above will not be renamed or repurposed.
