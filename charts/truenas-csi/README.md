# truenas-csi

CSI driver for TrueNAS SCALE: NFS (RWX), iSCSI and NVMe-oF/TCP (RWO/RWX block),
with snapshots, clones, volume expansion, ZFS compression and encryption.

## Requirements

- Kubernetes 1.20 or later
- TrueNAS SCALE 25.10.0 or later, with an API key
- For iSCSI: `open-iscsi` installed on the nodes
- For snapshots: the external snapshot controller and its CRDs

## Install

```bash
helm repo add truenas-csi https://raw.githubusercontent.com/truenas/truenas-csi/master/charts
helm install truenas-csi truenas-csi/truenas-csi \
  --namespace truenas-csi --create-namespace \
  --set truenas.url=wss://10.0.0.100 \
  --set truenas.apiKey=YOUR-API-KEY \
  --set truenas.defaultPool=tank \
  --set truenas.insecureSkipTLS=true
```

## Values

### TrueNAS connection

| Key | Default | Description |
|-----|---------|-------------|
| `truenas.url` | `""` | **Required.** WebSocket URL of the TrueNAS API, for example `wss://10.0.0.100` |
| `truenas.defaultPool` | `""` | **Required.** Default ZFS pool for volumes |
| `truenas.apiKey` | `""` | API key. Required unless `truenas.existingSecret` is set |
| `truenas.existingSecret` | `""` | Name of a Secret you manage instead, for example one produced by ExternalSecrets or Sealed Secrets |
| `truenas.existingSecretKey` | `api-key` | Key inside that Secret holding the API key |
| `truenas.nfsServer` | `""` | NFS server address. Derived from `truenas.url` when empty |
| `truenas.iscsiPortal` | `""` | iSCSI portal `host:port`. Derived from `truenas.url` when empty |
| `truenas.nvmeofPortal` | `""` | NVMe-oF portal `host:port`. Derived from `truenas.url` when empty |
| `truenas.iscsiIQNBase` | `""` | Base IQN for iSCSI targets. The appliance's own basename wins when they differ |
| `truenas.insecureSkipTLS` | `false` | Skip TLS verification. Needed for the certificate TrueNAS self-signs |

### Deployment

| Key | Default | Description |
|-----|---------|-------------|
| `driverName` | `csi.truenas.io` | CSI driver name. Also the provisioner name and part of the kubelet socket path |
| `kubeletDir` | `/var/lib/kubelet` | Kubelet root directory. MicroK8s uses `/var/snap/microk8s/common/var/lib/kubelet` |
| `namespace.create` | `false` | Create the namespace. Usually left to `helm --create-namespace` |
| `nameOverride` / `fullnameOverride` | `""` | Override the name objects are prefixed with |
| `logLevel` | `4` | Driver log verbosity |
| `healthPort` | `9808` | Port the liveness probe sidecar serves `/healthz` on |
| `rbac.create` | `true` | Create the ClusterRoles and bindings |
| `serviceAccount.create` | `true` | Create the ServiceAccounts |
| `serviceAccount.controller` / `.node` | `""` | Use existing ServiceAccounts instead |
| `imagePullSecrets` | `[]` | Pull secrets for all images |

### Images

`image.driver` plus `image.csiProvisioner`, `image.csiAttacher`,
`image.csiSnapshotter`, `image.csiResizer`, `image.nodeDriverRegistrar` and
`image.livenessProbe`, each with `repository`, `tag` and `pullPolicy`. The driver
tag defaults to `v<appVersion>`; the sidecar tags are pinned in `values.yaml`.

### Controller and node

| Key | Default | Description |
|-----|---------|-------------|
| `controller.replicas` | `1` | Controller replicas. Leader election is enabled, so more than one is safe |
| `controller.timeout` | `60s` | Sidecar CSI call timeout. Volume creation waits on ZFS |
| `controller.defaultFSType` | `ext4` | Filesystem used when a StorageClass names none |
| `controller.resources` / `node.resources` | 128Mi/100m, limit 256Mi/200m | Resource requests and limits |
| `controller.nodeSelector` / `.tolerations` / `.affinity` | `{}` / `[]` / `{}` | Controller scheduling |
| `controller.podAnnotations` / `.podLabels` | `{}` | Extra controller pod metadata |
| `controller.priorityClassName` | `""` | Controller priority class |
| `node.nodeSelector` | `{}` | Node plugin scheduling |
| `node.tolerations` | `[{operator: Exists}]` | Runs on every node by default |
| `node.podAnnotations` / `.podLabels` | `{}` | Extra node pod metadata |
| `node.priorityClassName` | `system-node-critical` | Node plugin priority class |

### Metrics

See [docs/metrics.md](../../docs/metrics.md) for the metric reference and a
Grafana dashboard.

| Key | Default | Description |
|-----|---------|-------------|
| `metrics.enabled` | `false` | Serve `/metrics` from the controller and create the Service |
| `metrics.port` | `8080` | Controller metrics port |
| `metrics.node.enabled` | `false` | Serve `/metrics` from the node plugin. These pods use hostNetwork, so the port is bound on every node |
| `metrics.node.port` | `8080` | Node metrics port |
| `metrics.service.annotations` | `{}` | Annotations on the metrics Service |
| `metrics.serviceMonitor.enabled` | `false` | Create a ServiceMonitor. Requires the Prometheus Operator CRDs |
| `metrics.serviceMonitor.interval` | `30s` | Scrape interval |
| `metrics.serviceMonitor.scrapeTimeout` | `""` | Scrape timeout |
| `metrics.serviceMonitor.labels` | `{}` | Labels your Prometheus selects ServiceMonitors by, for example `release: kube-prometheus-stack` |
| `metrics.sidecars.enabled` | `true` | CSI sidecar metrics (`csi_sidecar_operations_seconds`) |
| `metrics.sidecars.provisionerPort` … `resizerPort` | `8081`-`8084` | One port per sidecar container |

### Storage

`storageClasses` is a list. `parameters` is passed to the driver verbatim; the
full parameter reference is in the [project README](../../README.md).

```yaml
storageClasses:
  - name: truenas-nfs
    isDefault: true
    parameters:
      protocol: nfs
      compression: LZ4
  - name: truenas-iscsi
    allowVolumeExpansion: true
    parameters:
      protocol: iscsi
      fsType: ext4

volumeSnapshotClass:
  enabled: true
  deletionPolicy: Delete
```

Per entry: `name`, `parameters`, `isDefault` (false), `reclaimPolicy` (Delete),
`volumeBindingMode` (Immediate), `allowVolumeExpansion` (true), `mountOptions`,
`annotations`.

### Extra objects

`extraObjects` installs additional manifests with the release, each rendered as
a template. Useful for the Secret machinery that feeds
`truenas.existingSecret`:

```yaml
truenas:
  existingSecret: truenas-api-credentials

extraObjects:
  - apiVersion: external-secrets.io/v1beta1
    kind: ExternalSecret
    metadata:
      name: truenas-api-credentials
      namespace: "{{ .Release.Namespace }}"
    spec:
      secretStoreRef:
        name: vault
        kind: ClusterSecretStore
      target:
        name: truenas-api-credentials
      data:
        - secretKey: api-key
          remoteRef:
            key: truenas/csi
            property: api-key
```

## Running two instances

To drive two appliances from one cluster, give each release its own driver name
and object prefix, and supply the API keys as separate Secrets:

```bash
helm install truenas-a ./charts/truenas-csi -n truenas-a --create-namespace \
  --set driverName=a.csi.truenas.io --set fullnameOverride=truenas-csi-a \
  --set truenas.existingSecret=truenas-a-credentials ...
```

`driverName` feeds the CSIDriver object, the StorageClass provisioner and the
kubelet socket path, so the three stay consistent automatically.

## Upgrading from deploy/truenas-csi-driver.yaml

The chart uses the same object names and the same `app` pod selector labels as
the flat manifest, so `helm upgrade --install` can adopt an existing install
in place. Two caveats:

- Helm needs to own the objects. Adopt them by adding the
  `meta.helm.sh/release-name` and `meta.helm.sh/release-namespace` annotations
  and the `app.kubernetes.io/managed-by=Helm` label before upgrading, or delete
  the workloads first. Deleting the Deployment and DaemonSet does not touch
  existing volumes or data.
- The flat manifest tracks the floating `:latest` driver tag, while the chart
  pins `v<appVersion>`. Set `image.driver.tag` if you want the old behaviour.
