# TrueNAS CSI Driver - Configuration Reference

This document provides a complete reference for configuring the TrueNAS CSI Driver on OpenShift.

## TrueNASCSI Custom Resource

The `TrueNASCSI` custom resource configures the CSI driver deployment.

### Full Specification

```yaml
apiVersion: csi.truenas.io/v1alpha1
kind: TrueNASCSI
metadata:
  name: truenas
spec:
  # Required: TrueNAS WebSocket API URL
  truenasURL: "wss://truenas.example.com/api/current"

  # Required: Name of the Secret containing the API key
  credentialsSecret: "truenas-api-credentials"

  # Required: Default ZFS pool for volume provisioning
  defaultPool: "tank"

  # Optional: NFS server IP address (required for NFS volumes)
  nfsServer: "192.168.1.100"

  # Optional: iSCSI portal address (required for iSCSI volumes)
  iscsiPortal: "192.168.1.100:3260"

  # Optional: Base IQN for iSCSI targets
  iscsiIQNBase: "iqn.2005-10.org.freenas.ctl"

  # Optional: Skip TLS certificate verification
  insecureSkipTLS: false

  # Optional: Custom driver image
  driverImage: "quay.io/truenas_solutions/truenas-csi:v0.1.0"

  # Optional: Number of controller replicas (default: 1)
  controllerReplicas: 1

  # Optional: Node selector for CSI pods
  nodeSelector:
    node-role.kubernetes.io/worker: ""

  # Optional: Tolerations for CSI pods
  tolerations:
    - key: "node.kubernetes.io/not-ready"
      operator: "Exists"
      effect: "NoExecute"
      tolerationSeconds: 300
```

### Spec Fields

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `truenasURL` | string | Yes | - | WebSocket URL to TrueNAS API |
| `credentialsSecret` | string | Yes | - | Name of Secret with API key |
| `defaultPool` | string | Yes | - | Default ZFS pool name |
| `nfsServer` | string | No | - | NFS server IP address |
| `iscsiPortal` | string | No | - | iSCSI portal (IP:port) |
| `iscsiIQNBase` | string | No | `iqn.2005-10.org.freenas.ctl` | Base IQN for targets |
| `insecureSkipTLS` | bool | No | `false` | Skip TLS verification |
| `driverImage` | string | No | Operator default | Custom driver image |
| `controllerReplicas` | int32 | No | `1` | Controller pod replicas |
| `nodeSelector` | map | No | - | Node selector labels |
| `tolerations` | array | No | - | Pod tolerations |
| `managementState` | string | No | `Managed` | `Managed`, `Unmanaged` or `Removed`; see below |

Editing the TrueNASCSI resource, or rotating the API key in its credentials
Secret, rolls the controller and node pods so they pick up the change. The
operator tracks the settings the pods read through a hash on their templates, so
re-applying an unchanged resource restarts nothing.

### Management State

- **Managed**: the operator deploys the driver and keeps it as declared. Every
  object it creates is owned by the TrueNASCSI resource, so an object that is
  deleted or edited is put back right away. The namespace and your credentials
  Secret are not owned, and a CSIDriver object that already exists keeps its spec.
- **Unmanaged**: the operator leaves the deployed objects alone, so they can be
  changed by hand. Deleting the resource still removes the driver.
- **Removed**: the operator deletes the driver but keeps the resource, and
  switching back to `Managed` deploys it again. Volumes already mounted stay
  mounted, but nothing can be provisioned, attached, mounted or unmounted while the
  driver is removed.

Only one TrueNASCSI resource can run the driver. A second one reports `Failed`
with an error naming the resource that owns it, and deleting the second one
leaves the first one's driver running.

### Status Fields

The operator updates the status subresource with deployment information:

```yaml
status:
  phase: Running          # Current phase: Pending, Running, Failed, Removed
  controllerReady: true   # Controller deployment ready
  nodeDaemonSetReady: true # Node DaemonSet ready
  conditions:
    - type: Ready
      status: "True"
      reason: DeploymentReady
      message: "CSI driver is ready"
      lastTransitionTime: "2026-01-20T00:00:00Z"
```

## API Credentials Secret

Create a Secret containing your TrueNAS API key:

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: truenas-api-credentials
  namespace: truenas-csi
type: Opaque
stringData:
  api-key: "1-abcdef1234567890"
```

### Generating an API Key

1. Log in to TrueNAS web interface
2. Navigate to **Settings** > **API Keys**
3. Click **Add** to create a new key
4. Give it a descriptive name (e.g., "OpenShift CSI")
5. Copy the generated key (shown only once)

**Required Permissions**: The API key needs permissions to:
- Create/delete datasets
- Create/delete NFS shares
- Create/delete iSCSI targets, extents, and target-extent associations
- Create/delete ZFS snapshots

## StorageClass Configuration

### NFS StorageClass

```yaml
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: truenas-nfs
provisioner: csi.truenas.io
parameters:
  # Required
  protocol: nfs
  pool: tank

  # Optional: Dataset path within pool
  datasetPath: "kubernetes/volumes"

  # Optional: ZFS compression (off, lz4, gzip, zstd, etc.)
  compression: "lz4"

  # Optional: Sync mode (standard, always, disabled)
  sync: "standard"

  # Optional: Record size (default: 128K)
  recordsize: "128K"

  # Optional: Access time updates (on, off)
  atime: "off"

  # Optional: NFS export options
  nfsExportOptions: "sec=sys,rw,no_root_squash"

reclaimPolicy: Delete
allowVolumeExpansion: true
volumeBindingMode: Immediate
mountOptions:
  - nfsvers=4.1
  - hard
  - intr
```

### iSCSI StorageClass

```yaml
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: truenas-iscsi
provisioner: csi.truenas.io
parameters:
  # Required
  protocol: iscsi
  pool: tank

  # Required: Filesystem type
  fsType: ext4  # or xfs

  # Optional: Dataset path within pool
  datasetPath: "kubernetes/volumes"

  # Optional: ZFS compression
  compression: "lz4"

  # Optional: Volume block size
  volblocksize: "16K"

  # Optional: Sparse volumes (thin provisioning)
  sparse: "true"

reclaimPolicy: Delete
allowVolumeExpansion: true
volumeBindingMode: Immediate
```

### StorageClass Parameters Reference

| Parameter | Protocols | Values | Description |
|-----------|-----------|--------|-------------|
| `protocol` | all | `nfs`, `iscsi` | Storage protocol |
| `pool` | all | string | ZFS pool name |
| `datasetPath` | all | string | Path within pool |
| `compression` | all | `off`, `lz4`, `gzip`, `zstd` | ZFS compression |
| `sync` | all | `standard`, `always`, `disabled` | Sync behavior |
| `fsType` | iscsi | `ext4`, `xfs` | Filesystem type |
| `recordsize` | nfs | `4K`-`1M` | NFS record size |
| `volblocksize` | iscsi | `512`-`128K` | iSCSI block size |
| `sparse` | iscsi | `true`, `false` | Thin provisioning |
| `atime` | all | `on`, `off` | Access time updates |

## VolumeSnapshotClass Configuration

```yaml
apiVersion: snapshot.storage.k8s.io/v1
kind: VolumeSnapshotClass
metadata:
  name: truenas-snapclass
driver: csi.truenas.io
deletionPolicy: Delete
parameters:
  # Optional: Snapshot naming prefix
  snapshotPrefix: "k8s-snap"
```

## Network Requirements

### Firewall Rules

Ensure the following connectivity between OpenShift nodes and TrueNAS:

| Protocol | Port | Direction | Purpose |
|----------|------|-----------|---------|
| TCP | 443 | Nodes -> TrueNAS | WebSocket API |
| TCP | 2049 | Nodes -> TrueNAS | NFS |
| TCP | 3260 | Nodes -> TrueNAS | iSCSI |

### TLS Configuration

For production environments, use valid TLS certificates:

1. **TrueNAS with valid certificate**: Set `insecureSkipTLS: false`
2. **TrueNAS with self-signed certificate**: Set `insecureSkipTLS: true` (not recommended for production)

## Resource Tuning

### Controller Resources

The controller handles volume lifecycle operations. Default resources:

```yaml
resources:
  requests:
    cpu: 10m
    memory: 64Mi
  limits:
    cpu: 500m
    memory: 256Mi
```

### Node Resources

Node pods handle mount operations. Default resources:

```yaml
resources:
  requests:
    cpu: 10m
    memory: 64Mi
  limits:
    cpu: 500m
    memory: 256Mi
```

## Troubleshooting Configuration Issues

### Verify TrueNAS Connectivity

```bash
# Test WebSocket connection from a pod
oc run test-ws --rm -it --image=curlimages/curl -- \
  curl -k wss://your-truenas-ip/api/current
```

### Check API Key

```bash
# Verify secret exists and has correct key
oc get secret truenas-api-credentials -n truenas-csi -o yaml
```

### Validate StorageClass

```bash
# Check StorageClass configuration
oc describe storageclass truenas-nfs
```

### Test Volume Provisioning

```bash
# Create a test PVC
cat <<EOF | oc apply -f -
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: test-pvc
spec:
  accessModes:
    - ReadWriteOnce
  storageClassName: truenas-iscsi
  resources:
    requests:
      storage: 1Gi
EOF

# Check PVC status
oc get pvc test-pvc

# Check events for errors
oc describe pvc test-pvc
```
