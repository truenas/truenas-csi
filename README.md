# TrueNAS CSI Driver

A Container Storage Interface (CSI) driver for [TrueNAS 25.10.0+](https://www.truenas.com/truenas-scale/), enabling dynamic provisioning of persistent volumes in Kubernetes using TrueNAS storage.

## Features

- **NFS volumes** - ReadWriteMany (RWX) access mode for shared storage
- **iSCSI volumes** - Block storage with ReadWriteOnce (RWO) and ReadWriteMany (RWX) access modes (RWX requires cluster filesystem like GFS2/OCFS2)
- **Dynamic provisioning** - Automatic volume creation and deletion
- **Volume expansion** - Online resize of volumes
- **Snapshots and clones** - CSI snapshot support for backup and cloning
- **CHAP authentication** - Secure iSCSI connections
- **ZFS compression** - LZ4, ZSTD, GZIP, and other algorithms
- **ZFS encryption** - Dataset-level encryption with key management
- **Automatic snapshot scheduling** - Periodic snapshots via StorageClass
- **TrueNAS Websocket API** - Uses the modern TrueNAS Websocket API

## Requirements

### TrueNAS
- TrueNAS SCALE 25.10.0+
- API access enabled
- At least one ZFS pool configured

### Kubernetes
- Kubernetes 1.26+
- For snapshots: [snapshot-controller](https://github.com/kubernetes-csi/external-snapshotter) installed

### Node Requirements
- **NFS volumes**: No additional requirements
- **iSCSI volumes**: `open-iscsi` package installed on worker nodes

## Quick Start

1. **Create an API key in TrueNAS**
   - Log into TrueNAS web UI
   - Navigate to your profile → API Keys
   - Create a new API key and copy it

2. **Configure the driver**
   ```bash
   # Edit the deployment manifest
   vi deploy/truenas-csi-driver.yaml
   ```
   Update the ConfigMap with your TrueNAS connection details and the Secret with your API key.

3. **Deploy the driver**
   ```bash
   kubectl apply -f deploy/truenas-csi-driver.yaml
   ```

4. **Create a StorageClass and PVC**
   ```bash
   kubectl apply -f examples/storageclass-nfs.yaml
   kubectl apply -f examples/pvc-nfs.yaml
   ```

## Installation

### Prerequisites

Install the snapshot controller (required for snapshot support):
```bash
kubectl apply -f https://raw.githubusercontent.com/kubernetes-csi/external-snapshotter/master/client/config/crd/snapshot.storage.k8s.io_volumesnapshotclasses.yaml
kubectl apply -f https://raw.githubusercontent.com/kubernetes-csi/external-snapshotter/master/client/config/crd/snapshot.storage.k8s.io_volumesnapshotcontents.yaml
kubectl apply -f https://raw.githubusercontent.com/kubernetes-csi/external-snapshotter/master/client/config/crd/snapshot.storage.k8s.io_volumesnapshots.yaml
kubectl apply -f https://raw.githubusercontent.com/kubernetes-csi/external-snapshotter/master/deploy/kubernetes/snapshot-controller/rbac-snapshot-controller.yaml
kubectl apply -f https://raw.githubusercontent.com/kubernetes-csi/external-snapshotter/master/deploy/kubernetes/snapshot-controller/setup-snapshot-controller.yaml
```

### Deploy the Driver

1. Edit `deploy/truenas-csi-driver.yaml` with your configuration
2. Apply the manifest:
   ```bash
   kubectl apply -f deploy/truenas-csi-driver.yaml
   ```

### Verify Installation

```bash
# Check driver pods are running
kubectl get pods -n truenas-csi

# Verify CSI driver is registered
kubectl get csidrivers
```

## Configuration

### Driver Configuration (ConfigMap)

| Setting | Description | Example |
|---------|-------------|---------|
| `truenasURL` | WebSocket URL to TrueNAS API | `wss://10.0.0.100/api/current` |
| `truenasInsecure` | Skip TLS verification | `true` (for self-signed certs) |
| `defaultPool` | Default ZFS pool for volumes | `tank` |
| `nfsServer` | NFS server address | `10.0.0.100` |
| `iscsiPortal` | iSCSI portal address | `10.0.0.100:3260` |
| `iscsiIQNBase` | Base IQN for iSCSI targets | `iqn.2024-01.com.example` |

### StorageClass Parameters

#### General Parameters

| Parameter | Description | Values |
|-----------|-------------|--------|
| `protocol` | Storage protocol | `nfs`, `iscsi` |
| `pool` | ZFS pool (overrides default) | pool name |
| `compression` | ZFS compression algorithm | `OFF`, `LZ4`, `GZIP`, `ZSTD`, `ZLE`, `LZJB` |
| `sync` | ZFS sync mode | `STANDARD`, `ALWAYS`, `DISABLED` |

#### NFS Parameters

| Parameter | Description | Example |
|-----------|-------------|---------|
| `nfs.hosts` | Allowed hosts | `10.0.0.0/8,192.168.1.0/24` |
| `nfs.networks` | Allowed networks | `10.0.0.0/8` |
| `nfs.mountOptions` | Client mount options | `hard,nfsvers=4.1` |

#### iSCSI Parameters

| Parameter | Description | Values |
|-----------|-------------|--------|
| `volblocksize` | ZVOL block size | `512`, `1K`, `2K`, `4K`, `8K`, `16K`, `32K`, `64K`, `128K` |
| `iscsi.blocksize` | iSCSI logical block size | `512`, `1024`, `2048`, `4096` |
| `iscsi.chapUser` | CHAP username | string |
| `iscsi.chapSecret` | CHAP password (12-16 chars) | string |
| `iscsi.chapPeerUser` | Mutual CHAP peer user | string |
| `iscsi.chapPeerSecret` | Mutual CHAP peer password | string |
| `iscsi.initiators` | Allowed initiator IQNs | comma-separated |
| `iscsi.networks` | Allowed network CIDRs | comma-separated |

#### Snapshot Task Parameters

| Parameter | Description | Values |
|-----------|-------------|--------|
| `snapshot.schedule` | Cron schedule (5 fields) | `0 0 * * *` |
| `snapshot.retention` | Retention period | `1`-`365` |
| `snapshot.retentionUnit` | Retention unit | `HOUR`, `DAY`, `WEEK`, `MONTH`, `YEAR` |
| `snapshot.naming` | Naming schema | `auto-%Y-%m-%d_%H-%M` |
| `snapshot.recursive` | Include child datasets | `true`, `false` |

#### Encryption Parameters

| Parameter | Description | Values |
|-----------|-------------|--------|
| `encryption` | Enable encryption | `true`, `false` |
| `encryption.algorithm` | Encryption algorithm | `AES-256-GCM`, `AES-128-CCM` |
| `encryption.passphrase` | Passphrase (min 8 chars) | string |
| `encryption.key` | Hex-encoded key (64 chars) | string |
| `encryption.generateKey` | Auto-generate key | `true`, `false` |

## Examples

See the [`examples/`](examples/) folder for sample configurations:

- `storageclass-nfs.yaml` - Basic NFS StorageClass
- `storageclass-nfs-compressed.yaml` - NFS with ZSTD compression
- `storageclass-iscsi.yaml` - Basic iSCSI StorageClass
- `storageclass-iscsi-chap.yaml` - iSCSI with CHAP authentication
- `storageclass-encrypted.yaml` - Encrypted storage
- `pvc-nfs.yaml` / `pvc-iscsi.yaml` - PVC examples
- `pod-with-pvc.yaml` - Pod using a PVC
- `volumesnapshotclass.yaml` / `volumesnapshot.yaml` - Snapshot examples

## Building

### Build the binary
```bash
go build -o truenas-csi-driver ./cmd/
```

### Build the container image
```bash
docker build -t truenas-csi-driver:latest .
```

### Run tests
```bash
go test ./...
```

## Running the Demo

For an interactive demonstration of all driver features using a local Kind cluster, see [docs/demo.md](docs/demo.md).


## Contributing

- Report issues: https://github.com/truenas/truenas-csi/issues
- Submit pull requests: https://github.com/truenas/truenas-csi/pulls

## License

GNU General Public License 3.0
