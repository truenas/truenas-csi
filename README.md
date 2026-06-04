# TrueNAS CSI Driver

A Container Storage Interface (CSI) driver for [TrueNAS 25.10.0+](https://www.truenas.com/truenas-scale/), enabling dynamic provisioning of persistent volumes in Kubernetes using TrueNAS storage.

## Features

- **NFS volumes** - ReadWriteMany (RWX) access mode for shared storage
- **iSCSI volumes** - Block storage with ReadWriteOnce (RWO) and ReadWriteMany (RWX) access modes (RWX requires cluster filesystem like GFS2/OCFS2)
- **NVMe-oF/TCP volumes** - Block storage over NVMe over Fabrics (TCP) with optional DH-CHAP authentication
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
- **NVMe-oF volumes**: `nvme_tcp`/`nvme_fabrics` kernel modules available on worker nodes (the node DaemonSet loads them); requires TrueNAS SCALE 25.10+ with the NVMe-oF target service enabled

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

### Non-Standard Kubelet Paths

The default deployment manifest uses `/var/lib/kubelet` as the kubelet root directory. Some Kubernetes distributions use a different path. If your distribution uses a non-standard path, you must update the following in `deploy/truenas-csi-driver.yaml` before deploying:

1. All `hostPath` values containing `/var/lib/kubelet`
2. The `DRIVER_REG_SOCK_PATH` environment variable
3. The `--kubelet-registration-path` argument
4. The `mountPath` for the `kubelet-dir` volume mount on the `csi-node` container

| Distribution | Kubelet Path |
|---|---|
| Standard Kubernetes | `/var/lib/kubelet` (default) |
| MicroK8s | `/var/snap/microk8s/common/var/lib/kubelet` |
| K3s | `/var/lib/rancher/k3s/agent/kubelet` |

> **Important:** The `kubelet-dir` `mountPath` must match the `hostPath`. If they differ, NFS mounts will succeed inside the CSI container but will not propagate to kubelet, causing pods to see local storage instead of NFS.

#### MicroK8s Mount Propagation

MicroK8s runs inside a snap with its own mount namespace. For CSI mount propagation to work, the host root filesystem must have `shared` propagation **before** MicroK8s starts:

```bash
sudo mount --make-rshared /
microk8s start
```

To make this persistent across reboots, create a systemd unit:

```bash
sudo tee /etc/systemd/system/microk8s-mount-propagation.service <<EOF
[Unit]
Description=Ensure shared mount propagation for MicroK8s
Before=snap.microk8s.daemon-containerd.service

[Service]
Type=oneshot
ExecStart=/bin/mount --make-rshared /
RemainAfterExit=yes

[Install]
WantedBy=multi-user.target
EOF

sudo systemctl enable microk8s-mount-propagation
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
| `nvmeofPortal` | NVMe-oF portal address (optional; auto-derived) | `10.0.0.100:4420` |
| `iscsiIQNBase` | Base IQN for iSCSI targets | `iqn.2024-01.com.example` |

### StorageClass Parameters

#### General Parameters

| Parameter | Description | Values |
|-----------|-------------|--------|
| `protocol` | Storage protocol | `nfs`, `iscsi`, `nvmeof` |
| `pool` | ZFS pool (overrides default) | pool name |
| `compression` | ZFS compression algorithm | `OFF`, `LZ4`, `GZIP`, `ZSTD`, `ZLE`, `LZJB` |
| `sync` | ZFS sync mode | `STANDARD`, `ALWAYS`, `DISABLED` |

#### NFS Parameters

| Parameter | Description | Example |
|-----------|-------------|---------|
| `nfs.hosts` | Allowed hosts | `10.0.0.0/8,192.168.1.0/24` |
| `nfs.networks` | Allowed networks | `10.0.0.0/8` |
| `nfs.mountOptions` | Client mount options | `hard,nfsvers=4.1` |
| `nfs.mapAllUser` | NFS user mapping (default: `root`) | `postgres` |
| `nfs.mapAllGroup` | NFS group mapping (default: `wheel`) | `postgres` |

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

#### NVMe-oF Parameters

NVMe-oF also uses the `volblocksize` parameter above. DH-CHAP authentication is optional.

| Parameter | Description | Values |
|-----------|-------------|--------|
| `nvmeof.hostNQN` | Authorized host NQN (required for DH-CHAP) | `nqn.2014-08.org.nvmexpress:uuid:...` |
| `nvmeof.dhchapKey` | DH-CHAP host key | `DHHC-1:00:...` |
| `nvmeof.dhchapCtrlKey` | Mutual DH-CHAP controller key | `DHHC-1:00:...` |
| `nvmeof.dhchapHash` | DH-CHAP hash (default `SHA-256`) | `SHA-256`, `SHA-384`, `SHA-512` |
| `nvmeof.dhchapDHGroup` | DH group | `2048-BIT`, `3072-BIT`, `4096-BIT`, `6144-BIT`, `8192-BIT` |

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
- `storageclass-nvmeof.yaml` - Basic NVMe-oF/TCP StorageClass
- `storageclass-nvmeof-dhchap.yaml` - NVMe-oF with DH-CHAP authentication
- `storageclass-encrypted.yaml` - Encrypted storage
- `pvc-nfs.yaml` / `pvc-iscsi.yaml` / `pvc-nvmeof.yaml` - PVC examples
- `pod-with-pvc.yaml` - Pod using a PVC
- `volumesnapshotclass.yaml` / `volumesnapshot.yaml` - Snapshot examples

## Building

### Build the binary
```bash
make build
```

### Build container images
```bash
# Build Alpine-based image (standard Kubernetes)
make docker-build

# Build UBI-based image (Red Hat OpenShift certification)
make build-ubi
```

### Push to quay.io
```bash
# Login to quay.io
docker login quay.io

# Push UBI image to quay.io/truenas_solutions
make push-ubi

# Push all images (driver, operator, bundle)
make push-all
```

### Run tests
```bash
make test
```

## Container Images

| Image | Description |
|-------|-------------|
| `ghcr.io/truenas/truenas-csi` | CSI driver (Alpine-based, for standard Kubernetes) |
| `quay.io/truenas_solutions/truenas-csi` | CSI driver (UBI-based, for Red Hat OpenShift) |
| `quay.io/truenas_solutions/truenas-csi-operator` | Kubernetes operator |
| `quay.io/truenas_solutions/truenas-csi-operator-bundle` | OLM bundle for OperatorHub |

## Running the Demo

For an interactive demonstration of all driver features using a local Kind cluster, see [docs/demo.md](docs/demo.md).

## OpenShift

The TrueNAS CSI Driver supports Red Hat OpenShift 4.20+ and is designed for OperatorHub distribution.

### Quick Start (OpenShift)

1. **Install via OperatorHub**
   - Navigate to **Operators** > **OperatorHub**
   - Search for "TrueNAS CSI"
   - Click **Install**

2. **Create credentials secret**
   ```yaml
   apiVersion: v1
   kind: Secret
   metadata:
     name: truenas-api-credentials
     namespace: truenas-csi
   stringData:
     api-key: "YOUR-API-KEY"
   ```

3. **Create TrueNASCSI resource**
   ```yaml
   apiVersion: csi.truenas.io/v1alpha1
   kind: TrueNASCSI
   metadata:
     name: truenas
   spec:
     truenasURL: "wss://your-truenas-ip/api/current"
     credentialsSecret: "truenas-api-credentials"
     defaultPool: "tank"
     nfsServer: "your-truenas-ip"
   ```

### OpenShift Documentation

- [Installation Guide](docs/openshift/installation.md) - Detailed installation steps
- [Configuration Reference](docs/openshift/configuration.md) - CRD and StorageClass options
- [Upgrade Guide](docs/openshift/upgrade.md) - Upgrade procedures
- [Red Hat Certification Guide](docs/openshift/certification.md) - Certification process and requirements

## Demo Scripts

Interactive demo scripts are provided to test the CSI driver:

### Standard Kubernetes (Kind)

```bash
# Set TrueNAS connection details in deploy/truenas-csi-driver.yaml, then:
./demo-simple.sh
```

### OpenShift (CRC/OpenShift Local)

```bash
# Set environment variables
export TRUENAS_IP=192.168.1.100
export TRUENAS_API_KEY=your-api-key
export TRUENAS_POOL=tank

# Run the demo
./demo-openshift.sh
```

Both demos provide interactive menus to test NFS/iSCSI provisioning, volume expansion, snapshots, and cloning.

## Contributing

- Report issues: https://github.com/truenas/truenas-csi/issues
- Submit pull requests: https://github.com/truenas/truenas-csi/pulls

## License

GNU General Public License 3.0
