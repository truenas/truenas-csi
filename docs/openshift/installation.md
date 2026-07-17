# TrueNAS CSI Driver - OpenShift Installation Guide

This guide covers installing the TrueNAS CSI Driver on Red Hat OpenShift using the Operator.

## Prerequisites

- OpenShift 4.20 or later
- TrueNAS SCALE 25.10.0+
- TrueNAS API key with appropriate permissions
- Network connectivity between OpenShift nodes and TrueNAS

## Installation via OperatorHub

### Step 1: Install the Operator

1. Log in to the OpenShift web console
2. Navigate to **Operators** → **OperatorHub**
3. Search for "TrueNAS CSI"
4. Click on the **TrueNAS CSI Driver** tile
5. Click **Install**
6. Select the installation options:
   - **Update channel**: stable
   - **Installation mode**: All namespaces on the cluster
   - **Installed Namespace**: openshift-operators
7. Click **Install**

Wait for the operator to install. The status should show "Succeeded".

### Step 2: Create the Driver Namespace

The credentials secret (next step) lives in the namespace where the driver
components run, so create it first. It must match `spec.namespace` in the
TrueNASCSI resource (default `truenas-csi`). The operator also reconciles this
namespace, but the secret is applied before the resource exists, so it must be
created up front:

```bash
oc new-project truenas-csi
```

### Step 3: Create API Credentials Secret

Create a secret containing your TrueNAS API key:

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: truenas-api-credentials
  namespace: truenas-csi
type: Opaque
stringData:
  api-key: "YOUR-TRUENAS-API-KEY"
```

Apply the secret:

```bash
oc apply -f truenas-api-credentials.yaml
```

### Step 4: Create TrueNASCSI Resource

Create a TrueNASCSI custom resource to deploy the CSI driver:

```yaml
apiVersion: csi.truenas.io/v1alpha1
kind: TrueNASCSI
metadata:
  name: truenas
spec:
  # Required: TrueNAS WebSocket API URL
  truenasURL: "wss://your-truenas-ip/api/current"

  # Required: Name of the secret containing the API key
  credentialsSecret: "truenas-api-credentials"

  # Required: Default ZFS pool for volume provisioning
  defaultPool: "tank"

  # Optional: NFS server IP (required for NFS volumes)
  nfsServer: "your-truenas-ip"

  # Optional: iSCSI portal address (required for iSCSI volumes)
  iscsiPortal: "your-truenas-ip:3260"

  # Optional: Skip TLS verification (for self-signed certs)
  insecureSkipTLS: false
```

Apply the resource:

```bash
oc apply -f truenas-csi.yaml
```

### Step 5: Verify Installation

Check the status of the TrueNASCSI resource:

```bash
oc get truenascsi truenas -o yaml
```

Verify that controller and node pods are running:

```bash
oc get pods -n truenas-csi
```

You should see:
- `truenas-csi-controller-*` - Controller deployment pod
- `truenas-csi-node-*` - Node DaemonSet pods (one per node)

## Creating StorageClasses

### NFS StorageClass

```yaml
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: truenas-nfs
provisioner: csi.truenas.io
parameters:
  protocol: nfs
  pool: tank
  # Optional parameters
  compression: "lz4"
  sync: "standard"
reclaimPolicy: Delete
allowVolumeExpansion: true
volumeBindingMode: Immediate
```

### iSCSI StorageClass

```yaml
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: truenas-iscsi
provisioner: csi.truenas.io
parameters:
  protocol: iscsi
  pool: tank
  fsType: ext4
  # Optional parameters
  compression: "lz4"
reclaimPolicy: Delete
allowVolumeExpansion: true
volumeBindingMode: Immediate
```

## Creating Persistent Volume Claims

```yaml
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: my-pvc
spec:
  accessModes:
    - ReadWriteOnce  # Use ReadWriteMany for NFS
  storageClassName: truenas-iscsi  # or truenas-nfs
  resources:
    requests:
      storage: 10Gi
```

## Uninstallation

### Step 1: Delete TrueNASCSI Resource

```bash
oc delete truenascsi truenas
```

### Step 2: Uninstall the Operator

1. Navigate to **Operators** → **Installed Operators**
2. Find "TrueNAS CSI Driver"
3. Click the three dots menu → **Uninstall Operator**

### Step 3: Clean Up (Optional)

Delete the namespace and secrets:

```bash
oc delete namespace truenas-csi
```

## Interactive Demo

An interactive demo script is provided to test the CSI driver on OpenShift:

```bash
# Set environment variables (optional - script will prompt if not set)
export TRUENAS_IP=192.168.1.100
export TRUENAS_API_KEY=1-abcdef1234567890
export TRUENAS_POOL=tank
export TRUENAS_INSECURE=true

# Run the demo
./demo-openshift.sh
```

The demo provides an interactive menu to:
- Create NFS and iSCSI volumes
- Test volume expansion
- Create and restore snapshots
- Clone volumes
- View driver status and logs

## Troubleshooting

### Check Operator Logs

```bash
oc logs -n openshift-operators deployment/truenas-csi-operator-controller-manager
```

### Check CSI Controller Logs

```bash
oc logs -n truenas-csi deployment/truenas-csi-controller -c csi-controller
```

### Check CSI Node Logs

```bash
oc logs -n truenas-csi daemonset/truenas-csi-node -c csi-node
```

### Common Issues

1. **Connection refused to TrueNAS**: Verify the `truenasURL` is correct and the API is accessible from the cluster.

2. **Authentication failed**: Check that the API key in the secret is correct and has the required permissions.

3. **Volume provisioning fails**: Check the TrueNAS logs and ensure the specified pool exists with sufficient space.

4. **iSCSI volumes not mounting**: Ensure iSCSI initiator is installed on all nodes and the portal address is correct.

## Support

For issues and feature requests, please visit:
- GitHub: https://github.com/truenas/truenas-csi
- TrueNAS Documentation: https://www.truenas.com/docs/
