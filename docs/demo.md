# TrueNAS CSI Driver - Demo Guide

This guide will help you run the TrueNAS CSI driver demo on a local Kubernetes cluster.

## Overview

The `demo-simple.sh` script provides an interactive demonstration of the TrueNAS CSI driver's capabilities:
- NFS volume provisioning
- iSCSI volume provisioning
- Volume expansion
- Volume cloning
- Volume snapshots
- Multiple volume creation
- Storage class variations

This demo runs entirely on a local Kind (Kubernetes in Docker) cluster and provisions real storage on your TrueNAS system.

## Prerequisites

Before running the demo, ensure you have the following:

### 1. Required Tools

- **Docker**: Required for Kind cluster and building container images
  - Installation: https://docs.docker.com/get-docker/
  - Verify: `docker --version`

- **Kind**: Creates a local Kubernetes cluster for testing
  - Installation: https://kind.sigs.k8s.io/docs/user/quick-start/#installation
  - Verify: `kind --version`

- **kubectl**: Kubernetes command-line tool
  - Installation: https://kubernetes.io/docs/tasks/tools/
  - Verify: `kubectl version --client`

### 2. TrueNAS System

- **Version**: TrueNAS SCALE (tested with v25.10+)
- **Requirements**:
  - Network access from your machine to TrueNAS
  - API access enabled
  - At least one ZFS pool created (e.g., "tank")
  - API key or username/password credentials

## Configuration

### Step 1: Edit the Deployment YAML

Before running the demo, configure your TrueNAS connection details:

```bash
# Edit the deployment file
deploy/truenas-csi-driver.yaml
```

### Step 2: Configure TrueNAS Connection

Update the **ConfigMap** section with your TrueNAS details:

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: truenas-csi-config
  namespace: truenas-csi
data:
  truenasURL: "wss://YOUR_TRUENAS_IP/api/current"    # Use wss:// for secure connection
  truenasInsecure: "true"                            # Set to "true" for self-signed certs
  defaultPool: "tank"                                # Change to your pool name
  nfsServer: "YOUR_TRUENAS_IP"                       # Change to your TrueNAS IP
  iscsiPortal: "YOUR_TRUENAS_IP:3260"                # Change to your TrueNAS IP
  iscsiIQNBase: "iqn.2000-01.io.truenas"             # Optional: customize for your org
```

**Example:**
```yaml
data:
  truenasURL: "wss://10.0.0.136/api/current"          # Secure WebSocket (wss://)
  truenasInsecure: "true"                             # Allow self-signed certificate
  defaultPool: "tank"
  nfsServer: "10.0.0.136"
  iscsiPortal: "10.0.0.136:3260"
  iscsiIQNBase: "iqn.2024-01.com.acmecorp"  # Optional: use your company domain
```

**Note:** TrueNAS requires API keys to be used over secure connections (wss://) for security. Set `truenasInsecure: "true"` if using self-signed certificates.

### Step 3: Configure API Key Authentication

1. **Get your API key** from TrueNAS:
   - Log into TrueNAS web UI
   - Click your profile icon → API Keys
   - Click Add → Create new key
   - Copy the generated key

2. **Update the Secret** in `deploy/truenas-csi-driver.yaml`:
```yaml
apiVersion: v1
kind: Secret
metadata:
  name: truenas-api-credentials
  namespace: truenas-csi
type: Opaque
stringData:
  api-key: "1-YOUR_ACTUAL_API_KEY_HERE"
```

## Running the Demo

Once you've configured the YAML file:

```bash
chmod +x ./demo-simple.sh
./demo-simple.sh
```

### First Run

The script will:
1. ✅ Verify prerequisites are installed
2. ✅ Confirm you've configured the YAML file
3. ✅ Create a Kind cluster with 2 worker nodes
4. ✅ Build the CSI driver Docker image
5. ✅ Load the image into the cluster
6. ✅ Deploy the driver using your configuration
7. ✅ Set up snapshot support
8. ✅ Create StorageClasses for NFS and iSCSI
9. ✅ Launch the interactive demo menu

### Subsequent Runs

If the cluster and driver already exist, the script will:
- Skip cluster creation
- Skip driver deployment
- Go directly to the demo menu

## Demo Menu Options

The interactive menu provides these demos:

### Volume Provisioning
1. **Demo NFS volume creation** - Create a ReadWriteMany NFS volume
2. **Demo iSCSI volume creation** - Create a ReadWriteOnce iSCSI block volume
3. **Demo multiple volumes** - Create 3 volumes simultaneously
4. **Demo storage class variations** - Different compression settings

### Advanced Operations
5. **Demo volume expansion** - Expand an existing volume online
6. **Demo volume cloning** - Clone a volume (select existing or create new)
7. **Demo volume snapshots** - Create and restore from snapshots
8. **Demo clone with data verification** ⭐ - Write data, clone, verify data copied

### Inspection & Metadata
9. **Demo volume metadata inspection** - View volume attributes
10. **Demo capacity reporting** - Check volume sizes
11. **Demo topology awareness** - View node topology
12. **Demo driver capabilities** - List all CSI features

### Utilities
13. **Show current status** - View all pods, PVCs, PVs
14. **View driver logs** - Check controller and node logs
15. **Cleanup demo resources** - Delete all demo volumes

## What to Expect

### Successful Volume Creation

When you create a volume, you should see:
- ✅ PVC bound within 10-30 seconds
- ✅ New dataset appears in TrueNAS UI (Storage → Datasets)
- ✅ New share appears in TrueNAS UI (Shares → NFS or iSCSI)

### Check TrueNAS UI

After creating volumes, verify in TrueNAS:
- **Storage → Datasets** - See the new datasets (pvc-xxxxx)
- **Shares → NFS** - See NFS shares for NFS volumes
- **Shares → iSCSI** - See targets/extents for iSCSI volumes

## Advanced Configuration

### Custom IQN Prefix (Multi-Tenant)

For enterprise/multi-tenant deployments, customize IQN prefixes per StorageClass:

```yaml
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: finance-iscsi
parameters:
  protocol: "iscsi"
  iscsi.iqn-base: "iqn.2024-01.com.acme.finance"
---
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: engineering-iscsi
parameters:
  protocol: "iscsi"
  iscsi.iqn-base: "iqn.2024-01.com.acme.engineering"
```

### Self-Signed Certificates

The driver uses secure WebSocket (wss://) by default. For self-signed certificates:

```yaml
data:
  truenasURL: "wss://10.0.0.136/api/current"
  truenasInsecure: "true"  # Skip certificate validation for self-signed certs
```

**For production with valid certificates**, set to `"false"` or remove the field:
```yaml
data:
  truenasURL: "wss://truenas.example.com/api/current"
  truenasInsecure: "false"  # Validate certificate
```

## Troubleshooting

### "Have you configured deploy/truenas-csi-driver.yaml?"

The demo requires you to edit the YAML file first. If you see this message:
1. Edit `deploy/truenas-csi-driver.yaml`
2. Update ConfigMap with your TrueNAS IP and pool
3. Update Secret with your credentials
4. Run the demo again

### "Failed to create driver: invalid iSCSI IQN base format"

Your `iscsiIQNBase` in the ConfigMap has an invalid format. It must:
- Start with `iqn.`
- Include date in `YYYY-MM` format
- Include reversed domain name
- Example: `iqn.2024-01.com.example` 
- Example: `iqn.invalid` 

### "Pool 'tank' not found"

- Verify the pool exists in TrueNAS UI (Storage → Pools)
- Update `defaultPool` in the ConfigMap to match your actual pool name
- Pool names are case-sensitive

### "PVC stuck in Pending"

Check driver logs from the menu (Option 14) or:
```bash
kubectl logs -n truenas-csi -l app=truenas-csi-controller -c csi-controller
```

Common causes:
- TrueNAS not accessible from Kind cluster
- Authentication failure
- Pool doesn't exist
- Network connectivity issues

## Cleanup

### Clean Demo Resources Only
From the menu, choose **Option 15** to delete all demo PVCs while keeping the driver and cluster.

### Clean Everything
```bash
# Delete the entire Kind cluster
kind delete cluster --name truenas-csi-demo
```

This removes the cluster and all associated resources. TrueNAS datasets/shares will be deleted automatically (due to `reclaimPolicy: Delete`).

## Production Deployment

For production use on a real Kubernetes cluster:

1. **Edit the deployment YAML** with your production settings
2. **Update image tag** from `demo` to `latest` or a specific version
3. **Change imagePullPolicy** from `IfNotPresent` to `Always`
4. **Configure proper IQN prefix** for your organization
5. **Use TLS** (wss://) for TrueNAS connection
6. **Deploy**:
   ```bash
   kubectl apply -f deploy/truenas-csi-driver.yaml
   ```

## Getting Help
- **Report issues**: https://github.com/iXsystems/truenas_k8_driver/issues

