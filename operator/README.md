# TrueNAS CSI Operator

Kubernetes operator for deploying and managing the TrueNAS CSI driver on OpenShift and Kubernetes.

## Overview

The TrueNAS CSI Operator automates the deployment and lifecycle management of the TrueNAS CSI driver. It handles:

- CSI driver deployment (controller and node components)
- RBAC configuration
- NetworkPolicies for security hardening
- Configuration management via ConfigMaps
- Status monitoring and reporting

## Supported Versions

| Component | Minimum Version |
|-----------|-----------------|
| OpenShift | 4.20+ |
| Kubernetes | 1.26+ |
| TrueNAS SCALE | 25.10.0+ |

## Prerequisites

- Go 1.22+
- Docker or Podman
- kubectl configured for your cluster
- operator-sdk v1.42+ (optional, for bundle operations)

## Quick Start

```bash
# Build and run locally (outside cluster)
make build
make run

# Run tests
make test

# Build container image
make docker-build IMG=quay.io/truenas_solutions/truenas-csi-operator:v0.1.0

# Push to registry
make docker-push IMG=quay.io/truenas_solutions/truenas-csi-operator:v0.1.0
```

## Development Workflow

### Building

```bash
# Build binary with version injection
make build

# Build with specific version
VERSION=0.1.0 make build

# Build multi-arch image
make docker-buildx IMG=quay.io/truenas_solutions/truenas-csi-operator:v0.1.0
```

### Cleanup

```bash
# Remove build artifacts and downloaded tools
make clean
```

### Testing

```bash
# Run unit tests (envtest)
make test

# Run E2E tests (requires Kind)
make test-e2e

# Run integration tests against OpenShift/CRC with real TrueNAS
make test-integration TRUENAS_IP=192.168.1.100 TRUENAS_API_KEY=your-key

# Run linter
make lint
```

### Integration Tests (OpenShift/CRC)

Integration tests verify end-to-end volume provisioning against a real TrueNAS instance on an OpenShift cluster (CRC or full OpenShift).

**Prerequisites:**
- CRC or OpenShift cluster running and accessible via `oc`
- Logged in with cluster-admin privileges: `oc login -u kubeadmin ...`
- TrueNAS SCALE 25.10.0+ accessible from cluster nodes
- TrueNAS API key with appropriate permissions
- Container images pushed to quay.io/truenas_solutions and **publicly accessible**

**Image Preparation:**

Before running integration tests, you must build and push the required container images. The operator defaults to using the `latest` tag for the CSI driver image.

```bash
# From the project root directory (not operator/)

# Recommended: Use the release target to build and push everything
make release

# This is equivalent to:
#   make build-all      - Build driver, operator, and bundle images
#   make push-all       - Push all images with version tag
#   make push-latest    - Push all images with 'latest' tag
```

**Important:** The `push-latest` target (included in `make release`) is required because the operator defaults to `quay.io/truenas_solutions/truenas-csi:latest` for the driver image. Without this, pods will fail with `ImagePullBackOff`.

**Automatic Setup:**

The test suite automatically handles the following (no manual steps required):
- Installs VolumeSnapshot CRDs from [kubernetes-csi/external-snapshotter](https://github.com/kubernetes-csi/external-snapshotter)
- Deploys the snapshot controller for snapshot reconciliation
- Applies SecurityContextConstraints for OpenShift
- Deploys and undeploys the operator

**Required environment variables:**
| Variable | Description |
|----------|-------------|
| `TRUENAS_IP` | TrueNAS IP address |
| `TRUENAS_API_KEY` | API key for authentication |

**Optional environment variables:**
| Variable | Default | Description |
|----------|---------|-------------|
| `TRUENAS_POOL` | `tank` | ZFS pool for test volumes |
| `TRUENAS_INSECURE` | `true` | Skip TLS verification |
| `OPERATOR_IMAGE` | `quay.io/truenas_solutions/truenas-csi-operator:v0.1.0` | Operator image to deploy |
| `DRIVER_IMAGE` | `quay.io/truenas_solutions/truenas-csi:latest` | CSI driver image |
| `SKIP_OPERATOR_DEPLOY` | `false` | Skip operator deployment if already deployed |

**Run the tests:**
```bash
# Basic usage (from the operator/ directory)
make test-integration TRUENAS_IP=192.168.1.100 TRUENAS_API_KEY=1-abcdef123456

# With all options
TRUENAS_IP=192.168.1.100 \
TRUENAS_API_KEY=1-abcdef123456 \
TRUENAS_POOL=mypool \
SKIP_OPERATOR_DEPLOY=true \
make test-integration
```

**Complete workflow example:**
```bash
# 1. Start CRC and login
crc start
eval $(crc oc-env)
oc login -u kubeadmin -p $(crc console --credentials | grep kubeadmin | awk -F"'" '{print $2}') https://api.crc.testing:6443

# 2. Build and push images (from project root)
cd /path/to/truenas-csi
make release

# 3. Run integration tests (from operator directory)
cd operator
make test-integration TRUENAS_IP=10.0.0.100 TRUENAS_API_KEY=1-your-api-key TRUENAS_POOL=tank
```

**What the tests cover:**
- NFS volume provisioning and deletion
- iSCSI volume provisioning
- Volume expansion
- Volume snapshots and restore
- Volume cloning
- TrueNASCSI CR status reporting

### Code Generation

```bash
# Regenerate CRDs and RBAC manifests
make manifests

# Regenerate DeepCopy methods
make generate
```

### Bundle Operations (OLM)

```bash
# Generate OLM bundle
make bundle VERSION=0.1.0 IMG=quay.io/truenas_solutions/truenas-csi-operator:v0.1.0

# Build bundle image
make bundle-build BUNDLE_IMG=quay.io/truenas_solutions/truenas-csi-operator-bundle:v0.1.0

# Push bundle
make bundle-push BUNDLE_IMG=quay.io/truenas_solutions/truenas-csi-operator-bundle:v0.1.0

# Validate bundle with scorecard
operator-sdk scorecard bundle/
```

### Deployment

```bash
# Install CRDs
make install

# Deploy operator to cluster
make deploy IMG=quay.io/truenas_solutions/truenas-csi-operator:v0.1.0

# Remove operator
make undeploy
```

## Configuration

### Environment Variables

The operator reads sidecar images from environment variables:

| Variable | Description |
|----------|-------------|
| `PROVISIONER_IMAGE` | CSI provisioner sidecar image |
| `ATTACHER_IMAGE` | CSI attacher sidecar image |
| `SNAPSHOTTER_IMAGE` | CSI snapshotter sidecar image |
| `RESIZER_IMAGE` | CSI resizer sidecar image |
| `NODE_DRIVER_REGISTRAR_IMAGE` | Node driver registrar sidecar image |
| `LIVENESS_PROBE_IMAGE` | Liveness probe sidecar image |

### TrueNASCSI Custom Resource

```yaml
apiVersion: csi.truenas.io/v1alpha1
kind: TrueNASCSI
metadata:
  name: truenas-csi
spec:
  # Required: TrueNAS WebSocket URL
  truenasURL: "wss://truenas.example.com/api/current"

  # Required: Name of secret containing api-key
  credentialsSecret: "truenas-credentials"

  # Required: Default ZFS pool for provisioning
  defaultPool: "tank"

  # Optional: NFS server address (for NFS volumes)
  nfsServer: "192.168.1.100"

  # Optional: iSCSI portal address (for iSCSI volumes)
  iscsiPortal: "192.168.1.100:3260"

  # Optional: Driver image (default: quay.io/truenas_solutions/truenas-csi:latest)
  driverImage: "quay.io/truenas_solutions/truenas-csi:v0.1.0"

  # Optional: Controller replicas (default: 1, max: 3)
  controllerReplicas: 1

  # Optional: Log level 1-5 (default: 4)
  logLevel: 4

  # Optional: Skip TLS verification (default: false)
  insecureSkipTLS: false

  # Optional: Namespace for CSI components (default: truenas-csi)
  namespace: "truenas-csi"

  # Optional: ManagementState (Managed, Unmanaged, Removed)
  managementState: "Managed"
```

## Project Structure

```
operator/
├── api/v1alpha1/           # CRD type definitions
│   ├── truenascsi_types.go # TrueNASCSI spec and status
│   └── groupversion_info.go
├── cmd/                    # Operator entrypoint
│   └── main.go
├── config/                 # Kustomize manifests
│   ├── crd/               # CRD definitions
│   ├── manager/           # Operator deployment
│   ├── rbac/              # RBAC rules
│   ├── network-policy/    # NetworkPolicy for operator
│   └── scorecard/         # Scorecard tests
├── internal/
│   ├── controller/        # Reconciliation logic
│   │   ├── truenascsi_controller.go
│   │   ├── constants.go   # All constants
│   │   ├── errors.go      # Sentinel errors
│   │   ├── validation.go  # Pre-flight validation
│   │   ├── helpers.go     # Helper functions
│   │   └── volumes.go     # Volume definitions
│   └── version/           # Version info (ldflags)
├── bundle/                # OLM bundle (generated)
└── test/                  # E2E tests
```

## Architecture

### Reconciliation Flow

1. **Validation** - Check URL format, verify credentials secret exists
2. **Namespace** - Create CSI namespace if needed
3. **NetworkPolicy** - Create policy for metrics access
4. **ServiceAccounts** - Create controller and node service accounts
5. **RBAC** - Create ClusterRoles and ClusterRoleBindings
6. **CSIDriver** - Register CSI driver with Kubernetes
7. **ConfigMap** - Create configuration for CSI driver
8. **Controller Deployment** - Deploy CSI controller with sidecars
9. **Node DaemonSet** - Deploy CSI node pods on all nodes

The pods read the ConfigMap and the API key through environment variables, which
are resolved only at startup. Both pod templates carry a hash of those values in
the `csi.truenas.io/config-hash` annotation, so a change to either rolls the pods.
The operator also watches the credentials Secret, so a key rotation rolls them
without waiting for the next periodic reconcile.

### Error Handling

The controller uses standard controller-runtime error handling:

- **Transient errors** - Return `err` for automatic retry with exponential backoff
- **Permanent errors** - Return `reconcile.TerminalError(err)` to stop retries
- **Configuration errors** - Treated as permanent (invalid URL, missing secret key)

### Status Conditions

| Condition | Description |
|-----------|-------------|
| `Ready` | All components are running |
| `Progressing` | Components are being deployed |
| `Degraded` | Reconciliation failed |

## OLM Bundle and CSV Markers

The operator uses OLM (Operator Lifecycle Manager) for distribution via OperatorHub. The bundle contains metadata that describes the operator to users in the OpenShift/OLM console.

### CSV Markers

CSV (ClusterServiceVersion) markers in `api/v1alpha1/truenascsi_types.go` automatically generate UI descriptors for the OperatorHub console. These markers define how fields appear in the OpenShift UI.

**Spec field markers:**
```go
// +operator-sdk:csv:customresourcedefinitions:type=spec,displayName="TrueNAS URL",xDescriptors="urn:alm:descriptor:com.tectonic.ui:text"
TrueNASURL string `json:"truenasURL"`
```

**Status field markers:**
```go
// +operator-sdk:csv:customresourcedefinitions:type=status,displayName="Phase",xDescriptors="urn:alm:descriptor:io.kubernetes.phase"
Phase string `json:"phase,omitempty"`
```

**Resource markers (on the main type):**
```go
// +operator-sdk:csv:customresourcedefinitions:displayName="TrueNAS CSI",resources={{Deployment,v1},{DaemonSet,v1},{ServiceAccount,v1}}
type TrueNASCSI struct { ... }
```

### Common xDescriptors

| Descriptor | Use Case |
|------------|----------|
| `urn:alm:descriptor:com.tectonic.ui:text` | Text input field |
| `urn:alm:descriptor:com.tectonic.ui:number` | Numeric input |
| `urn:alm:descriptor:com.tectonic.ui:booleanSwitch` | Toggle switch |
| `urn:alm:descriptor:io.kubernetes:Secret` | Secret selector |
| `urn:alm:descriptor:com.tectonic.ui:podCount` | Pod replica count |
| `urn:alm:descriptor:io.kubernetes.phase` | Status phase display |
| `urn:alm:descriptor:io.kubernetes.conditions` | Conditions table |

### Regenerating the Bundle

When you modify `truenascsi_types.go`, regenerate the bundle:

```bash
make bundle VERSION=0.1.0
```

The CSV markers ensure that `specDescriptors`, `statusDescriptors`, and `resources` are automatically generated in the CSV - no manual editing required.

### Scorecard Tests

Scorecard validates the operator bundle against OLM best practices and Red Hat certification requirements.

**Run scorecard:**
```bash
# From the operator directory
operator-sdk scorecard bundle/

# Or from the project root
make scorecard
```

**Required tests for certification:**

| Test | Description |
|------|-------------|
| `olm-bundle-validation` | Bundle structure and metadata |
| `olm-crds-have-validation` | CRD has OpenAPI validation |
| `olm-crds-have-resources` | CSV lists created resources |
| `olm-spec-descriptors` | Spec fields have UI descriptors |
| `olm-status-descriptors` | Status fields have UI descriptors |
| `basic-check-spec` | Sample CR has valid spec |

All tests must pass for Red Hat certification.

## Releasing

When releasing a new version, all images must be built, pushed with the version tag, and the `latest` tag must be updated to point to the new release.

### Release Checklist

1. Update `VERSION` in both Makefiles:
   - `Makefile` (project root)
   - `operator/Makefile`

2. Regenerate manifests and bundle:
   ```bash
   cd operator
   make manifests generate
   make bundle VERSION=x.y.z
   ```

3. Build all images (from project root):
   ```bash
   make build-all VERSION=x.y.z
   ```

4. Push all images with version tag AND update `latest`:
   ```bash
   # Option A: Use the release target (recommended - does everything)
   make release VERSION=x.y.z

   # Option B: Manual steps
   make push-all VERSION=x.y.z      # Push versioned images
   make push-latest VERSION=x.y.z   # Update 'latest' tag for all images
   ```

5. Validate with scorecard:
   ```bash
   cd operator
   operator-sdk scorecard bundle/
   ```

6. Submit to Red Hat certification (if applicable)

### Why `latest` Tag Matters

The operator's default configuration uses `quay.io/truenas_solutions/truenas-csi:latest` for the CSI driver image. The `push-latest` target updates the `latest` tag for all images (driver, operator, and bundle).

If the `latest` tag is not updated after a release:
- New operator deployments will pull an outdated driver image
- Integration tests will fail with `ImagePullBackOff` if `latest` doesn't exist
- Users who don't specify a custom `driverImage` will get inconsistent behavior

Always use `make release` or run `make push-latest` after pushing a new versioned release.

## License

This project is licensed under the GNU General Public License v3.0 - see the [LICENSE](../LICENSE) file for details.
