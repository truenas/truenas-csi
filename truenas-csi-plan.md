# TrueNAS Enterprise CSI Driver - Engineering & Architecture Plan

## Executive Summary

This document outlines the engineering and architecture plan for developing an enterprise-grade Kubernetes Container Storage Interface (CSI) driver for TrueNAS storage appliances. The driver will provide native integration between Kubernetes and TrueNAS, supporting both NFS and iSCSI protocols while leveraging TrueNAS's ZFS capabilities for advanced storage features.

## Project Overview

### Vision
Create a production-ready, enterprise-grade CSI driver that enables seamless integration between Kubernetes clusters and TrueNAS storage systems, providing reliable persistent storage with advanced ZFS features.

### Key Differentiators
- 100% API-driven operations (no SSH or CLI dependencies)
- Native support for both NFS and iSCSI protocols
- Full ZFS feature integration (snapshots, clones, compression, deduplication)
- Enterprise-grade reliability and monitoring
- Comprehensive testing and documentation

## Technical Architecture

### High-Level Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    Kubernetes Cluster                        │
│                                                              │
│  ┌─────────────────┐         ┌─────────────────┐           │
│  │ kube-controller │         │   kubelet       │           │
│  │   manager       │         │                 │           │
│  └────────┬────────┘         └────────┬────────┘           │
│           │                           │                      │
│           ▼                           ▼                      │
│  ┌─────────────────┐         ┌─────────────────┐           │
│  │ CSI Controller  │         │   CSI Node      │           │
│  │   Plugin        │         │   Plugin        │           │
│  └────────┬────────┘         └────────┬────────┘           │
│           │                           │                      │
│           └───────────┬───────────────┘                     │
│                       │                                      │
│                       ▼                                      │
│              ┌─────────────────┐                            │
│              │ TrueNAS API     │                            │
│              │   Client        │                            │
│              └────────┬────────┘                            │
└───────────────────────┼──────────────────────────────────────┘
                        │
                        ▼
                ┌─────────────────┐
                │  TrueNAS API    │
                │   (REST/WebSocket)│
                └────────┬────────┘
                        │
                ┌───────▼────────┐
                │  TrueNAS       │
                │  Storage       │
                │  (ZFS)         │
                └────────────────┘
```

### Component Architecture

#### 1. CSI Driver Core
- **Language**: Go (following Kubernetes ecosystem standards)
- **CSI Version**: v1.9.0 (latest stable)
- **Dependencies**:
  - `github.com/container-storage-interface/spec` - CSI specification
  - `google.golang.org/grpc` - gRPC framework
  - `k8s.io/klog` - Kubernetes logging
  - `github.com/kubernetes-csi/csi-lib-utils` - CSI utilities

#### 2. TrueNAS API Client
- **Purpose**: Abstraction layer for all TrueNAS API interactions
- **Features**:
  - Connection pooling and retry logic
  - Request/response caching where appropriate
  - Comprehensive error handling and logging
  - API version compatibility checks

#### 3. Storage Backends
- **NFS Backend**:
  - Dataset creation and management
  - Export configuration
  - Access control (IP-based)
  - Performance tuning parameters
  
- **iSCSI Backend**:
  - ZVOL creation and management
  - Target and initiator configuration
  - CHAP authentication support
  - Multipath I/O support

### CSI Service Implementation

#### Identity Service
```go
type IdentityServer struct {
    driver *TrueNASDriver
}

// Required methods:
- GetPluginInfo() - Return driver name and version
- GetPluginCapabilities() - Advertise supported features
- Probe() - Health check endpoint
```

#### Controller Service
```go
type ControllerServer struct {
    driver *TrueNASDriver
    client *TrueNASAPIClient
}

// Required methods:
- CreateVolume() - Provision new volumes
- DeleteVolume() - Remove volumes
- ControllerPublishVolume() - Attach volumes (iSCSI)
- ControllerUnpublishVolume() - Detach volumes
- ValidateVolumeCapabilities() - Validate volume requirements
- ListVolumes() - List existing volumes
- GetCapacity() - Report available capacity
- ControllerGetCapabilities() - Advertise controller features

// Optional methods (for advanced features):
- CreateSnapshot() - Create ZFS snapshots
- DeleteSnapshot() - Remove snapshots
- ListSnapshots() - List snapshots
- ControllerExpandVolume() - Resize volumes
- ControllerGetVolume() - Get volume details
```

#### Node Service
```go
type NodeServer struct {
    driver *TrueNASDriver
    mounter *SafeMounter
}

// Required methods:
- NodeStageVolume() - Stage volume on node (iSCSI)
- NodeUnstageVolume() - Unstage volume
- NodePublishVolume() - Mount volume to pod
- NodeUnpublishVolume() - Unmount volume
- NodeGetInfo() - Return node information
- NodeGetCapabilities() - Advertise node features

// Optional methods:
- NodeGetVolumeStats() - Report volume usage
- NodeExpandVolume() - Expand filesystem
```

### Supported Capabilities

#### Plugin Capabilities
- **CONTROLLER_SERVICE** - Full controller functionality
- **VOLUME_ACCESSIBILITY_CONSTRAINTS** - Topology-aware provisioning
- **VOLUME_EXPANSION** - Online volume expansion

#### Controller Capabilities
- **CREATE_DELETE_VOLUME** - Dynamic provisioning
- **PUBLISH_UNPUBLISH_VOLUME** - Volume attach/detach
- **CREATE_DELETE_SNAPSHOT** - Snapshot management
- **LIST_SNAPSHOTS** - Snapshot enumeration
- **CLONE_VOLUME** - Fast ZFS clones
- **EXPAND_VOLUME** - Volume resizing
- **LIST_VOLUMES** - Volume enumeration

#### Node Capabilities
- **STAGE_UNSTAGE_VOLUME** - Staging support (iSCSI)
- **GET_VOLUME_STATS** - Usage reporting
- **EXPAND_VOLUME** - Filesystem expansion

### Storage Class Parameters

```yaml
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: truenas-iscsi
provisioner: csi.truenas.com
parameters:
  # Common parameters
  protocol: "iscsi"  # or "nfs"
  pool: "tank1"
  
  # ZFS parameters
  compression: "lz4"
  deduplication: "off"
  recordsize: "128k"
  
  # iSCSI specific
  targetPortal: "192.168.1.100:3260"
  targetInterface: "default"
  chapAuthEnabled: "true"
  chapAuthUser: "username"
  chapAuthSecret: "secret-name"
  
  # NFS specific
  nfsVersion: "4"
  nfsMountOptions: "hard,nointr"
  
  # Advanced features
  snapshotPolicy: "hourly"
  replicationTarget: "backup-nas"
```

## Implementation Plan

### Phase 1: Foundation (Weeks 1-4)
1. **Project Setup**
   - Repository structure
   - CI/CD pipeline (GitHub Actions)
   - Development environment
   - Dependency management

2. **TrueNAS API Client**
   - API authentication
   - Connection management
   - Basic CRUD operations
   - Error handling framework

3. **CSI Driver Skeleton**
   - gRPC server setup
   - Service stubs
   - Logging framework
   - Configuration management

### Phase 2: Core Functionality (Weeks 5-8)
1. **NFS Implementation**
   - Dataset provisioning
   - Export management
   - Mount operations
   - Basic testing

2. **iSCSI Implementation**
   - ZVOL provisioning
   - Target configuration
   - Initiator management
   - Multipath support

3. **Basic CSI Compliance**
   - Identity service
   - Basic controller operations
   - Basic node operations
   - CSI sanity tests

### Phase 3: Advanced Features (Weeks 9-12)
1. **Snapshot Management**
   - Create/delete snapshots
   - List snapshots
   - Restore from snapshot
   - Scheduled snapshots

2. **Volume Cloning**
   - Fast ZFS clones
   - Clone from snapshot
   - Clone from volume

3. **Volume Expansion**
   - Online expansion
   - Filesystem resize
   - Capacity management

4. **Topology Support**
   - Node affinity
   - Availability zones
   - Failure domains

### Phase 4: Enterprise Features (Weeks 13-16)
1. **High Availability**
   - Controller failover
   - Connection retry logic
   - State management
   - Leader election

2. **Monitoring & Observability**
   - Prometheus metrics
   - Structured logging
   - Distributed tracing
   - Health endpoints

3. **Security Hardening**
   - RBAC policies
   - Secret management
   - TLS communication
   - Security scanning

4. **Performance Optimization**
   - Connection pooling
   - Request batching
   - Caching strategies
   - Resource limits

### Phase 5: Testing & Documentation (Weeks 17-20)
1. **Comprehensive Testing**
   - Unit tests (>80% coverage)
   - Integration tests
   - E2E tests
   - Performance tests
   - Chaos testing

2. **Documentation**
   - Installation guide
   - Configuration reference
   - Troubleshooting guide
   - API documentation
   - Architecture diagrams

3. **Certification**
   - CSI conformance tests
   - Kubernetes certification
   - Security audit
   - Performance benchmarks

## Deployment Architecture

### Controller Deployment
```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: truenas-csi-controller
spec:
  replicas: 2  # HA configuration
  template:
    spec:
      containers:
      - name: csi-driver
        image: truenas/csi-driver:v1.0.0
        args:
        - --endpoint=unix:///csi/csi.sock
        - --mode=controller
        - --driver-name=csi.truenas.com
      
      # Sidecar containers
      - name: csi-provisioner
        image: registry.k8s.io/sig-storage/csi-provisioner:v3.6.0
      
      - name: csi-attacher
        image: registry.k8s.io/sig-storage/csi-attacher:v4.4.0
      
      - name: csi-snapshotter
        image: registry.k8s.io/sig-storage/csi-snapshotter:v6.3.0
      
      - name: csi-resizer
        image: registry.k8s.io/sig-storage/csi-resizer:v1.9.0
```

### Node DaemonSet
```yaml
apiVersion: apps/v1
kind: DaemonSet
metadata:
  name: truenas-csi-node
spec:
  template:
    spec:
      containers:
      - name: csi-driver
        image: truenas/csi-driver:v1.0.0
        args:
        - --endpoint=unix:///csi/csi.sock
        - --mode=node
        - --driver-name=csi.truenas.com
        securityContext:
          privileged: true
      
      - name: node-driver-registrar
        image: registry.k8s.io/sig-storage/csi-node-driver-registrar:v2.9.0
```

## Testing Strategy

### Unit Testing
- Mock TrueNAS API responses
- Test all error conditions
- Validate business logic
- Coverage target: >80%

### Integration Testing
- Real TrueNAS instance
- API interaction validation
- Protocol-specific tests
- Failure scenario testing

### E2E Testing
- Full Kubernetes cluster
- Workload scenarios
- Performance benchmarks
- Failure injection

### Conformance Testing
- CSI sanity tests
- Kubernetes e2e storage tests
- Custom test scenarios
- Multi-cluster validation

## Monitoring & Observability

### Metrics (Prometheus)
- Volume provisioning latency
- API call success/failure rates
- Storage capacity utilization
- Operation queue depths
- Error rates by type

### Logging
- Structured JSON logging
- Log levels: DEBUG, INFO, WARN, ERROR
- Request/response tracing
- Correlation IDs

### Alerts
- Provisioning failures
- API connectivity issues
- Capacity thresholds
- Performance degradation

## Security Considerations

### Authentication & Authorization
- TrueNAS API key management
- Kubernetes RBAC integration
- Service account permissions
- Secret rotation

### Network Security
- TLS for API communication
- Network policies
- Firewall rules
- IP whitelisting

### Data Security
- Encryption at rest (ZFS)
- Encryption in transit
- Access control lists
- Audit logging

## Performance Optimization

### API Client
- Connection pooling
- Request batching
- Response caching
- Retry with backoff

### Volume Operations
- Parallel provisioning
- Async operations
- Resource quotas
- Rate limiting

### Monitoring
- Performance metrics
- Bottleneck identification
- Capacity planning
- Trend analysis

## Maintenance & Support

### Version Compatibility
- Kubernetes versions: 1.24+
- TrueNAS versions: CORE 13.0+, SCALE 22.12+
- CSI spec versions: v1.5.0+
- Regular compatibility testing

### Upgrade Strategy
- Rolling updates
- Version migration tools
- Backward compatibility
- Deprecation notices

### Support Model
- GitHub issues
- Community forums
- Enterprise support tiers
- SLA definitions

## Success Metrics

### Technical Metrics
- 99.9% uptime
- <5s provisioning time
- <1s attach/detach time
- Zero data loss

### Adoption Metrics
- GitHub stars/forks
- Download statistics
- Production deployments
- Community contributions

### Quality Metrics
- Bug discovery rate
- Time to resolution
- Test coverage
- Documentation completeness

## Risk Management

### Technical Risks
- API breaking changes
- Performance bottlenecks
- Compatibility issues
- Security vulnerabilities

### Mitigation Strategies
- Comprehensive testing
- Version pinning
- Performance monitoring
- Security scanning
- Regular updates

## Conclusion

This engineering plan provides a comprehensive roadmap for developing an enterprise-grade CSI driver for TrueNAS. By following this plan, we can deliver a reliable, performant, and feature-rich storage solution that seamlessly integrates TrueNAS with Kubernetes environments.

The phased approach ensures incremental delivery of value while maintaining high quality standards. Regular testing, monitoring, and community engagement will be crucial for the long-term success of the project.
