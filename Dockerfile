FROM golang:1.24.5-alpine AS builder

WORKDIR /build
COPY go.mod go.sum* ./
RUN go mod download 2>/dev/null || true
COPY . .
ARG VERSION=dev
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags "-X github.com/truenas/truenas-csi/pkg/driver.DRIVER_VERSION=${VERSION}" -o truenas-csi-driver cmd/main.go

FROM alpine:3.19
# On Alpine, `resize2fs` lives in `e2fsprogs-extra` (not the base `e2fsprogs`),
# and `xfs_growfs` lives in `xfsprogs-extra`. The CSI driver calls both during
# MountDevice (idempotent FS-resize after attach) and NodeExpandVolume (for
# PVCs with allowVolumeExpansion=true). Without the -extra packages the driver
# returns "executable file not found in $PATH" on every fresh mount and
# silently fails to grow the filesystem on PVC expansion. See issue #25.
RUN apk add --no-cache ca-certificates nfs-utils open-iscsi nvme-cli \
    e2fsprogs e2fsprogs-extra xfsprogs xfsprogs-extra util-linux
COPY --from=builder /build/truenas-csi-driver /truenas-csi-driver
ENTRYPOINT ["/truenas-csi-driver"]
