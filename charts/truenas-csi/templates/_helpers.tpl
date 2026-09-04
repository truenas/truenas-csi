{{/*
Chart name, overridable.
*/}}
{{- define "truenas-csi.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Resource name prefix. Deliberately NOT release-prefixed: the names must match
the ones in deploy/truenas-csi-driver.yaml so an existing kubectl install can be
adopted by Helm, and because a Deployment's selector cannot be changed after
creation. Set fullnameOverride when running a second instance in one cluster.
*/}}
{{- define "truenas-csi.fullname" -}}
{{- default (include "truenas-csi.name" .) .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Labels attached to every object. These are metadata only. The pod selectors use
the single legacy `app` label below, which must never change.
*/}}
{{- define "truenas-csi.labels" -}}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
app.kubernetes.io/name: {{ include "truenas-csi.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/part-of: {{ include "truenas-csi.name" . }}
{{- end -}}

{{/*
Selector label for the controller. Immutable once deployed.
*/}}
{{- define "truenas-csi.controllerSelectorLabel" -}}
app: {{ include "truenas-csi.fullname" . }}-controller
{{- end -}}

{{/*
Selector label for the node plugin. Immutable once deployed.
*/}}
{{- define "truenas-csi.nodeSelectorLabel" -}}
app: {{ include "truenas-csi.fullname" . }}-node
{{- end -}}

{{- define "truenas-csi.controllerServiceAccountName" -}}
{{- default (printf "%s-controller-sa" (include "truenas-csi.fullname" .)) .Values.serviceAccount.controller -}}
{{- end -}}

{{- define "truenas-csi.nodeServiceAccountName" -}}
{{- default (printf "%s-node-sa" (include "truenas-csi.fullname" .)) .Values.serviceAccount.node -}}
{{- end -}}

{{- define "truenas-csi.configMapName" -}}
{{ include "truenas-csi.fullname" . }}-config
{{- end -}}

{{/*
Secret holding the API key: the one the user manages, or the one this chart creates.
*/}}
{{- define "truenas-csi.secretName" -}}
{{- if .Values.truenas.existingSecret -}}
{{ .Values.truenas.existingSecret }}
{{- else -}}
truenas-api-credentials
{{- end -}}
{{- end -}}

{{- define "truenas-csi.secretKey" -}}
{{- default "api-key" .Values.truenas.existingSecretKey -}}
{{- end -}}

{{/*
Driver image. The published tags carry a leading v, so an unset tag becomes
v<appVersion> rather than the bare appVersion.
*/}}
{{- define "truenas-csi.driverImage" -}}
{{ .Values.image.driver.repository }}:{{ .Values.image.driver.tag | default (printf "v%s" .Chart.AppVersion) }}
{{- end -}}

{{/*
Environment shared by the controller and node containers. metricsKey selects
which ConfigMap key supplies the metrics listen address, since the two workloads
use separate keys.
*/}}
{{- define "truenas-csi.driverEnv" -}}
{{- $ctx := .ctx -}}
- name: CSI_ENDPOINT
  value: unix:///csi/csi.sock
- name: TRUENAS_URL
  valueFrom:
    configMapKeyRef:
      name: {{ include "truenas-csi.configMapName" $ctx }}
      key: truenasURL
- name: TRUENAS_API_KEY
  valueFrom:
    secretKeyRef:
      name: {{ include "truenas-csi.secretName" $ctx }}
      key: {{ include "truenas-csi.secretKey" $ctx }}
- name: TRUENAS_DEFAULT_POOL
  valueFrom:
    configMapKeyRef:
      name: {{ include "truenas-csi.configMapName" $ctx }}
      key: defaultPool
- name: TRUENAS_NFS_SERVER
  valueFrom:
    configMapKeyRef:
      name: {{ include "truenas-csi.configMapName" $ctx }}
      key: nfsServer
- name: TRUENAS_ISCSI_PORTAL
  valueFrom:
    configMapKeyRef:
      name: {{ include "truenas-csi.configMapName" $ctx }}
      key: iscsiPortal
- name: TRUENAS_NVMEOF_PORTAL
  valueFrom:
    configMapKeyRef:
      name: {{ include "truenas-csi.configMapName" $ctx }}
      key: nvmeofPortal
      optional: true
- name: TRUENAS_ISCSI_IQN_BASE
  valueFrom:
    configMapKeyRef:
      name: {{ include "truenas-csi.configMapName" $ctx }}
      key: iscsiIQNBase
      optional: true
- name: TRUENAS_INSECURE_SKIP_VERIFY
  valueFrom:
    configMapKeyRef:
      name: {{ include "truenas-csi.configMapName" $ctx }}
      key: truenasInsecure
      optional: true
- name: TRUENAS_METRICS_ADDR
  valueFrom:
    configMapKeyRef:
      name: {{ include "truenas-csi.configMapName" $ctx }}
      key: {{ .metricsKey }}
      optional: true
{{- if ne $ctx.Values.driverName "csi.truenas.io" }}
- name: CSI_DRIVER_NAME
  value: {{ $ctx.Values.driverName | quote }}
{{- end }}
- name: NODE_ID
  valueFrom:
    fieldRef:
      fieldPath: spec.nodeName
{{- end -}}
