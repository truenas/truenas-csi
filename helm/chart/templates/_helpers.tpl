{{- define "truenas-csi.namespace" -}}
{{- .Release.Namespace }}
{{- end }}

{{- define "truenas-csi.labels" -}}
helm.sh/chart: {{ .Chart.Name }}-{{ .Chart.Version | replace "+" "_" }}
app.kubernetes.io/name: {{ .Chart.Name }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{- define "truenas-csi.selectorLabels" -}}
app.kubernetes.io/name: {{ .Chart.Name }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{- define "truenas-csi.controllerSelectorLabels" -}}
{{ include "truenas-csi.selectorLabels" . }}
app.kubernetes.io/component: controller
{{- end }}

{{- define "truenas-csi.nodeSelectorLabels" -}}
{{ include "truenas-csi.selectorLabels" . }}
app.kubernetes.io/component: node
{{- end }}

{{- define "truenas-csi.csiImage" -}}
{{ .Values.images.csiDriver.repository }}:{{ .Values.images.csiDriver.tag | default .Chart.AppVersion }}
{{- end }}
