{{/* Names and labels */}}
{{- define "caregraph.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 40 | trimSuffix "-" -}}
{{- end -}}

{{- define "caregraph.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 40 | trimSuffix "-" -}}
{{- else -}}
{{- $name := default .Chart.Name .Values.nameOverride -}}
{{- if contains $name .Release.Name -}}
{{- .Release.Name | trunc 40 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name $name | trunc 40 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{- define "caregraph.commonLabels" -}}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end -}}

{{- define "caregraph.labels" -}}
{{ include "caregraph.commonLabels" . }}
app.kubernetes.io/name: {{ include "caregraph.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{/* Labels that select one component's pods. Call with a dict: root, component. */}}
{{- define "caregraph.selectorLabels" -}}
app.kubernetes.io/name: {{ include "caregraph.name" .root }}
app.kubernetes.io/instance: {{ .root.Release.Name }}
app.kubernetes.io/component: {{ .component }}
{{- end -}}

{{- define "caregraph.isProduction" -}}
{{- if eq .Values.environment "production" -}}true{{- end -}}
{{- end -}}

{{- define "caregraph.scheme" -}}
{{- if .Values.tls.enabled -}}https{{- else -}}http{{- end -}}
{{- end -}}

{{/* Images */}}
{{- define "caregraph.writerImage" -}}
{{- printf "%s/%s:%s" .Values.image.registry .Values.image.writer.repository (default .Chart.AppVersion .Values.image.writer.tag) -}}
{{- end -}}

{{- define "caregraph.readerImage" -}}
{{- printf "%s/%s:%s" .Values.image.registry .Values.image.reader.repository (default .Chart.AppVersion .Values.image.reader.tag) -}}
{{- end -}}

{{/* Service names */}}
{{- define "caregraph.headless" -}}{{ include "caregraph.fullname" . }}-headless{{- end -}}
{{- define "caregraph.writeService" -}}{{ include "caregraph.fullname" . }}-write{{- end -}}
{{- define "caregraph.readService" -}}{{ include "caregraph.fullname" . }}-read{{- end -}}
{{- define "caregraph.standbyService" -}}{{ include "caregraph.fullname" . }}-standby{{- end -}}

{{- define "caregraph.devAuthSecret" -}}
{{- if .Values.auth.existingSecret -}}{{ .Values.auth.existingSecret }}{{- else -}}{{ include "caregraph.fullname" . }}-dev-auth{{- end -}}
{{- end -}}

{{/* Refuse an install that would not be safe or would not start. */}}
{{- define "caregraph.validate" -}}
{{- if not (has .Values.environment (list "development" "production")) -}}
{{- fail "environment must be development or production" -}}
{{- end -}}
{{- if ne (int .Values.writer.replicas) 2 -}}
{{- fail "writer.replicas must be 2: one leader and one synchronous standby" -}}
{{- end -}}
{{- if and .Values.tls.enabled (not .Values.tls.secretName) -}}
{{- fail "tls.enabled needs tls.secretName (or externalSecrets to create it)" -}}
{{- end -}}
{{- if include "caregraph.isProduction" . -}}
{{- if not .Values.tls.enabled -}}{{- fail "production needs tls.enabled (mutual TLS)" -}}{{- end -}}
{{- if not .Values.encryption.secretName -}}{{- fail "production needs encryption.secretName (key-encryption key and wrapped data key)" -}}{{- end -}}
{{- if not .Values.rbac.principals -}}{{- fail "production needs rbac.principals (who may do what)" -}}{{- end -}}
{{- if not .Values.config.syncWrites -}}{{- fail "production needs config.syncWrites=true" -}}{{- end -}}
{{- if eq .Values.config.tsPolicy "legacy" -}}{{- fail "production does not allow config.tsPolicy=legacy" -}}{{- end -}}
{{- else -}}
{{- if and (not .Values.auth.existingSecret) (not .Values.auth.apiKey) (not .Values.tls.enabled) -}}
{{- fail "development without TLS needs auth.apiKey or auth.existingSecret" -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{/* Settings every CareGraph container shares. Call with the root context. */}}
{{- define "caregraph.commonEnv" -}}
- name: CAREGRAPH_ENV
  value: {{ .Values.environment | quote }}
- name: CAREGRAPH_GRPC_ADDR
  value: "0.0.0.0:50051"
- name: CAREGRAPH_METRICS_ADDR
  value: "0.0.0.0:9100"
- name: CAREGRAPH_DB_PATH
  value: /var/lib/caregraph/db
- name: CAREGRAPH_AUDIT_DIR
  value: /var/lib/caregraph/audit
- name: CAREGRAPH_POD_NAME
  valueFrom:
    fieldRef:
      fieldPath: metadata.name
- name: POD_NAME
  valueFrom:
    fieldRef:
      fieldPath: metadata.name
- name: POD_NAMESPACE
  valueFrom:
    fieldRef:
      fieldPath: metadata.namespace
- name: HOME
  value: /tmp
- name: TORCH_HOME
  value: /tmp/torch
- name: PYTHONDONTWRITEBYTECODE
  value: "1"
- name: CAREGRAPH_LOG_JSON
  value: "true"
- name: CAREGRAPH_SYNC_WRITES
  value: {{ .Values.config.syncWrites | quote }}
- name: CAREGRAPH_TS_POLICY
  value: {{ .Values.config.tsPolicy | quote }}
- name: CAREGRAPH_WAL_TTL_SECS
  value: {{ .Values.config.walTtlSecs | quote }}
- name: CAREGRAPH_LEADER_ADDR
  value: {{ printf "%s://%s:50051" (include "caregraph.scheme" .) (include "caregraph.writeService" .) | quote }}
{{- if .Values.tls.enabled }}
- name: CAREGRAPH_TLS_CERT
  value: /etc/caregraph/tls/tls.crt
- name: CAREGRAPH_TLS_KEY
  value: /etc/caregraph/tls/tls.key
- name: CAREGRAPH_TLS_CLIENT_CA
  value: /etc/caregraph/tls/ca.crt
- name: CAREGRAPH_REPLICATION_CA
  value: /etc/caregraph/tls/ca.crt
- name: CAREGRAPH_REPLICATION_CERT
  value: /etc/caregraph/tls/tls.crt
- name: CAREGRAPH_REPLICATION_KEY
  value: /etc/caregraph/tls/tls.key
- name: CAREGRAPH_REPLICATION_SERVER_NAME
  value: {{ default (include "caregraph.writeService" .) .Values.tls.serverName | quote }}
{{- end }}
{{- if .Values.rbac.principals }}
- name: CAREGRAPH_RBAC_POLICY
  value: /etc/caregraph/rbac/policy.toml
{{- end }}
{{- if .Values.tokens.secretName }}
- name: CAREGRAPH_TOKENS_FILE
  value: /etc/caregraph/tokens/tokens.toml
{{- end }}
{{- if .Values.tokens.replicationTokenSecret }}
- name: CAREGRAPH_REPLICATION_TOKEN
  valueFrom:
    secretKeyRef:
      name: {{ .Values.tokens.replicationTokenSecret }}
      key: token
{{- end }}
{{- if .Values.encryption.secretName }}
- name: CAREGRAPH_KEK_FILE
  value: /etc/caregraph/keys/kek
- name: CAREGRAPH_WRAPPED_DEK
  value: /etc/caregraph/keys/wrapped-dek
{{- end }}
{{- if not (include "caregraph.isProduction" .) }}
{{- if not .Values.tls.enabled }}
- name: CAREGRAPH_API_KEY
  valueFrom:
    secretKeyRef:
      name: {{ include "caregraph.devAuthSecret" . }}
      key: {{ .Values.auth.existingSecretKey }}
{{- if not .Values.tokens.replicationTokenSecret }}
- name: CAREGRAPH_REPLICATION_TOKEN
  valueFrom:
    secretKeyRef:
      name: {{ include "caregraph.devAuthSecret" . }}
      key: {{ .Values.auth.existingSecretKey }}
{{- end }}
{{- end }}
{{- end }}
{{- if .Values.otel.endpoint }}
- name: CAREGRAPH_OTEL_ENDPOINT
  value: {{ .Values.otel.endpoint | quote }}
- name: CAREGRAPH_OTEL_SAMPLE_RATIO
  value: {{ .Values.otel.sampleRatio | quote }}
{{- end }}
{{- range $k, $v := .Values.config.extra }}
- name: {{ $k }}
  value: {{ $v | quote }}
{{- end }}
{{- with .Values.extraEnv }}
{{ toYaml . }}
{{- end }}
{{- end -}}

{{/* Volume mounts for credentials and policy. */}}
{{- define "caregraph.secretMounts" -}}
{{- if .Values.tls.enabled }}
- name: tls
  mountPath: /etc/caregraph/tls
  readOnly: true
{{- end }}
{{- if .Values.rbac.principals }}
- name: rbac
  mountPath: /etc/caregraph/rbac
  readOnly: true
{{- end }}
{{- if .Values.tokens.secretName }}
- name: tokens
  mountPath: /etc/caregraph/tokens
  readOnly: true
{{- end }}
{{- if .Values.encryption.secretName }}
- name: keys
  mountPath: /etc/caregraph/keys
  readOnly: true
{{- end }}
{{- end -}}

{{- define "caregraph.secretVolumes" -}}
{{- if .Values.tls.enabled }}
- name: tls
  secret:
    secretName: {{ .Values.tls.secretName }}
    defaultMode: 0440
{{- end }}
{{- if .Values.rbac.principals }}
- name: rbac
  configMap:
    name: {{ include "caregraph.fullname" . }}-rbac
{{- end }}
{{- if .Values.tokens.secretName }}
- name: tokens
  secret:
    secretName: {{ .Values.tokens.secretName }}
    defaultMode: 0440
{{- end }}
{{- if .Values.encryption.secretName }}
- name: keys
  secret:
    secretName: {{ .Values.encryption.secretName }}
    defaultMode: 0440
{{- end }}
{{- end -}}

{{/* Hardened container security context. */}}
{{- define "caregraph.containerSecurityContext" -}}
allowPrivilegeEscalation: false
readOnlyRootFilesystem: true
runAsNonRoot: true
runAsUser: 10001
runAsGroup: 10001
capabilities:
  drop: ["ALL"]
seccompProfile:
  type: RuntimeDefault
{{- end -}}

{{- define "caregraph.podSecurityContext" -}}
runAsNonRoot: true
runAsUser: 10001
runAsGroup: 10001
fsGroup: 10001
fsGroupChangePolicy: OnRootMismatch
seccompProfile:
  type: RuntimeDefault
{{- end -}}

{{/* Probes against the HTTP port (the gRPC port requires a client certificate). */}}
{{- define "caregraph.probes" -}}
startupProbe:
  httpGet: {path: /startupz, port: metrics}
  periodSeconds: 5
  failureThreshold: 120
readinessProbe:
  httpGet: {path: /readyz, port: metrics}
  periodSeconds: 5
  timeoutSeconds: 3
  failureThreshold: 2
livenessProbe:
  httpGet: {path: /healthz, port: metrics}
  periodSeconds: 10
  timeoutSeconds: 5
  failureThreshold: 6
{{- end -}}
