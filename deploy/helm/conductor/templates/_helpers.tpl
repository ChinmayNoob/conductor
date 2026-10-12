{{/* Names. */}}
{{- define "conductor.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "conductor.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{- define "conductor.labels" -}}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{ include "conductor.selectorLabels" . }}
app.kubernetes.io/version: {{ .Values.image.tag | default .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{- define "conductor.selectorLabels" -}}
app.kubernetes.io/name: {{ include "conductor.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/* Labels for one component: include "conductor.componentLabels" (list $ "worker") */}}
{{- define "conductor.componentLabels" -}}
{{ include "conductor.labels" (index . 0) }}
app.kubernetes.io/component: {{ index . 1 }}
{{- end }}

{{- define "conductor.componentSelector" -}}
{{ include "conductor.selectorLabels" (index . 0) }}
app.kubernetes.io/component: {{ index . 1 }}
{{- end }}

{{- define "conductor.image" -}}
{{ .Values.image.repository }}:{{ .Values.image.tag | default .Chart.AppVersion }}
{{- end }}

{{- define "conductor.authSecret" -}}
{{- .Values.auth.existingSecret | default (printf "%s-auth" (include "conductor.fullname" .)) }}
{{- end }}

{{- define "conductor.tlsSecret" -}}
{{- .Values.tls.existingSecret | default (printf "%s-tls" (include "conductor.fullname" .)) }}
{{- end }}

{{- define "conductor.coordinatorAddr" -}}
{{ include "conductor.fullname" . }}-coordinator:8080
{{- end }}

{{/* A model is configured: workers advertise llm tasks. */}}
{{- define "conductor.llmEnabled" -}}
{{- if or .Values.llm.baseURL .Values.llm.existingSecret .Values.mockLLM.enabled }}true{{ end }}
{{- end }}

{{/* Database connection, per postgres.mode. */}}
{{- define "conductor.dbHost" -}}
{{- if eq .Values.postgres.mode "dev" }}{{ include "conductor.fullname" . }}-postgres
{{- else if eq .Values.postgres.mode "cloudnativepg" }}{{ include "conductor.fullname" . }}-pg-rw
{{- else if eq .Values.postgres.mode "external" }}{{ required "postgres.external.host is required" .Values.postgres.external.host }}
{{- else }}{{ fail (printf "postgres.mode must be dev, cloudnativepg or external, not %q" .Values.postgres.mode) }}
{{- end }}
{{- end }}

{{/* The host as seen from other namespaces (KEDA's). */}}
{{- define "conductor.dbHostFQDN" -}}
{{- if eq .Values.postgres.mode "external" }}{{ include "conductor.dbHost" . }}
{{- else }}{{ include "conductor.dbHost" . }}.{{ .Release.Namespace }}.svc
{{- end }}
{{- end }}

{{- define "conductor.dbPort" -}}
{{- if eq .Values.postgres.mode "external" }}{{ .Values.postgres.external.port }}{{ else }}5432{{ end }}
{{- end }}

{{- define "conductor.dbUser" -}}
{{- if eq .Values.postgres.mode "external" }}{{ .Values.postgres.external.user }}{{ else }}conductor{{ end }}
{{- end }}

{{- define "conductor.dbSSLMode" -}}
{{- if eq .Values.postgres.mode "external" }}{{ .Values.postgres.external.sslMode }}
{{- else if eq .Values.postgres.mode "cloudnativepg" }}require
{{- else }}disable{{ end }}
{{- end }}

{{- define "conductor.dbPasswordSecret" -}}
{{- if eq .Values.postgres.mode "external" }}{{ required "postgres.external.existingSecret is required" .Values.postgres.external.existingSecret }}
{{- else if eq .Values.postgres.mode "cloudnativepg" }}{{ include "conductor.fullname" . }}-pg-app
{{- else }}{{ include "conductor.fullname" . }}-postgres{{ end }}
{{- end }}

{{- define "conductor.dbPasswordKey" -}}
{{- if eq .Values.postgres.mode "external" }}{{ .Values.postgres.external.passwordKey }}{{ else }}password{{ end }}
{{- end }}

{{/* Environment for components that use the database. */}}
{{- define "conductor.dbEnv" -}}
- name: POSTGRES_HOST
  value: {{ include "conductor.dbHost" . | quote }}
- name: POSTGRES_PORT
  value: {{ include "conductor.dbPort" . | quote }}
- name: POSTGRES_DB
  value: {{ .Values.postgres.database | quote }}
- name: POSTGRES_USER
  value: {{ include "conductor.dbUser" . | quote }}
- name: POSTGRES_PASSWORD
  valueFrom:
    secretKeyRef:
      name: {{ include "conductor.dbPasswordSecret" . }}
      key: {{ include "conductor.dbPasswordKey" . }}
- name: POSTGRES_SSLMODE
  value: {{ include "conductor.dbSSLMode" . | quote }}
{{- end }}

{{/* Environment every component needs: the coordinators, the cluster token, TLS, logging, tracing. */}}
{{- define "conductor.clusterEnv" -}}
- name: POD_IP
  valueFrom:
    fieldRef:
      fieldPath: status.podIP
- name: CONDUCTOR_COORDINATOR_ADDR
  value: {{ include "conductor.coordinatorAddr" . | quote }}
- name: CONDUCTOR_CLUSTER_TOKEN
  valueFrom:
    secretKeyRef:
      name: {{ include "conductor.authSecret" . }}
      key: cluster-token
{{- if .Values.tls.enabled }}
- name: CONDUCTOR_TLS_CERT
  value: /etc/conductor/tls/tls.crt
- name: CONDUCTOR_TLS_KEY
  value: /etc/conductor/tls/tls.key
- name: CONDUCTOR_TLS_CA
  value: /etc/conductor/tls/ca.crt
- name: CONDUCTOR_TLS_SERVER_NAME
  value: {{ .Values.tls.serverName | quote }}
{{- end }}
- name: CONDUCTOR_LOG_LEVEL
  value: {{ .Values.logging.level | quote }}
- name: CONDUCTOR_LOG_FORMAT
  value: {{ .Values.logging.format | quote }}
- name: CONDUCTOR_METRICS_LISTEN
  value: ":9090"
{{- with .Values.tracing.otlpEndpoint }}
- name: OTEL_EXPORTER_OTLP_ENDPOINT
  value: {{ . | quote }}
{{- end }}
{{- end }}

{{/* Language model settings, for workers and the API. */}}
{{- define "conductor.llmEnv" -}}
{{- with .Values.llm.existingSecret }}
- name: OPENAI_API_KEY
  valueFrom:
    secretKeyRef:
      name: {{ . }}
      key: {{ $.Values.llm.apiKeyKey }}
{{- end }}
{{- if .Values.llm.baseURL }}
- name: CONDUCTOR_LLM_BASE_URL
  value: {{ .Values.llm.baseURL | quote }}
{{- else if .Values.mockLLM.enabled }}
- name: CONDUCTOR_LLM_BASE_URL
  value: "http://{{ include "conductor.fullname" . }}-mock-llm:8090/v1"
{{- end }}
{{- if .Values.llm.model }}
- name: CONDUCTOR_LLM_MODEL
  value: {{ .Values.llm.model | quote }}
{{- else if and .Values.mockLLM.enabled (not .Values.llm.baseURL) }}
- name: CONDUCTOR_LLM_MODEL
  value: mock-model
{{- end }}
{{- with .Values.llm.prices }}
- name: CONDUCTOR_LLM_PRICES
  value: {{ . | quote }}
{{- end }}
{{- end }}

{{- define "conductor.tlsVolumeMount" -}}
{{- if .Values.tls.enabled }}
- name: tls
  mountPath: /etc/conductor/tls
  readOnly: true
{{- end }}
{{- end }}

{{- define "conductor.tlsVolume" -}}
{{- if .Values.tls.enabled }}
- name: tls
  secret:
    secretName: {{ include "conductor.tlsSecret" . }}
{{- end }}
{{- end }}

{{/* Probes against the metrics port, which every component serves. */}}
{{- define "conductor.probes" -}}
startupProbe:
  httpGet: {path: /healthz, port: metrics}
  periodSeconds: 2
  failureThreshold: 60
livenessProbe:
  httpGet: {path: /healthz, port: metrics}
  periodSeconds: 10
  failureThreshold: 3
readinessProbe:
  httpGet: {path: /readyz, port: metrics}
  periodSeconds: 5
  failureThreshold: 3
{{- end }}

{{/* Spread a component's pods across nodes when there are several. */}}
{{- define "conductor.spread" -}}
topologySpreadConstraints:
  - maxSkew: 1
    topologyKey: kubernetes.io/hostname
    whenUnsatisfiable: ScheduleAnyway
    labelSelector:
      matchLabels:
        {{- include "conductor.componentSelector" . | nindent 8 }}
{{- end }}

{{- define "conductor.metricsAnnotations" -}}
prometheus.io/scrape: "true"
prometheus.io/port: "9090"
prometheus.io/path: /metrics
{{- end }}

{{/* Worker pools: the default pool (worker) and workerPools, each merged over worker. */}}
{{- define "conductor.workerPools" -}}
{{- $pools := dict "default" (deepCopy .Values.worker) }}
{{- range $name, $overrides := .Values.workerPools }}
{{- $_ := set $pools $name (mergeOverwrite (deepCopy $.Values.worker) $overrides) }}
{{- end }}
{{- if .Values.kubernetesTasks.enabled }}
{{- if hasKey .Values.workerPools "k8s" }}{{ fail "workerPools.k8s is reserved for kubernetesTasks.workers" }}{{ end }}
{{- /* Only k8s tasks, nothing inherited that could add others or keep them out. */}}
{{- $k8s := mergeOverwrite (deepCopy .Values.worker) (deepCopy .Values.kubernetesTasks.workers) }}
{{- $_ := set $k8s "taskTypes" (list "k8s") }}
{{- $_ := set $k8s "labels" dict }}
{{- $_ := set $k8s "dedicated" false }}
{{- $_ := set $k8s "passEnv" list }}
{{- $_ := set $k8s "extraEnv" (.Values.kubernetesTasks.workers.extraEnv | default list) }}
{{- $_ := set $k8s "autoscaling" (dict "enabled" false) }}
{{- $_ := set $pools "k8s" $k8s }}
{{- end }}
{{- toYaml $pools }}
{{- end }}

{{/* Whether a pool runs k8s tasks (and so holds Kubernetes credentials): include ... (list $ $pool) */}}
{{- define "conductor.poolRunsK8s" -}}
{{- $ := index . 0 }}{{ $pool := index . 1 }}
{{- $types := $pool.taskTypes | default list }}
{{- if has "k8s" $types }}
{{- if not $.Values.kubernetesTasks.enabled }}{{ fail "a pool runs k8s tasks: set kubernetesTasks.enabled" }}{{ end }}
{{- if or (has "shell" $types) (has "container" $types) }}{{ fail "a pool that runs k8s tasks can't also run shell or container tasks: they could read its Kubernetes credentials" }}{{ end }}
true
{{- end }}
{{- end }}

{{- define "conductor.workerName" -}}
{{- $ := index . 0 }}{{ $name := index . 1 }}
{{- if eq $name "default" }}{{ include "conductor.fullname" $ }}-worker
{{- else }}{{ include "conductor.fullname" $ }}-worker-{{ $name }}{{ end }}
{{- end }}

{{/* The labels a pool's workers advertise, task types included: what autoscaling matches tasks against. */}}
{{- define "conductor.poolAdvertisedLabels" -}}
{{- $ := index . 0 }}{{ $name := index . 1 }}{{ $pool := index . 2 }}
{{- $labels := dict "pool" $name }}
{{- range $k, $v := $pool.labels }}{{ if not (hasKey $labels $k) }}{{ $_ := set $labels $k (toString $v) }}{{ end }}{{ end }}
{{- $types := $pool.taskTypes | default (list "shell" "http" "llm") }}
{{- range $types }}
{{- if and (eq . "llm") (not (include "conductor.llmEnabled" $)) }}
{{- else if ne . "container" }}{{ $_ := set $labels (printf "type.%s" .) "true" }}{{ end }}
{{- end }}
{{- toJson $labels }}
{{- end }}

{{- define "conductor.taskNamespace" -}}
{{- .Values.kubernetesTasks.namespace | default .Release.Namespace }}
{{- end }}

{{/* A pool is dedicated when dedicated is true (or the string "true"). */}}
{{- define "conductor.poolDedicated" -}}
{{- if eq (toString .dedicated) "true" }}true{{ end }}
{{- end }}

{{- define "conductor.anyPoolAutoscales" -}}
{{- range $name, $p := include "conductor.workerPools" . | fromYaml }}{{ if $p.autoscaling.enabled }}true{{ end }}{{ end }}
{{- end }}

{{/* NetworkPolicy rules. */}}
{{- define "conductor.np.metrics" -}}
- ports: [{port: 9090}]
  {{- with .Values.networkPolicy.metricsFrom }}
  from:
    {{- toYaml . | nindent 4 }}
  {{- end }}
{{- end }}
{{- define "conductor.np.from" -}}
- podSelector:
    matchLabels:
      {{- include "conductor.componentSelector" . | nindent 6 }}
{{- end }}
