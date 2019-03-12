{{/* vim: set filetype=mustache: */}}


{{- define "trackers" -}}
tracker:
  hosts:
    static:
{{- range $i, $e := until (int .Values.num_trackers) }}
      - kraken-tracker-{{$i}}:80
{{- end -}}
{{- end -}}



{{- define "origins" -}}
origin:
  hosts:
    static:
{{- range $i, $e := until (int .Values.num_origins) }}
      - kraken-origin-{{$i}}:80
{{- end -}}
{{- end -}}

{{- define "origin-cluster" -}}
cluster:
  static:
{{- range $i, $e := until (int .Values.num_origins) }}
    - kraken-origin-{{$i}}:80
{{- end -}}
{{- end -}}



{{- define "build-index" -}}
build_index:
  hosts:
    static:
{{- range $i, $e := until (int .Values.num_build_index) }}
      - kraken-build-index-{{$i}}:80
{{- end -}}
{{- end -}}

{{- define "build-index-cluster" -}}
cluster:
  hosts:
    static:
{{- range $i, $e := until (int .Values.num_build_index) }}
      - kraken-build-index-{{$i}}:80
{{- end -}}
{{- end -}}



{{- define "library-namespace" -}}
- namespace: library/.*
  backend:
    registry_blob:
      address: index.docker.io
      security:
        basic:
          username: ""
          password: ""
{{- end -}}

{{- define "tls" -}}
tls:
  client:
    disabled: true
  server:
    disabled: true
{{- end -}}
