{{/* vim: set filetype=mustache: */}}


{{- define "trackers" -}}
tracker:
  hosts:
    dns: kraken-tracker:80
{{- end -}}



{{- define "origins" -}}
origin:
  hosts:
    dns: kraken-origin:80
{{- end -}}

{{- define "origin-cluster" -}}
cluster:
  dns: kraken-origin:80
{{- end -}}



{{- define "build-index" -}}
build_index:
  hosts:
    dns: kraken-build-index:80
{{- end -}}

{{- define "build-index-cluster" -}}
cluster:
  hosts:
    dns: kraken-build-index:80
{{- end -}}



{{- define "tls" -}}
tls:
  client:
    disabled: true
  server:
    disabled: true
{{- end -}}
