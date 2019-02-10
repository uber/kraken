package config

// AgentTmpl is the default agent nginx tmpl.
var AgentTmpl = `
upstream registry-backend {
  server 127.0.0.1:8991; # TODO(codyg): Change this to unix socket.
  {{if ne .registry_backup ""}} server {{.registry_backup}} backup; {{end}}
}

server {
  listen {{.port}};

  ssl_verify_client optional;
  set $required_verified_client 1;
  if ($scheme = http) {
    set $required_verified_client 0;
  }
  if ($request_method ~ ^(GET|HEAD)$) {
    set $required_verified_client 0;
  }
  if ($remote_addr = "127.0.0.1") {
    set $required_verified_client 0;
  }

  set $verfied_client $required_verified_client$ssl_client_verify;
  if ($verfied_client !~ ^(0.*|1SUCCESS)$) {
    return 403;
  }

  access_log {{.log_dir}}/nginx-access.v2.log;
  error_log {{.log_dir}}/nginx-error.v2.log;

  gzip on;
  gzip_types text/plain test/csv application/json;

  location / {
    proxy_pass http://registry-backend;
    proxy_next_upstream error timeout http_404 http_500;
  }
}
`
