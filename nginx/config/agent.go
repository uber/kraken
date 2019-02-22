package config

// AgentTemplate is the default agent nginx tmpl.
var AgentTemplate = `
upstream registry-backend {
  server 127.0.0.1:8991; # TODO(codyg): Change this to unix socket.
  {{if ne .registry_backup ""}} server {{.registry_backup}} backup; {{end}}
}

server {
  listen {{.port}};

  {{.client_verification}}

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
