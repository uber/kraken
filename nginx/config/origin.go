package config

// OriginTmpl is the default origin nginx tmpl.
const OriginTmpl = `
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

  client_max_body_size 10G;

  access_log {{.log_dir}}/nginx-access.log;
  error_log {{.log_dir}}/nginx-error.log;

  location / {
    proxy_pass http://{{.server}};
  }
}
`
