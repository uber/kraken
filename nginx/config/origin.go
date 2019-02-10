package config

// OriginTmpl is the default origin nginx tmpl.
const OriginTmpl = `
server {
  listen {{.port}};

  {{.client_verification}}

  client_max_body_size 10G;

  access_log {{.log_dir}}/nginx-access.log;
  error_log {{.log_dir}}/nginx-error.log;

  location / {
    proxy_pass http://{{.server}};
  }
}
`
