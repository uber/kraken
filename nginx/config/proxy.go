package config

// ProxyTmpl is the default proxy nginx tmpl.
const ProxyTmpl = `
upstream registry {
  server {{.registry_server}};
}

upstream registry-override {
  server {{.registry_override_server}};
}

{{range .ports}}
server {
  listen {{.}};

  {{$.client_verification}}

  client_max_body_size 10G;

  access_log {{$.log_dir}}/nginx-access.log json;
  error_log {{$.log_dir}}/nginx-error.log;

  # Committing large blobs might take a while.
  proxy_read_timeout 3m;

  location /v2/_catalog {
    proxy_pass http://registry-override;

    set $hostheader $hostname;
    if ( $host = "localhost" ) {
      set $hostheader "localhost";
    }
    if ( $host = "127.0.0.1" ) {
      set $hostheader "127.0.0.1";
    }
    if ( $host = "192.168.65.1" ) {
      set $hostheader "192.168.65.1";
    }
    if ( $host = "host.docker.internal" ) {
      set $hostheader "host.docker.internal";
    }
    proxy_set_header Host $hostheader:{{.}};
  }

  location / {
    proxy_pass http://registry;

    set $hostheader $hostname;
    if ( $host = "localhost" ) {
      set $hostheader "localhost";
    }
    if ( $host = "127.0.0.1" ) {
      set $hostheader "127.0.0.1";
    }
    if ( $host = "192.168.65.1" ) {
      set $hostheader "192.168.65.1";
    }
    if ( $host = "host.docker.internal" ) {
      set $hostheader "host.docker.internal";
    }
    proxy_set_header Host $hostheader:{{.}};
  }
}
{{end}}
`
