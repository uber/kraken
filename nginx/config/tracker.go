package config

// TrackerTmpl is the default tracker nginx tmpl.
const TrackerTmpl = `
proxy_cache_path {{.cache_dir}}/metainfo levels=1:2 keys_zone=metainfo:10m max_size=256g;

upstream tracker {
  server {{.server}};
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

  access_log {{.log_dir}}/nginx-access.log;
  error_log {{.log_dir}}/nginx-error.log;

  location / {
    proxy_pass http://tracker;
  }

  location ~* ^/namespace/.*/blobs/.*/metainfo$ {
    proxy_pass http://tracker;

    proxy_cache         metainfo;
    proxy_cache_methods GET;
    proxy_cache_valid   200 5m;
    proxy_cache_valid   any 1s;
    proxy_cache_lock    on;
  }
}
`
