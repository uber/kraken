package config

// BuildIndexTemplate is the default build-index nginx tmpl.
const BuildIndexTemplate = `
proxy_cache_path {{.cache_dir}}/tags keys_zone=tags:20m;
proxy_cache_path {{.cache_dir}}/repositories keys_zone=repositories:20m;
proxy_cache_path {{.cache_dir}}/list keys_zone=list:20m;

upstream build-index {
  server {{.server}};
}

server {
  listen {{.port}};

  {{.client_verification}}

  access_log {{.log_dir}}/nginx-access.log;
  error_log {{.log_dir}}/nginx-error.log;

  location / {
    proxy_pass http://build-index;
  }

  location /tags {
    proxy_pass http://build-index;

    proxy_cache         tags;
    proxy_cache_methods GET;
    proxy_cache_valid   200 5m;
    proxy_cache_valid   any 1s;
    proxy_cache_lock    on;
  }

  location ~* ^/repositories/.*/tags$ {
    proxy_pass http://build-index;

    proxy_cache         repositories;
    proxy_cache_methods GET;
    proxy_cache_valid   any 1s;
    proxy_cache_lock    on;
  }

  location /list {
    proxy_pass http://build-index;

    proxy_cache         list;
    proxy_cache_methods GET;
    proxy_cache_valid   200 30s;
    proxy_cache_valid   any 1s;
    proxy_cache_lock    on;

    proxy_read_timeout 2m;
  }
}
`
