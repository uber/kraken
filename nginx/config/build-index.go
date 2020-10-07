// Copyright (c) 2016-2019 Uber Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
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

  access_log {{.access_log_path}};
  error_log {{.error_log_path}};

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
