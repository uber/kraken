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

// TrackerTemplate is the default tracker nginx tmpl.
const TrackerTemplate = `
proxy_cache_path {{.cache_dir}}/metainfo levels=1:2 keys_zone=metainfo:10m max_size=256g;

upstream tracker {
  server {{.server}};
}

server {
  listen {{.port}};

  {{.client_verification}}

  access_log {{.access_log_path}};
  error_log {{.error_log_path}};

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
