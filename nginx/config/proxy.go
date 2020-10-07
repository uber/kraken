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

// ProxyTemplate is the default proxy nginx tmpl.
const ProxyTemplate = `
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

  access_log {{$.access_log_path}} json;
  error_log {{$.error_log_path}};

  gzip on;
  gzip_types text/plain test/csv application/json;

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
