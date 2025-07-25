// Copyright (c) 2016-2019 Uber Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package config

// AgentTemplate is the default agent nginx tmpl.
var AgentTemplate = `
upstream registry-backend {
  server {{.registry_server}};
  {{if ne .registry_backup ""}} server {{.registry_backup}} backup; {{end}}
}

upstream agent-server {
  server {{.agent_server}};
}

server {
  listen {{.port}};

  {{range .allowed_cidrs}}
    allow {{.}};
  {{end}}
  deny all;

  {{.client_verification}}

  access_log {{.access_log_path}};
  error_log {{.error_log_path}};

  gzip on;
  gzip_types text/plain test/csv application/json;

  # Timeout configurations from agent server config
  proxy_connect_timeout {{.readiness_timeout}};
  proxy_send_timeout {{.download_timeout}};
  proxy_read_timeout {{.download_timeout}};

  location ~ ^/(health|readiness)$ {
    proxy_pass http://agent-server;
    
    # Use shorter timeout for health checks
    proxy_read_timeout {{.readiness_timeout}};
    proxy_send_timeout {{.readiness_timeout}};
  }

  # Container runtime operations (preload/pull) need longer timeouts
  location ~ ^/preload/ {
    proxy_pass http://agent-server;
    
    # Use container runtime timeout for these operations
    proxy_read_timeout {{.container_runtime_timeout}};
    proxy_send_timeout {{.container_runtime_timeout}};
  }

  # Download operations
  location ~ ^/namespace/.*/blobs/ {
    proxy_pass http://agent-server;
    
    # Use download timeout for blob operations
    proxy_read_timeout {{.download_timeout}};
    proxy_send_timeout {{.download_timeout}};
  }

  location / {
    proxy_pass http://registry-backend;
    proxy_next_upstream error timeout http_404 http_500;
    
    # Standard timeouts for registry operations
    proxy_read_timeout {{.download_timeout}};
    proxy_send_timeout {{.download_timeout}};
  }
}
`
