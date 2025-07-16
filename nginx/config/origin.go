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

// OriginTemplate is the default origin nginx tmpl.
const OriginTemplate = `
server {
  listen {{.port}};

  {{.client_verification}}

  client_max_body_size 10G;

  access_log {{.access_log_path}} json;
  error_log {{.error_log_path}};

  gzip on;
  gzip_types text/plain test/csv application/json;

  # Timeout configurations from origin server config
  proxy_connect_timeout {{.backend_timeout}};
  proxy_send_timeout {{.upload_timeout}};
  proxy_read_timeout {{.download_timeout}};
  
  # Keepalive settings
  proxy_buffering off;
  proxy_request_buffering off;

  location / {
    proxy_pass http://{{.server}};
    
    # Pass original client info
    proxy_set_header Host $host;
    proxy_set_header X-Real-IP $remote_addr;
    proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
    proxy_set_header X-Forwarded-Proto $scheme;
  }

  # Special handling for upload operations with longer timeout
  location ~ ^/namespace/.*/blobs/.*/uploads {
    proxy_pass http://{{.server}};
    
    # Use upload timeout for these operations
    proxy_read_timeout {{.upload_timeout}};
    proxy_send_timeout {{.upload_timeout}};
    
    # Pass original client info
    proxy_set_header Host $host;
    proxy_set_header X-Real-IP $remote_addr;
    proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
    proxy_set_header X-Forwarded-Proto $scheme;
  }

  # Replication operations with their own timeout
  location ~ ^/namespace/.*/blobs/.*/remote {
    proxy_pass http://{{.server}};
    
    # Use replication timeout for these operations
    proxy_read_timeout {{.replication_timeout}};
    proxy_send_timeout {{.replication_timeout}};
    
    # Pass original client info
    proxy_set_header Host $host;
    proxy_set_header X-Real-IP $remote_addr;
    proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
    proxy_set_header X-Forwarded-Proto $scheme;
  }
}
`
