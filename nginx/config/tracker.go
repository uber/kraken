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

  # Timeout configurations from tracker server config
  proxy_connect_timeout {{.readiness_timeout}};
  proxy_send_timeout {{.announce_timeout}};
  proxy_read_timeout {{.announce_timeout}};

  location / {
    proxy_pass http://tracker;
    
    # Pass original client info
    proxy_set_header Host $host;
    proxy_set_header X-Real-IP $remote_addr;
    proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
    proxy_set_header X-Forwarded-Proto $scheme;
  }

  # Health and readiness checks with shorter timeout
  location ~ ^/(health|readiness)$ {
    proxy_pass http://tracker;
    
    proxy_read_timeout {{.readiness_timeout}};
    proxy_send_timeout {{.readiness_timeout}};
    
    # Pass original client info
    proxy_set_header Host $host;
    proxy_set_header X-Real-IP $remote_addr;
    proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
    proxy_set_header X-Forwarded-Proto $scheme;
  }

  # Metainfo requests need longer timeout (cached)
  location ~* ^/namespace/.*/blobs/.*/metainfo$ {
    proxy_pass http://tracker;

    proxy_cache         metainfo;
    proxy_cache_methods GET;
    proxy_cache_valid   200 5m;
    proxy_cache_valid   any 1s;
    proxy_cache_lock    on;
    
    # Use metainfo timeout for these operations
    proxy_read_timeout {{.metainfo_timeout}};
    proxy_send_timeout {{.metainfo_timeout}};
    
    # Pass original client info
    proxy_set_header Host $host;
    proxy_set_header X-Real-IP $remote_addr;
    proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
    proxy_set_header X-Forwarded-Proto $scheme;
  }

  # Announce operations
  location ~ ^/announce {
    proxy_pass http://tracker;
    
    # Use announce timeout for these operations
    proxy_read_timeout {{.announce_timeout}};
    proxy_send_timeout {{.announce_timeout}};
    
    # Pass original client info
    proxy_set_header Host $host;
    proxy_set_header X-Real-IP $remote_addr;
    proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
    proxy_set_header X-Forwarded-Proto $scheme;
  }
}
`
