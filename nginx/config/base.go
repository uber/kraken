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

// BaseTemplate defines the nginx template which all components share.
const BaseTemplate = `
worker_processes 4;
worker_rlimit_nofile 4096;
pid /tmp/nginx.pid;
user root root;

events {
  worker_connections 2048;
  # multi_accept on;
}

http {

  ##
  # Basic Settings
  ##

  sendfile on;
  tcp_nopush on;
  tcp_nodelay on;
  keepalive_timeout 65;
  types_hash_max_size 2048;
  # server_tokens off;

  # server_names_hash_bucket_size 64;
  # server_name_in_redirect off;

  include /etc/nginx/mime.types;
  default_type application/octet-stream;

  ##
  # Proxy Settings
  ##

  proxy_set_header  X-Forwarded-For   $proxy_add_x_forwarded_for;
  proxy_set_header  X-Forwarded-Proto $http_x_forwarded_proto;
  proxy_set_header  X-Real-IP         $remote_addr;
  proxy_set_header  X-Original-URI    $request_uri;

  # Overwrites http with $scheme if Location header is set to http by upstream.
  proxy_redirect ~^http://[^:]+:\d+(/.+)$ $1;

  ##
  # SSL Settings
  ##

  {{if .ssl_enabled}}
    ssl on;
    ssl_certificate {{.ssl_certificate}};
    ssl_certificate_key {{.ssl_certificate_key}};
    {{if .ssl_password_file}}
      ssl_password_file {{.ssl_password_file}};
    {{end}}

    # This is important to enforce client to use certificate.
    # The client of nginx cannot use a self-signed cert.
    ssl_verify_client on;
    ssl_client_certificate {{.ssl_client_certificate}};
  {{end}}
  ssl_protocols TLSv1 TLSv1.1 TLSv1.2; # Dropping SSLv3, ref: POODLE
  ssl_prefer_server_ciphers on;
  ssl_ciphers ECDH+AES256:ECDH+AES128:DH+3DES:!ADH:!AECDH:!MD5@SECLEVEL=1;

  ##
  # Logging Settings
  ##

  # JSON log_format
  log_format json '{'
       '"verb":"$request_method",'
       '"path":"$request_uri",'
       '"bytes":$request_length,'
       '"request_scheme":"$scheme",'
       '"request_port":$server_port,'
       '"request_host":"$http_host",'
       '"clientip":"$remote_addr",'
       '"agent":"$http_user_agent",'
       '"response_redirect_location":"$sent_http_location",'
       '"response_length":$bytes_sent,'
       '"response_body_length":$body_bytes_sent,'
       '"responseStatusCode":"$status",'
       '"responseTime":$request_time,'
       '"esStatusCode":"$status",'
       '"content_type":"$content_type",'
       '"email":"$http_x_auth_params_email",'
       '"uberSource":"$http_x_uber_source",'
       '"callsite":"$http_x_uber_callsite",'
       '"app":"$http_x_uber_app",'
       '"request":"$request_uri",'
       '"connection":"$connection",'
       '"connection_requests":$connection_requests,'
       '"@timestamp":"$time_iso8601",'
       '"@source_host":"$hostname",'
       '"referer":"$http_referer",'
       '"service_name":"kraken",'
       '"message":"access log",'
       '"logtype":"access_log",'
       '"proxy_type":"nginx",'
       '"server_protocol":"$server_protocol",'
       '"proxy_host": "$proxy_host",'
       '"upstream_address":"$upstream_addr",'
       '"upstream_response_time":"$upstream_response_time"'
     '}';

  ##
  # Gzip Settings
  ##

  gzip off;
  gzip_disable "msie6";

  # gzip_vary on;
  # gzip_proxied any;
  # gzip_comp_level 6;
  # gzip_buffers 16 8k;
  # gzip_http_version 1.1;

  gzip_types text/plain text/css application/json application/javascript text/xml application/xml application/xml+rss text/javascript;

  ##
  # Virtual Host Configs
  ##

  include /etc/nginx/conf.d/*.conf;

  {{.site}}
}
`
