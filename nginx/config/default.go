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

import (
	"fmt"
)

var _nameToDefaultTemplate = map[string]string{
	"base":               BaseTemplate,
	"kraken-agent":       AgentTemplate,
	"kraken-origin":      OriginTemplate,
	"kraken-build-index": BuildIndexTemplate,
	"kraken-tracker":     TrackerTemplate,
	"kraken-proxy":       ProxyTemplate,
}

// DefaultClientVerification is the default nginx configuration for
// client verification in the server block.
const DefaultClientVerification = `
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
`

// GetDefaultTemplate returns the tmpl given name.
func GetDefaultTemplate(name string) (string, error) {
	if tmpl, ok := _nameToDefaultTemplate[name]; ok {
		return tmpl, nil
	}
	return "", fmt.Errorf("name not found")
}
