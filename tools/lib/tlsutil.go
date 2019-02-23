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
package lib

import (
	"crypto/tls"
	"fmt"
	"io/ioutil"

	"github.com/uber/kraken/utils/httputil"

	"gopkg.in/yaml.v2"
)

// ReadTLSFile reads config file in path and returns *tls.Config.
// It returns nil when path is nil.
func ReadTLSFile(path *string) (*tls.Config, error) {
	if path == nil {
		return nil, nil
	}
	data, err := ioutil.ReadFile(*path)
	if err != nil {
		return nil, fmt.Errorf("read tls config: %s", err)
	}
	var config httputil.TLSConfig
	if err := yaml.Unmarshal(data, &config); err != nil {
		return nil, fmt.Errorf("unmarshal tls config: %s", err)
	}
	tls, err := config.BuildClient()
	if err != nil {
		return nil, fmt.Errorf("build tls client: %s", err)
	}
	return tls, nil
}
