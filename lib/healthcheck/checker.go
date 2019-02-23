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
package healthcheck

import (
	"context"
	"crypto/tls"
	"fmt"

	"github.com/uber/kraken/utils/httputil"
)

// Checker runs a health check against an address.
type Checker interface {
	Check(ctx context.Context, addr string) error
}

// Default returns a Checker which makes a GET request against /health.
func Default(tls *tls.Config) Checker {
	return defaultChecker{tls}
}

type defaultChecker struct{ tls *tls.Config }

func (c defaultChecker) Check(ctx context.Context, addr string) error {
	_, err := httputil.Get(
		fmt.Sprintf("http://%s/health", addr),
		httputil.SendContext(ctx),
		httputil.SendTLS(c.tls))
	return err
}
