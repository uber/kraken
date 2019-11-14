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
package dockerdaemon

import (
	"context"
	"fmt"
	"net/url"
)

// ImagePull calls `docker pull` on an image
func (cli *dockerClient) ImagePull(ctx context.Context, registry, repo, tag string) error {
	v := url.Values{}
	fromImage := repo
	if registry != "" {
		fromImage = fmt.Sprintf("%s/%s", registry, repo)
	}
	v.Set("fromImage", fromImage)
	v.Set("tag", tag)
	headers := map[string][]string{"X-Registry-Auth": {""}}
	return cli.post(ctx, "/images/create", v, nil, headers, true)
}
