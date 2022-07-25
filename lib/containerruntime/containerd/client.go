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
// limitations under the License.ackage containerd
package containerd

import (
	"context"
	"fmt"

	"github.com/containerd/containerd"
)

type Client interface {
	PullImage(context.Context, string, string, string) error
}

type Impl struct {
	config   Config
	registry string
}

func New(config Config, registry string) *Impl {
	return &Impl{config.applyDefaults(), registry}
}

func (c *Impl) PullImage(ctx context.Context, ns, repo, tag string) error {
	client, err := containerd.New(c.config.Address, containerd.WithDefaultNamespace(ns))
	if err != nil {
		return fmt.Errorf("new containerd client: %s", err)
	}
	defer client.Close()

	_, err = client.Pull(ctx, fmt.Sprintf("%s/%s:%s", c.registry, repo, tag))
	if err != nil {
		return fmt.Errorf("containerd pull image: %s", err)
	}
	return nil
}
