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
package containerruntime

import (
	"fmt"

	"github.com/uber/kraken/lib/containerruntime/containerd"
	"github.com/uber/kraken/lib/containerruntime/dockerdaemon"
)

type Config struct {
	Docker     dockerdaemon.Config `yaml:"docker"`
	Containerd containerd.Config   `yaml:"containerd"`
}

type Factory interface {
	DockerClient() dockerdaemon.DockerClient
	ContainerdClient() containerd.Client
}

type Impl struct {
	config           Config
	dockerClient     dockerdaemon.DockerClient
	containerdClient containerd.Client
}

func NewFactory(config Config, registry string) (*Impl, error) {
	d, err := dockerdaemon.NewDockerClient(config.Docker, registry)
	if err != nil {
		return nil, fmt.Errorf("new docker client: %s", err)
	}
	c := containerd.New(config.Containerd, registry)
	return &Impl{config, d, c}, nil
}

func (f *Impl) DockerClient() dockerdaemon.DockerClient {
	return f.dockerClient
}

func (f *Impl) ContainerdClient() containerd.Client {
	return f.containerdClient
}
