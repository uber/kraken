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
package hdfsbackend

import "github.com/uber/kraken/lib/backend/hdfsbackend/webhdfs"

// Config defines configuration for all HDFS clients.
type Config struct {
	NameNodes     []string `yaml:"namenodes"`
	UserName      string   `yaml:"username"`
	RootDirectory string   `yaml:"root_directory"`

	// ListConcurrency is the number of threads used for listing.
	ListConcurrency int `yaml:"list_concurrency"`

	// NamePath identifies which namepath.Pather to use.
	NamePath string `yaml:"name_path"`

	// UploadDirectory is scratch space, relative to RootDirectory, used for
	// uploading files before moving them to content-addressable storage. Avoids
	// partial uploads corrupting the content-addressable storage space.
	UploadDirectory string `yaml:"upload_directory"`

	WebHDFS webhdfs.Config `yaml:"webhdfs"`

	// Enables test-only behavior.
	testing bool
}

func (c *Config) applyDefaults() {
	if c.RootDirectory == "" {
		c.RootDirectory = "/infra/dockerRegistry/"
	}
	if c.ListConcurrency == 0 {
		c.ListConcurrency = 16
	}
	if c.UploadDirectory == "" {
		c.UploadDirectory = "_uploads"
	}
}
