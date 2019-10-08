// Copyright (c) 2016-2019 Uber Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package gcsbackend

import (
	"github.com/c2h5oh/datasize"

	"github.com/uber/kraken/lib/backend"
)

// Config defines gcs connection specific
// parameters and authetication credentials
type Config struct {
	Username string `yaml:"username"` // IAM username for selecting credentials.
	Location string `yaml:"location"` // Location of the bucket. Defautls to "US".
	Bucket   string `yaml:"bucket"`   // GCS bucket

	RootDirectory   string `yaml:"root_directory"`   // GCS root directory for docker images
	UploadChunkSize int64  `yaml:"upload_part_size"` // part size gcs manager uses for upload

	// ListMaxKeys sets the max keys returned per page.
	ListMaxKeys int `yaml:"list_max_keys"`

	// BufferGuard protects download from downloading into an oversized buffer
	// when io.WriterAt is not implemented.
	BufferGuard datasize.ByteSize `yaml:"buffer_guard"`

	// NamePath identifies which namepath.Pather to use.
	NamePath string `yaml:"name_path"`
}

// UserAuthConfig defines authentication configuration overlayed by Langley.
// Each key is the iam username of the credentials.
type UserAuthConfig map[string]AuthConfig

// AuthConfig matches Langley format.
type AuthConfig struct {
	GCS struct {
		AccessBlob string `yaml:"access_blob"`
	} `yaml:"gcs"`
}

func (c *Config) applyDefaults() {
	if c.UploadChunkSize == 0 {
		c.UploadChunkSize = backend.DefaultPartSize
	}
	if c.BufferGuard == 0 {
		c.BufferGuard = backend.DefaultBufferGuard
	}
	if c.ListMaxKeys == 0 {
		c.ListMaxKeys = backend.DefaultListMaxKeys
	}
}
