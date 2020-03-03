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
package s3backend

import (
	"github.com/c2h5oh/datasize"

	"github.com/uber/kraken/lib/backend"
)

// Config defines s3 connection specific
// parameters and authetication credentials
type Config struct {
	Username         string `yaml:"username"`         // IAM username for selecting credentials.
	Region           string `yaml:"region"`           // AWS S3 region
	Bucket           string `yaml:"bucket"`           // S3 bucket
	Endpoint         string `yaml:"endpoint"`         // S3 endpoint
	DisableSSL       bool   `yaml:"disable_ssl"`      // use clear HTTP when talking to endpoint
	S3ForcePathStyle bool   `yaml:"force_path_style"` // use path style instead of DNS style

	RootDirectory    string `yaml:"root_directory"`     // S3 root directory for docker images
	UploadPartSize   int64  `yaml:"upload_part_size"`   // part size s3 manager uses for upload
	DownloadPartSize int64  `yaml:"download_part_size"` // part size s3 manager uses for download

	UploadConcurrency   int `yaml:"upload_concurrency"`   // # of concurrent go-routines s3 manager uses for upload
	DownloadConcurrency int `yaml:"download_concurrency"` // # of concurrent go-routines s3 manager uses for download

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
	S3 struct {
		AccessKeyID     string `yaml:"aws_access_key_id"`
		AccessSecretKey string `yaml:"aws_secret_access_key"`
		SessionToken    string `yaml:"aws_session_token"`
	} `yaml:"s3"`
}

func (c *Config) applyDefaults() {
	if c.UploadPartSize == 0 {
		c.UploadPartSize = backend.DefaultPartSize
	}
	if c.DownloadPartSize == 0 {
		c.DownloadPartSize = backend.DefaultPartSize
	}
	if c.UploadConcurrency == 0 {
		c.UploadConcurrency = backend.DefaultConcurrency
	}
	if c.DownloadConcurrency == 0 {
		c.DownloadConcurrency = backend.DefaultConcurrency
	}
	if c.BufferGuard == 0 {
		c.BufferGuard = backend.DefaultBufferGuard
	}
	if c.ListMaxKeys == 0 {
		c.ListMaxKeys = backend.DefaultListMaxKeys
	}
}
