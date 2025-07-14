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
package blobserver

import (
	"time"

	"github.com/uber/kraken/utils/listener"
)

// Config defines the configuration used by Origin cluster for hashing blob digests.
type Config struct {
	Listener                  listener.Config `yaml:"listener"`
	DuplicateWriteBackStagger time.Duration   `yaml:"duplicate_write_back_stagger"`
	
	// Timeout configurations
	DownloadTimeout       time.Duration `yaml:"download_timeout"`
	UploadTimeout         time.Duration `yaml:"upload_timeout"`
	ReplicationTimeout    time.Duration `yaml:"replication_timeout"`
	BackendTimeout        time.Duration `yaml:"backend_timeout"`
	ReadinessTimeout      time.Duration `yaml:"readiness_timeout"`
	
	// Limit configurations
	MaxConcurrentDownloads int `yaml:"max_concurrent_downloads"`
	MaxConcurrentUploads   int `yaml:"max_concurrent_uploads"`
	MaxRequestSize         int64 `yaml:"max_request_size"`
	
	// Retry configurations
	MaxRetries    int           `yaml:"max_retries"`
	RetryDelay    time.Duration `yaml:"retry_delay"`
	RetryMaxDelay time.Duration `yaml:"retry_max_delay"`
}

func (c Config) applyDefaults() Config {
	if c.DuplicateWriteBackStagger == 0 {
		c.DuplicateWriteBackStagger = 30 * time.Minute
	}
	if c.DownloadTimeout == 0 {
		c.DownloadTimeout = 5 * time.Minute
	}
	if c.UploadTimeout == 0 {
		c.UploadTimeout = 10 * time.Minute
	}
	if c.ReplicationTimeout == 0 {
		c.ReplicationTimeout = 3 * time.Minute
	}
	if c.BackendTimeout == 0 {
		c.BackendTimeout = 2 * time.Minute
	}
	if c.ReadinessTimeout == 0 {
		c.ReadinessTimeout = 30 * time.Second
	}
	if c.MaxConcurrentDownloads == 0 {
		c.MaxConcurrentDownloads = 10
	}
	if c.MaxConcurrentUploads == 0 {
		c.MaxConcurrentUploads = 5
	}
	if c.MaxRequestSize == 0 {
		c.MaxRequestSize = 1024 * 1024 * 1024 // 1GB
	}
	if c.MaxRetries == 0 {
		c.MaxRetries = 3
	}
	if c.RetryDelay == 0 {
		c.RetryDelay = 100 * time.Millisecond
	}
	if c.RetryMaxDelay == 0 {
		c.RetryMaxDelay = 5 * time.Second
	}
	return c
}
