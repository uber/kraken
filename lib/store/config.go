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
package store

// Volume - if provided, volumes are used to store the actual files.
// Symlinks will be created under state directories.
// This configuration is needed on hosts with multiple disks.
type Volume struct {
	Location string
	Weight   int
}

// CAStoreConfig defines CAStore configuration.
type CAStoreConfig struct {
	UploadDir     string        `yaml:"upload_dir"`
	CacheDir      string        `yaml:"cache_dir"`
	Volumes       []Volume      `yaml:"volumes"`
	Capacity      int           `yaml:"capacity"`
	UploadCleanup CleanupConfig `yaml:"upload_cleanup"`
	CacheCleanup  CleanupConfig `yaml:"cache_cleanup"`

	SkipHashVerification bool `yaml:"skip_hash_verification"`
}

func (c CAStoreConfig) applyDefaults() CAStoreConfig {
	if c.Capacity == 0 {
		c.Capacity = 1 << 20 // 1 million
	}
	return c
}

// SimpleStoreConfig defines SimpleStore configuration.
type SimpleStoreConfig struct {
	UploadDir     string        `yaml:"upload_dir"`
	CacheDir      string        `yaml:"cache_dir"`
	UploadCleanup CleanupConfig `yaml:"upload_cleanup"`
	CacheCleanup  CleanupConfig `yaml:"cache_cleanup"`
}

// CADownloadStoreConfig defines CADownloadStore configuration.
// TODO(evelynl94): rename
type CADownloadStoreConfig struct {
	DownloadDir     string        `yaml:"download_dir"`
	CacheDir        string        `yaml:"cache_dir"`
	DownloadCleanup CleanupConfig `yaml:"download_cleanup"`
	CacheCleanup    CleanupConfig `yaml:"cache_cleanup"`
}
