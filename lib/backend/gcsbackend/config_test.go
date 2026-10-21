// Copyright (c) 2016-2019 Uber Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package gcsbackend

import (
	"testing"

	"github.com/c2h5oh/datasize"
	"github.com/stretchr/testify/require"
	"github.com/uber/kraken/lib/backend"
	"gopkg.in/yaml.v2"
)

func TestConfigApplyDefaults(t *testing.T) {
	require := require.New(t)

	var config Config
	config.applyDefaults()

	require.Equal(backend.DefaultPartSize, config.UploadChunkSize)
	require.Equal(backend.DefaultBufferGuard, config.BufferGuard)
	require.Equal(backend.DefaultListMaxKeys, config.ListMaxKeys)
	require.Equal(backend.DefaultConcurrency, config.DownloadConcurrency)
	require.Equal(backend.DefaultPartSize, config.DownloadPartSize)
	require.Equal(600, config.DownloadTimeoutSeconds)
}

func TestConfigApplyDefaultsKeepsConfiguredValues(t *testing.T) {
	require := require.New(t)

	config := Config{
		UploadChunkSize:        1,
		BufferGuard:            datasize.ByteSize(2),
		ListMaxKeys:            3,
		DownloadConcurrency:    4,
		DownloadPartSize:       5,
		DownloadTimeoutSeconds: 6,
	}
	config.applyDefaults()

	require.Equal(int64(1), config.UploadChunkSize)
	require.Equal(datasize.ByteSize(2), config.BufferGuard)
	require.Equal(3, config.ListMaxKeys)
	require.Equal(4, config.DownloadConcurrency)
	require.Equal(int64(5), config.DownloadPartSize)
	require.Equal(6, config.DownloadTimeoutSeconds)
}

func TestConfigUnmarshal(t *testing.T) {
	require := require.New(t)

	raw := `
username: test-user
location: US
bucket: test-bucket
root_directory: root
upload_part_size: 1024
list_max_keys: 50
buffer_guard: 10MB
name_path: identity
download_concurrency: 8
download_part_size: 2048
private_service_connect: https://psc.example.com
download_timeout_secs: 30
`
	var config Config
	require.NoError(yaml.Unmarshal([]byte(raw), &config))

	require.Equal("test-user", config.Username)
	require.Equal("US", config.Location)
	require.Equal("test-bucket", config.Bucket)
	require.Equal("root", config.RootDirectory)
	require.Equal(int64(1024), config.UploadChunkSize)
	require.Equal(50, config.ListMaxKeys)
	require.Equal(10*datasize.MB, config.BufferGuard)
	require.Equal("identity", config.NamePath)
	require.Equal(8, config.DownloadConcurrency)
	require.Equal(int64(2048), config.DownloadPartSize)
	require.Equal("https://psc.example.com", config.PrivateServiceConnect)
	require.Equal(30, config.DownloadTimeoutSeconds)
}

func TestAuthConfigUnmarshal(t *testing.T) {
	require := require.New(t)

	raw := `
test-user:
  gcs:
    access_blob: some-access-blob
`
	var userAuth UserAuthConfig
	require.NoError(yaml.Unmarshal([]byte(raw), &userAuth))

	require.Equal("some-access-blob", userAuth["test-user"].GCS.AccessBlob)
}
