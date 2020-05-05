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

import (
	"io/ioutil"
	"os"

	"github.com/uber/kraken/utils/testutil"

	"github.com/uber-go/tally"
)

func tempdir(cleanup *testutil.Cleanup, name string) string {
	d, err := ioutil.TempDir("/tmp", name)
	if err != nil {
		panic(err)
	}
	cleanup.Add(func() { os.RemoveAll(d) })
	return d
}

// CAStoreConfigFixture returns config for CAStore for testing purposes.
func CAStoreConfigFixture() (CAStoreConfig, func()) {
	cleanup := &testutil.Cleanup{}
	defer cleanup.Recover()

	upload := tempdir(cleanup, "upload")
	cache := tempdir(cleanup, "cache")

	return CAStoreConfig{
		UploadDir:            upload,
		CacheDir:             cache,
		SkipHashVerification: false,
	}, cleanup.Run
}

// CAStoreFixture returns a CAStore for testing purposes.
func CAStoreFixture() (*CAStore, func()) {
	var cleanup testutil.Cleanup
	defer cleanup.Recover()

	config, c := CAStoreConfigFixture()
	cleanup.Add(c)

	s, err := NewCAStore(config, tally.NoopScope)
	if err != nil {
		panic(err)
	}
	cleanup.Add(s.Close)

	return s, cleanup.Run
}

// CADownloadStoreFixture returns a CADownloadStore for testing purposes.
func CADownloadStoreFixture() (*CADownloadStore, func()) {
	cleanup := &testutil.Cleanup{}
	defer cleanup.Recover()

	download := tempdir(cleanup, "download")
	cache := tempdir(cleanup, "cache")

	config := CADownloadStoreConfig{
		DownloadDir: download,
		CacheDir:    cache,
	}
	s, err := NewCADownloadStore(config, tally.NoopScope)
	if err != nil {
		panic(err)
	}
	cleanup.Add(s.Close)

	return s, cleanup.Run
}

// SimpleStoreFixture returns a SimpleStore for testing purposes.
func SimpleStoreFixture() (*SimpleStore, func()) {
	cleanup := &testutil.Cleanup{}
	defer cleanup.Recover()

	upload := tempdir(cleanup, "upload")
	cache := tempdir(cleanup, "cache")

	config := SimpleStoreConfig{
		UploadDir: upload,
		CacheDir:  cache,
	}
	s, err := NewSimpleStore(config, tally.NoopScope)
	if err != nil {
		panic(err)
	}
	cleanup.Add(s.Close)

	return s, cleanup.Run
}
