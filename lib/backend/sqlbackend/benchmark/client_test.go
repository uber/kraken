// Copyright (c) 2016-2020 Uber Technologies, Inc.
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
package benchmark

import (
	"bytes"
	"fmt"
	"log"
	"math/rand"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber/kraken/core"
	"github.com/uber/kraken/lib/backend/backenderrors"
	"github.com/uber/kraken/lib/backend/sqlbackend"
)

var sqlClient *sqlbackend.Client

const maxTags = 100000
const maxRepos = 100000
const maxTagsPerRepo = 100

// If desired, you can change the connection string and dialect to run the benchmarks against a real database:
// const dialect = "mysql"
// const connString = "kraken:kraken@tcp(localhost:3306)/kraken?parseTime=True"
const dialect = "sqlite3"
const connString = ":memory:"

// We don't generate the test data here because when running bazel test, this method will get executed even though there
// are no non-benchmark tests. Instead, each benchmark test will call generateTestData(), which will load the database
// if it is not already loaded. After this, we reset the benchmark timers before beginning the test.
func TestMain(m *testing.M) {
	c, err := sqlbackend.NewClient(sqlbackend.Config{Dialect: dialect, ConnectionString: connString}, sqlbackend.UserAuthConfig{})
	if err != nil {
		panic(err)
	}
	sqlClient = c
	os.Exit(m.Run())
}

func generateTestData() {
	res, err := sqlClient.Stat("", "many-tags:tag0")
	if err != nil && err.Error() != backenderrors.ErrBlobNotFound.Error() {
		panic(err)
	}

	if res == nil {
		// create repository with many tags
		for i := 0; i < maxTags; i++ {
			r := strings.NewReader(core.DigestFixture().String())
			if err := sqlClient.Upload("", fmt.Sprintf("many-tags:tag%d", i), r); err != nil {
				panic(err)
			}
		}
		// create many repositories with a random number of tags
		for i := 0; i < maxRepos; i++ {
			for j := 0; j < rand.Intn(maxTagsPerRepo)+1; j++ {
				r := strings.NewReader(core.DigestFixture().String())
				if err := sqlClient.Upload("", fmt.Sprintf("hello/world%d:tag%d", i, j), r); err != nil {
					panic(err)
				}
			}
		}
	}
}

func BenchmarkStat(b *testing.B) {
	generateTestData()
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		if _, err := sqlClient.Stat("", fmt.Sprintf("many-tags:tag%d", rand.Intn(maxTags))); err != nil {
			if err != backenderrors.ErrBlobNotFound {
				assert.Fail(b, "unknown error", err)
			}
		}
	}
}

func BenchmarkDownload(b *testing.B) {
	generateTestData()
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		w := new(bytes.Buffer)
		if err := sqlClient.Download("", fmt.Sprintf("many-tags:tag%d", rand.Intn(maxTags)), w); err != nil {
			if err != backenderrors.ErrBlobNotFound {
				assert.Fail(b, "unknown error", err)
			}
		}
	}
}

func BenchmarkListCatalog(b *testing.B) {
	generateTestData()
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		start := time.Now()
		res, err := sqlClient.List("")
		if err != nil && err != backenderrors.ErrBlobNotFound {
			assert.Fail(b, "unknown error", err)
		}
		require.NotNil(b, res)
		assert.True(b, len(res.Names) >= maxRepos)
		elapsed := time.Since(start)
		log.Printf("Docker catalog took %s for %d repos", elapsed, len(res.Names))
	}
}

func BenchmarkListTags(b *testing.B) {
	generateTestData()
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		start := time.Now()
		res, err := sqlClient.List("many-tags/_manifests/tags")
		if err != nil && err != backenderrors.ErrBlobNotFound {
			assert.Fail(b, "unknown error", err)
		}
		require.NotNil(b, res)
		assert.True(b, len(res.Names) >= maxTags)
		elapsed := time.Since(start)
		log.Printf("Docker tag list took %s for %d tags", elapsed, len(res.Names))
	}
}
