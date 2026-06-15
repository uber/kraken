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
package testfs

import (
	"bytes"
	"testing"

	"github.com/uber-go/tally"
	"github.com/uber/kraken/core"
	"github.com/uber/kraken/lib/backend/namepath"
	"github.com/uber/kraken/utils/closers"
	"github.com/uber/kraken/utils/testutil"

	"github.com/stretchr/testify/require"
)

func TestClientDownloadRange(t *testing.T) {
	content := []byte("0123456789")

	tests := []struct {
		desc     string
		offset   int64
		length   int64
		expected []byte
	}{
		{"first piece", 0, 4, []byte("0123")},
		{"interior piece", 4, 3, []byte("456")},
		{"short last piece", 8, 2, []byte("89")},
		{"length past eof clamps", 8, 5, []byte("89")},
		{"full length", 0, 10, content},
	}
	for _, test := range tests {
		t.Run(test.desc, func(t *testing.T) {
			require := require.New(t)

			s := NewServer()
			defer s.Cleanup()

			addr, stop := testutil.StartServer(s.Handler())
			defer stop()

			c, err := NewClient(Config{Addr: addr, NamePath: namepath.Identity}, tally.NoopScope)
			require.NoError(err)
			defer closers.Close(c)

			ns := core.NamespaceFixture()
			name := core.DigestFixture().Hex()
			require.NoError(c.Upload(ns, name, bytes.NewReader(content)))

			var b bytes.Buffer
			require.NoError(c.DownloadRange(ns, name, &b, test.offset, test.length))
			require.Equal(test.expected, b.Bytes())
		})
	}
}
