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
package webhdfs

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"path"
	"testing"

	"github.com/uber/kraken/lib/backend/backenderrors"
	"github.com/uber/kraken/utils/randutil"
	"github.com/uber/kraken/utils/rwutil"
	"github.com/uber/kraken/utils/testutil"

	"github.com/go-chi/chi"
	"github.com/stretchr/testify/require"
)

const _testFile = "/root/test"

type testServer struct {
	getName, getData, putName, putData http.HandlerFunc
}

func (s *testServer) handler() http.Handler {
	r := chi.NewRouter()
	r.Get("/webhdfs/v1*", s.getName)
	r.Get("/datanode/webhdfs/v1*", s.getData)
	r.Put("/webhdfs/v1*", s.putName)
	r.Put("/datanode/webhdfs/v1*", s.putData)
	return r
}

func redirectToDataNode(w http.ResponseWriter, r *http.Request) {
	datanode := fmt.Sprintf(
		"http://%s/%s?%s",
		r.Host, path.Join("datanode", r.URL.Path), r.URL.Query().Encode())
	http.Redirect(w, r, datanode, http.StatusTemporaryRedirect)
}

func writeResponse(status int, body []byte) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(status)
		w.Write(body)
	}
}

func checkBody(t *testing.T, expected []byte) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		b, err := ioutil.ReadAll(r.Body)
		require.NoError(t, err)
		require.Equal(t, string(expected), string(b))
		w.WriteHeader(http.StatusCreated)
	}
}

func newClient(nodes ...string) Client {
	c, err := NewClient(Config{}, nodes, "")
	if err != nil {
		panic(err)
	}
	return c
}

func TestNewClientError(t *testing.T) {
	require := require.New(t)

	_, err := NewClient(Config{}, nil, "")
	require.Error(err)
}

func TestClientOpen(t *testing.T) {
	require := require.New(t)

	data := randutil.Text(64)

	server := &testServer{
		getName: redirectToDataNode,
		getData: writeResponse(http.StatusOK, data),
	}
	addr, stop := testutil.StartServer(server.handler())
	defer stop()

	client := newClient(addr)

	var b bytes.Buffer
	require.NoError(client.Open(_testFile, &b))
	require.Equal(data, b.Bytes())
}

func TestClientOpenRetriesNextNameNode(t *testing.T) {
	require := require.New(t)

	data := randutil.Text(64)

	server1 := &testServer{
		getName: redirectToDataNode,
		getData: writeResponse(http.StatusForbidden, nil),
	}
	addr1, stop := testutil.StartServer(server1.handler())
	defer stop()

	server2 := &testServer{
		getName: redirectToDataNode,
		getData: writeResponse(http.StatusOK, data),
	}
	addr2, stop := testutil.StartServer(server2.handler())
	defer stop()

	client := newClient(addr1, addr2)

	var b bytes.Buffer
	require.NoError(client.Open(_testFile, &b))
	require.Equal(data, b.Bytes())
}

func TestClientOpenErrBlobNotFound(t *testing.T) {
	require := require.New(t)

	server := &testServer{
		getName: writeResponse(http.StatusNotFound, []byte("file not found")),
	}
	addr, stop := testutil.StartServer(server.handler())
	defer stop()

	client := newClient(addr)

	f, err := ioutil.TempFile("", "hdfs3test")
	require.NoError(err)
	defer os.Remove(f.Name())

	var b bytes.Buffer
	require.Equal(backenderrors.ErrBlobNotFound, client.Open(_testFile, &b))
}

func TestClientCreate(t *testing.T) {
	require := require.New(t)

	data := randutil.Text(64)

	server := &testServer{
		putName: redirectToDataNode,
		putData: checkBody(t, data),
	}
	addr, stop := testutil.StartServer(server.handler())
	defer stop()

	client := newClient(addr)

	require.NoError(client.Create(_testFile, bytes.NewReader(data)))
}

func TestClientCreateUnknownFailure(t *testing.T) {
	require := require.New(t)

	server := &testServer{
		putName: redirectToDataNode,
		putData: writeResponse(http.StatusInternalServerError, []byte("unknown error")),
	}
	addr, stop := testutil.StartServer(server.handler())
	defer stop()

	client := newClient(addr)

	data := randutil.Text(64)

	require.Error(client.Create(_testFile, bytes.NewReader(data)))
}

func TestClientCreateRetriesNextNameNode(t *testing.T) {
	tests := []struct {
		desc    string
		server1 *testServer
	}{
		{
			"name node forbidden",
			&testServer{
				putName: writeResponse(http.StatusForbidden, nil),
			},
		}, {
			"data node forbidden",
			&testServer{
				putName: redirectToDataNode,
				putData: writeResponse(http.StatusForbidden, nil),
			},
		},
	}
	for _, test := range tests {
		t.Run(test.desc, func(t *testing.T) {
			require := require.New(t)

			data := randutil.Text(64)

			addr1, stop := testutil.StartServer(test.server1.handler())
			defer stop()

			server2 := &testServer{
				putName: redirectToDataNode,
				putData: checkBody(t, data),
			}
			addr2, stop := testutil.StartServer(server2.handler())
			defer stop()

			client := newClient(addr1, addr2)

			require.NoError(client.Create(_testFile, bytes.NewReader(data)))

			// Ensure bytes.Buffer can replay data.
			require.NoError(client.Create(_testFile, bytes.NewBuffer(data)))

			// Ensure non-buffer non-seekers can replay data.
			require.NoError(client.Create(_testFile, rwutil.PlainReader(data)))
		})
	}
}

func TestClientCreateErrorsWhenExceedsBufferGuard(t *testing.T) {
	require := require.New(t)

	client, err := NewClient(Config{BufferGuard: 50}, []string{"dummy-addr"}, "")
	require.NoError(err)

	// Exceeds BufferGuard.
	data := randutil.Text(100)

	err = client.Create(_testFile, rwutil.PlainReader(data))
	require.Error(err)
	_, ok := err.(drainSrcError).err.(exceededCapError)
	require.True(ok)
}

func TestClientRename(t *testing.T) {
	require := require.New(t)

	from := "/root/from"
	to := "/root/to"

	called := false

	server := &testServer{
		putName: redirectToDataNode,
		putData: func(w http.ResponseWriter, r *http.Request) {
			called = true
			require.Equal("/datanode/webhdfs/v1"+from, r.URL.Path)
			require.Equal(to, r.URL.Query().Get("destination"))
		},
	}
	addr, stop := testutil.StartServer(server.handler())
	defer stop()

	client := newClient(addr)

	require.NoError(client.Rename(from, to))
	require.True(called)
}

func TestClientMkdirs(t *testing.T) {
	require := require.New(t)

	called := false

	server := &testServer{
		putName: redirectToDataNode,
		putData: func(w http.ResponseWriter, r *http.Request) {
			called = true
			require.Equal("/datanode/webhdfs/v1"+_testFile, r.URL.Path)
		},
	}
	addr, stop := testutil.StartServer(server.handler())
	defer stop()

	client := newClient(addr)

	require.NoError(client.Mkdirs(_testFile))
	require.True(called)
}

func TestClientGetFileStatus(t *testing.T) {
	require := require.New(t)

	var resp fileStatusResponse
	resp.FileStatus.Length = 32
	b, err := json.Marshal(resp)
	require.NoError(err)

	server := &testServer{
		getName: redirectToDataNode,
		getData: writeResponse(http.StatusOK, b),
	}
	addr, stop := testutil.StartServer(server.handler())
	defer stop()

	client := newClient(addr)

	fs, err := client.GetFileStatus(_testFile)
	require.NoError(err)
	require.Equal(resp.FileStatus, fs)
}

func TestClientGetFileStatusErrBlobNotFound(t *testing.T) {
	require := require.New(t)

	server := &testServer{
		getName: redirectToDataNode,
		getData: writeResponse(http.StatusNotFound, nil),
	}
	addr, stop := testutil.StartServer(server.handler())
	defer stop()

	client := newClient(addr)

	_, err := client.GetFileStatus(_testFile)
	require.Equal(backenderrors.ErrBlobNotFound, err)
}

func TestClientListFileStatus(t *testing.T) {
	require := require.New(t)

	data := fmt.Sprintf(`
	  {
		"FileStatuses": {
		  "FileStatus": [{
			"accessTime"      : 1320171722771,
			"blockSize"       : 33554432,
			"group"           : "supergroup",
			"length"          : 24930,
			"modificationTime": 1320171722771,
			"owner"           : "webuser",
			"pathSuffix"      : %q,
			"permission"      : "644",
			"replication"     : 1,
			"type"            : "FILE"
		  }]
	    }
	  }
	`, _testFile)

	server := &testServer{
		getName: redirectToDataNode,
		getData: writeResponse(http.StatusOK, []byte(data)),
	}
	addr, stop := testutil.StartServer(server.handler())
	defer stop()

	client := newClient(addr)

	result, err := client.ListFileStatus("/root")
	require.NoError(err)
	require.Equal([]FileStatus{{
		PathSuffix: _testFile,
		Type:       "FILE",
		Length:     24930,
	}}, result)
}
