package hdfsbackend

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"testing"

	"code.uber.internal/infra/kraken/core"
	"code.uber.internal/infra/kraken/lib/backend/backenderrors"
	"code.uber.internal/infra/kraken/utils/randutil"
	"code.uber.internal/infra/kraken/utils/rwutil"
	"code.uber.internal/infra/kraken/utils/testutil"
	"github.com/pressly/chi"
	"github.com/stretchr/testify/require"
)

type testServer struct {
	getName, getData, putName, putData http.HandlerFunc
}

func (s *testServer) handler() http.Handler {
	r := chi.NewRouter()
	r.Get("/data/:blob", s.getName)
	r.Get("/datanode", s.getData)
	r.Put("/data/:blob", s.putName)
	r.Put("/datanode", s.putData)
	return r
}

func redirectToDataNode(w http.ResponseWriter, r *http.Request) {
	http.Redirect(w, r, fmt.Sprintf("http://%s/datanode", r.Host), http.StatusTemporaryRedirect)
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

func configFixture(nodes ...string) Config {
	return Config{
		NameNodes:     nodes,
		RootDirectory: "data",
		NamePath:      "identity",
	}.applyDefaults()
}

func TestClientDownloadSuccess(t *testing.T) {
	require := require.New(t)

	blob := core.NewBlobFixture()

	server := &testServer{
		getName: redirectToDataNode,
		getData: writeResponse(http.StatusOK, blob.Content),
	}
	addr, stop := testutil.StartServer(server.handler())
	defer stop()

	client, err := NewClient(configFixture(addr))
	require.NoError(err)

	var b bytes.Buffer
	require.NoError(client.Download(blob.Digest.Hex(), &b))
	require.Equal(blob.Content, b.Bytes())
}

func TestClientDownloadRetriesNextNameNode(t *testing.T) {
	require := require.New(t)

	blob := core.NewBlobFixture()

	server1 := &testServer{
		getName: redirectToDataNode,
		getData: writeResponse(http.StatusForbidden, nil),
	}
	addr1, stop := testutil.StartServer(server1.handler())
	defer stop()

	server2 := &testServer{
		getName: redirectToDataNode,
		getData: writeResponse(http.StatusOK, blob.Content),
	}
	addr2, stop := testutil.StartServer(server2.handler())
	defer stop()

	client, err := NewClient(configFixture(addr1, addr2))
	require.NoError(err)

	var b bytes.Buffer
	require.NoError(client.Download(blob.Digest.Hex(), &b))
	require.Equal(blob.Content, b.Bytes())
}

func TestClientDownloadErrBlobNotFound(t *testing.T) {
	require := require.New(t)

	server := &testServer{
		getName: writeResponse(http.StatusNotFound, []byte("file not found")),
	}
	addr, stop := testutil.StartServer(server.handler())
	defer stop()

	client, err := NewClient(configFixture(addr))
	require.NoError(err)

	f, err := ioutil.TempFile("", "hdfs3test")
	require.NoError(err)
	defer os.Remove(f.Name())

	d := core.DigestFixture()

	var b bytes.Buffer
	require.Equal(backenderrors.ErrBlobNotFound, client.Download(d.Hex(), &b))
}

func TestClientUploadSuccess(t *testing.T) {
	require := require.New(t)

	blob := core.NewBlobFixture()

	server := &testServer{
		putName: redirectToDataNode,
		putData: checkBody(t, blob.Content),
	}
	addr, stop := testutil.StartServer(server.handler())
	defer stop()

	client, err := NewClient(configFixture(addr))
	require.NoError(err)

	require.NoError(client.Upload(blob.Digest.Hex(), bytes.NewReader(blob.Content)))
}

func TestClientUploadUnknownFailure(t *testing.T) {
	require := require.New(t)

	server := &testServer{
		putName: redirectToDataNode,
		putData: writeResponse(http.StatusInternalServerError, []byte("unknown error")),
	}
	addr, stop := testutil.StartServer(server.handler())
	defer stop()

	client, err := NewClient(configFixture(addr))
	require.NoError(err)

	blob := core.NewBlobFixture()

	require.Error(client.Upload(blob.Digest.Hex(), bytes.NewReader(blob.Content)))
}

func TestClientUploadRetriesNextNameNode(t *testing.T) {
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

			blob := core.NewBlobFixture()

			addr1, stop := testutil.StartServer(test.server1.handler())
			defer stop()

			server2 := &testServer{
				putName: redirectToDataNode,
				putData: checkBody(t, blob.Content),
			}
			addr2, stop := testutil.StartServer(server2.handler())
			defer stop()

			client, err := NewClient(configFixture(addr1, addr2))
			require.NoError(err)

			require.NoError(client.Upload(blob.Digest.Hex(), bytes.NewReader(blob.Content)))

			// Ensure bytes.Buffer can replay data.
			require.NoError(client.Upload(blob.Digest.Hex(), bytes.NewBuffer(blob.Content)))

			// Ensure non-buffer non-seekers can replay data.
			require.NoError(client.Upload(blob.Digest.Hex(), rwutil.PlainReader(blob.Content)))
		})
	}
}

func TestClientUploadErrorsWhenExceedsBufferGuard(t *testing.T) {
	require := require.New(t)

	config := configFixture("dummy-addr")
	config.BufferGuard = 50

	client, err := NewClient(config)
	require.NoError(err)

	// Exceeds BufferGuard.
	data := randutil.Text(100)

	err = client.Upload("/some/path", rwutil.PlainReader(data))
	require.Error(err)
	_, ok := err.(drainSrcError).err.(exceededCapError)
	require.True(ok)
}

func TestClientStat(t *testing.T) {
	require := require.New(t)

	blob := core.NewBlobFixture()

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

	client, err := NewClient(configFixture(addr))
	require.NoError(err)

	info, err := client.Stat(blob.Digest.Hex())
	require.NoError(err)
	require.Equal(int64(32), info.Size)
}

func TestClientStatNotFound(t *testing.T) {
	require := require.New(t)

	blob := core.NewBlobFixture()

	server := &testServer{
		getName: redirectToDataNode,
		getData: writeResponse(http.StatusNotFound, nil),
	}
	addr, stop := testutil.StartServer(server.handler())
	defer stop()

	client, err := NewClient(configFixture(addr))
	require.NoError(err)

	_, err = client.Stat(blob.Digest.Hex())
	require.Equal(backenderrors.ErrBlobNotFound, err)
}

func TestClientList(t *testing.T) {
	require := require.New(t)

	// Copied from webhdfs documentation.
	resp := `
		{
		  "FileStatuses":
		  {
			"FileStatus":
			[
			  {
				"accessTime"      : 1320171722771,
				"blockSize"       : 33554432,
				"group"           : "supergroup",
				"length"          : 24930,
				"modificationTime": 1320171722771,
				"owner"           : "webuser",
				"pathSuffix"      : "a.patch",
				"permission"      : "644",
				"replication"     : 1,
				"type"            : "FILE"
			  },
			  {
				"accessTime"      : 0,
				"blockSize"       : 0,
				"group"           : "supergroup",
				"length"          : 0,
				"modificationTime": 1320895981256,
				"owner"           : "username",
				"pathSuffix"      : "bar",
				"permission"      : "711",
				"replication"     : 0,
				"type"            : "DIRECTORY"
			  }
			]
		  }
		}
	`

	server := &testServer{
		getName: redirectToDataNode,
		getData: writeResponse(http.StatusOK, []byte(resp)),
	}
	addr, stop := testutil.StartServer(server.handler())
	defer stop()

	client, err := NewClient(configFixture(addr))
	require.NoError(err)

	names, err := client.List("some dir")
	require.NoError(err)
	require.Equal([]string{"a.patch", "bar"}, names)
}

func TestClientListNotFound(t *testing.T) {
	require := require.New(t)

	server := &testServer{
		getName: redirectToDataNode,
		getData: writeResponse(http.StatusNotFound, nil),
	}
	addr, stop := testutil.StartServer(server.handler())
	defer stop()

	client, err := NewClient(configFixture(addr))
	require.NoError(err)

	_, err = client.List("some dir")
	require.Equal(backenderrors.ErrDirNotFound, err)
}
