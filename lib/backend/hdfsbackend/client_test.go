package hdfsbackend

import (
	"bytes"
	"fmt"
	"io"
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
	path                               string
	getName, getData, putName, putData http.HandlerFunc
}

func (s *testServer) handler() http.Handler {
	r := chi.NewRouter()
	r.Get("/"+s.path, s.getName)
	r.Get("/datanode", s.getData)
	r.Put("/"+s.path, s.putName)
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
	config, err := Config{NameNodes: nodes}.applyDefaults()
	if err != nil {
		panic(err)
	}
	return config
}

func TestClientDownloadSuccess(t *testing.T) {
	require := require.New(t)

	blob := core.NewBlobFixture()

	server := &testServer{
		path:    "data/:blob",
		getName: redirectToDataNode,
		getData: writeResponse(http.StatusOK, blob.Content),
	}
	addr, stop := testutil.StartServer(server.handler())
	defer stop()

	client := newClient(configFixture(addr))

	var b bytes.Buffer
	require.NoError(client.download("data/"+blob.Digest.Hex(), &b))
	require.Equal(blob.Content, b.Bytes())
}

func TestClientDownloadRetriesNextNameNode(t *testing.T) {
	require := require.New(t)

	blob := core.NewBlobFixture()

	server1 := &testServer{
		path:    "data/:blob",
		getName: redirectToDataNode,
		getData: writeResponse(http.StatusForbidden, nil),
	}
	addr1, stop := testutil.StartServer(server1.handler())
	defer stop()

	server2 := &testServer{
		path:    "data/:blob",
		getName: redirectToDataNode,
		getData: writeResponse(http.StatusOK, blob.Content),
	}
	addr2, stop := testutil.StartServer(server2.handler())
	defer stop()

	client := newClient(configFixture(addr1, addr2))

	var b bytes.Buffer
	require.NoError(client.download("data/"+blob.Digest.Hex(), &b))
	require.Equal(blob.Content, b.Bytes())
}

func TestClientDownloadErrBlobNotFound(t *testing.T) {
	require := require.New(t)

	server := &testServer{
		path:    "data/:blob",
		getName: writeResponse(http.StatusNotFound, []byte("file not found")),
	}
	addr, stop := testutil.StartServer(server.handler())
	defer stop()

	client := newClient(configFixture(addr))

	f, err := ioutil.TempFile("", "hdfs3test")
	require.NoError(err)
	defer os.Remove(f.Name())

	d := core.DigestFixture()

	var b bytes.Buffer
	require.Equal(backenderrors.ErrBlobNotFound, client.download("data/"+d.Hex(), &b))
}

func TestClientUploadSuccess(t *testing.T) {
	require := require.New(t)

	blob := core.NewBlobFixture()

	server := &testServer{
		path:    "data/:blob",
		putName: redirectToDataNode,
		putData: checkBody(t, blob.Content),
	}
	addr, stop := testutil.StartServer(server.handler())
	defer stop()

	client := newClient(configFixture(addr))

	require.NoError(client.upload("data/"+blob.Digest.Hex(), bytes.NewReader(blob.Content)))
}

func TestClientUploadUnknownFailure(t *testing.T) {
	require := require.New(t)

	server := &testServer{
		path:    "data/:blob",
		putName: redirectToDataNode,
		putData: writeResponse(http.StatusInternalServerError, []byte("unknown error")),
	}
	addr, stop := testutil.StartServer(server.handler())
	defer stop()

	client := newClient(configFixture(addr))

	blob := core.NewBlobFixture()

	require.Error(client.upload("data/"+blob.Digest.Hex(), bytes.NewReader(blob.Content)))
}

func TestClientUploadRetriesNextNameNode(t *testing.T) {
	tests := []struct {
		desc    string
		server1 *testServer
	}{
		{
			"name node forbidden",
			&testServer{
				path:    "data/:blob",
				putName: writeResponse(http.StatusForbidden, nil),
			},
		}, {
			"data node forbidden",
			&testServer{
				path:    "data/:blob",
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
				path:    "data/:blob",
				putName: redirectToDataNode,
				putData: checkBody(t, blob.Content),
			}
			addr2, stop := testutil.StartServer(server2.handler())
			defer stop()

			client := newClient(configFixture(addr1, addr2))

			require.NoError(client.upload("data/"+blob.Digest.Hex(), bytes.NewReader(blob.Content)))

			// Ensure bytes.Buffer can replay data.
			require.NoError(client.upload("data/"+blob.Digest.Hex(), bytes.NewBuffer(blob.Content)))

			// Ensure non-buffer non-seekers can replay data.
			require.NoError(client.upload("data/"+blob.Digest.Hex(), rwutil.PlainReader(blob.Content)))
		})
	}
}

func TestClientUploadErrorsWhenExceedsBufferGuard(t *testing.T) {
	require := require.New(t)

	config := configFixture("dummy-addr")
	config.BufferGuard = 50

	client := newClient(config)

	// Exceeds BufferGuard.
	data := randutil.Text(100)

	err := client.upload("/some/path", rwutil.PlainReader(data))
	require.Error(err)
	_, ok := err.(drainSrcError).err.(exceededCapError)
	require.True(ok)
}

type backendClient interface {
	Upload(string, io.Reader) error
	Download(string, io.Writer) error
}

// TestAllClients is a very mechanical test suite to provide coverage for download / upload
// operations on all HDFS clients. For more detailed testing of HDFS, see client_test.go.
func TestAllClients(t *testing.T) {

	clients := []struct {
		desc   string
		get    func(t *testing.T, config Config) backendClient
		path   string
		params func() (name string, data []byte)
	}{
		{
			"docker blob client",
			func(t *testing.T, config Config) backendClient {
				c, err := NewDockerBlobClient(config)
				require.NoError(t, err)
				return c
			},
			"webhdfs/v1/infra/dockerRegistry/docker/registry/v2/blobs/sha256/:shard/:blob/data",
			func() (string, []byte) {
				blob := core.NewBlobFixture()
				return blob.Digest.Hex(), blob.Content
			},
		}, {
			"docker tag client",
			func(t *testing.T, config Config) backendClient {
				c, err := NewDockerTagClient(config)
				require.NoError(t, err)
				return c
			},
			"webhdfs/v1/infra/dockerRegistry/docker/registry/v2/repositories/:repo/_manifests/tags/:tag/current/link",
			func() (string, []byte) {
				return "testrepo:testtag", randutil.Text(64)
			},
		},
	}

	for _, client := range clients {
		t.Run(client.desc, func(t *testing.T) {
			t.Run("download", func(t *testing.T) {
				name, data := client.params()

				server := &testServer{
					path:    client.path,
					getName: redirectToDataNode,
					getData: writeResponse(http.StatusOK, data),
				}
				addr, stop := testutil.StartServer(server.handler())
				defer stop()

				c := client.get(t, configFixture(addr))

				var b bytes.Buffer
				require.NoError(t, c.Download(name, &b))
				require.Equal(t, data, b.Bytes())
			})

			t.Run("upload", func(t *testing.T) {
				name, data := client.params()

				server := &testServer{
					path:    client.path,
					putName: redirectToDataNode,
					putData: checkBody(t, data),
				}
				addr, stop := testutil.StartServer(server.handler())
				defer stop()

				c := client.get(t, configFixture(addr))

				require.NoError(t, c.Upload(name, bytes.NewReader(data)))
			})
		})
	}
}
