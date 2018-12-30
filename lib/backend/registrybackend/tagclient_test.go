package registrybackend

import (
	"bytes"
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/pressly/chi"
	"github.com/stretchr/testify/require"
	"github.com/uber/kraken/core"
	"github.com/uber/kraken/lib/backend/backenderrors"
	"github.com/uber/kraken/utils/memsize"
	"github.com/uber/kraken/utils/randutil"
	"github.com/uber/kraken/utils/testutil"
)

func TestTagDownloadSuccess(t *testing.T) {
	require := require.New(t)

	blob := randutil.Blob(32 * memsize.KB)
	digest := core.DigestFixture()
	tag := core.TagFixture()
	namespace := strings.Split(tag, ":")[0]

	r := chi.NewRouter()
	r.Head(fmt.Sprintf("/v2/%s/manifests/:tag", namespace), func(w http.ResponseWriter, req *http.Request) {
		w.Header().Set("Content-Length", fmt.Sprintf("%d", len(blob)))
		w.Header().Set("Docker-Content-Digest", digest.String())
	})
	addr, stop := testutil.StartServer(r)
	defer stop()

	config := Config{Address: addr}
	client, err := NewTagClient(config)
	require.NoError(err)

	info, err := client.Stat(tag, tag)
	require.NoError(err)
	require.Equal(int64(len(blob)), info.Size)

	var b bytes.Buffer
	require.NoError(client.Download(tag, tag, &b))
	require.Equal(digest.String(), string(b.Bytes()))
}

func TestTagDownloadFileNotFound(t *testing.T) {
	require := require.New(t)

	tag := core.TagFixture()
	namespace := strings.Split(tag, ":")[0]

	r := chi.NewRouter()
	r.Head(fmt.Sprintf("/v2/%s/manifests/:tag", namespace), func(w http.ResponseWriter, req *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte("file not found"))
	})
	addr, stop := testutil.StartServer(r)
	defer stop()

	config := Config{Address: addr}
	client, err := NewTagClient(config)
	require.NoError(err)

	_, err = client.Stat(tag, tag)
	require.Equal(backenderrors.ErrBlobNotFound, err)

	var b bytes.Buffer
	require.Equal(backenderrors.ErrBlobNotFound, client.Download(tag, tag, &b))
}
