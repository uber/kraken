package s3backend

import (
	"bytes"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"testing"

	"code.uber.internal/infra/kraken/core"
	"code.uber.internal/infra/kraken/utils/memsize"
	"code.uber.internal/infra/kraken/utils/randutil"

	"github.com/stretchr/testify/require"
)

func configFixture(region string, bucket string) Config {
	return Config{Region: region, Bucket: bucket}.applyDefaults()
}

func authConfigFixture() AuthConfig {
	return AuthConfig{AccessKeyID: "accesskey", AccessSecretKey: "secret"}
}

func TestS3UploadSuccess(t *testing.T) {
	require := require.New(t)

	// generate 32KB of random data
	b := randutil.Blob(32 * memsize.KB)

	config := configFixture("us-west-1", "test_bucket")

	s3client, err := newClient(config, authConfigFixture(), "ns")
	require.NoError(err)
	req, err := http.NewRequest("POST", "", nil)
	require.NoError(err)

	s3client.s3Session = NewS3Mock(b, req)

	f, err := ioutil.TempFile("", "s3test")
	require.NoError(err)
	defer os.Remove(f.Name())

	err = s3client.upload(f.Name(), f)
	require.NoError(err)
}

func TestS3DownloadSuccess(t *testing.T) {
	require := require.New(t)

	// generate 32KB of random data
	b := randutil.Blob(32 * memsize.KB)

	config := configFixture("us-west-1", "test_bucket")

	s3client, err := newClient(config, authConfigFixture(), "ns")
	require.NoError(err)
	req, err := http.NewRequest("POST", "", nil)
	require.NoError(err)

	s3client.s3Session = NewS3Mock(b, req)

	f, err := ioutil.TempFile("", "s3test")
	require.NoError(err)
	defer os.Remove(f.Name())

	err = s3client.download(f.Name(), f)
	require.NoError(err)
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
		get    func(t *testing.T, config Config, name string, blob []byte) backendClient
		path   string
		params func() (name string, blob []byte)
	}{
		{
			"docker blob client",
			func(t *testing.T, config Config, name string, blob []byte) backendClient {
				s3client, err := newClient(config, authConfigFixture(), "ns")
				require.NoError(t, err)

				req, err := http.NewRequest("POST", "", nil)
				require.NoError(t, err)
				s3client.s3Session = NewS3Mock(blob, req)

				dbc := &DockerBlobClient{client: s3client}

				return dbc
			},
			"docker/registry/v2/blobs/sha256/:shard/:blob/data",
			func() (string, []byte) {
				blob := core.NewBlobFixture()
				return blob.Digest.Hex(), blob.Content
			},
		}, {
			"docker tag client",
			func(t *testing.T, config Config, name string, blob []byte) backendClient {
				s3client, err := newClient(config, authConfigFixture(), "ns")
				require.NoError(t, err)

				req, err := http.NewRequest("POST", "", nil)
				require.NoError(t, err)
				s3client.s3Session = NewS3Mock(blob, req)

				dtc := &DockerTagClient{client: s3client}
				return dtc
			},
			"docker/registry/v2/repositories/:repo/_manifests/tags/:tag/current/link",
			func() (string, []byte) {
				return "testrepo:testtag", randutil.Text(64)
			},
		},
	}

	for _, client := range clients {
		t.Run(client.desc, func(t *testing.T) {
			t.Run("download", func(t *testing.T) {
				name, blob := client.params()

				c := client.get(t, configFixture("us-west-1", "test-bucket"), name, blob)

				var b bytes.Buffer
				require.NoError(t, c.Download(name, &b))
				require.Equal(t, blob, b.Bytes())
			})

			t.Run("upload", func(t *testing.T) {
				name, blob := client.params()
				c := client.get(t, configFixture("us-west-1", "test-bucket"), name, blob)

				require.NoError(t, c.Upload(name, bytes.NewReader(blob)))
			})
		})
	}
}
