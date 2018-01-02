package s3

import (
	"io/ioutil"
	"net/http"
	"os"
	"testing"

	"code.uber.internal/infra/kraken/utils/memsize"
	"code.uber.internal/infra/kraken/utils/randutil"

	"github.com/stretchr/testify/require"
)

func TestS3UploadSuccess(t *testing.T) {
	require := require.New(t)

	// generate 32KB of random data
	b := randutil.Blob(32 * memsize.KB)

	config := Config{Region: "us-west-1", Bucket: "test_bucket"}
	s3client, err := NewClient(config)
	require.NoError(err)
	req, err := http.NewRequest("POST", "", nil)
	require.NoError(err)

	s3client.s3Session = NewS3Mock(b, req)

	f, err := ioutil.TempFile("", "s3test")
	require.NoError(err)
	defer os.Remove(f.Name())

	err = s3client.Upload(f.Name(), f)
	require.NoError(err)
}

func TestS3DownloadSuccess(t *testing.T) {
	require := require.New(t)

	// generate 32KB of random data
	b := randutil.Blob(32 * memsize.KB)

	config := Config{Region: "us-west-1", Bucket: "test_bucket"}
	s3client, err := NewClient(config)
	require.NoError(err)
	req, err := http.NewRequest("POST", "", nil)
	require.NoError(err)

	s3client.s3Session = NewS3Mock(b, req)

	f, err := ioutil.TempFile("", "s3test")
	require.NoError(err)
	defer os.Remove(f.Name())

	err = s3client.Download(f.Name(), f)
	require.NoError(err)
}
