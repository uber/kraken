package s3backend

import (
	"io/ioutil"
	"net/http"
	"os"
	"testing"

	"code.uber.internal/infra/kraken/utils/memsize"
	"code.uber.internal/infra/kraken/utils/randutil"

	"github.com/stretchr/testify/require"
)

func configFixture(region string, bucket string) Config {
	return Config{
		Username: "test-user",
		Region:   region,
		Bucket:   bucket,
		NamePath: "identity",
	}
}

func authFixture() UserAuthConfig {
	var c AuthConfig
	c.S3.AccessKeyID = "accesskey"
	c.S3.AccessSecretKey = "secret"
	return UserAuthConfig{"test-user": c}
}

func TestS3UploadSuccess(t *testing.T) {
	require := require.New(t)

	// generate 32KB of random data
	b := randutil.Blob(32 * memsize.KB)

	config := configFixture("us-west-1", "test_bucket")

	s3client, err := NewClient(config, authFixture())
	require.NoError(err)
	req, err := http.NewRequest("POST", "", nil)
	require.NoError(err)

	s3client.s3Session = NewS3Mock(b, req)

	f, err := ioutil.TempFile("", "s3test")
	require.NoError(err)
	defer os.Remove(f.Name())

	require.NoError(s3client.Upload(f.Name(), f))
}

func TestS3DownloadSuccess(t *testing.T) {
	require := require.New(t)

	// generate 32KB of random data
	b := randutil.Blob(32 * memsize.KB)

	config := configFixture("us-west-1", "test_bucket")

	s3client, err := NewClient(config, authFixture())
	require.NoError(err)
	req, err := http.NewRequest("POST", "", nil)
	require.NoError(err)

	s3client.s3Session = NewS3Mock(b, req)

	f, err := ioutil.TempFile("", "s3test")
	require.NoError(err)
	defer os.Remove(f.Name())

	require.NoError(s3client.Download(f.Name(), f))
}
