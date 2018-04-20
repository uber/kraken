package s3backend

import (
	"github.com/c2h5oh/datasize"

	"code.uber.internal/infra/kraken/utils/memsize"
)

// Config defines s3 connection specific
// parameters and authetication credentials
type Config struct {
	Region string `yaml:"region"` // AWS S3 region
	Bucket string `yaml:"bucket"` // S3 bucket

	RootDirectory    string `yaml:"root_directory"`     // S3 root directory for docker images
	UploadPartSize   int64  `yaml:"upload_part_size"`   // part size s3 manager uses for upload
	DownloadPartSize int64  `yaml:"download_part_size"` // part size s3 manager uses for download

	UploadConcurrency   int `yaml:"upload_concurrency"`   // # of concurrent go-routines s3 manager uses for upload
	DownloadConcurrency int `yaml:"donwload_concurrency"` // # of concurrent go-routines s3 manager uses for download

	// BufferGuard protects download from downloading into an oversized buffer
	// when io.WriterAt is not implemented.
	BufferGuard datasize.ByteSize `yaml:"buffer_guard"`

	// NamePath identifies which namepath.Pather to use.
	NamePath string `yaml:"name_path"`
}

// AuthConfig defines s3 credential parameters
type AuthConfig struct {
	// The credentialas are supposed to be delivered via secure
	// mechanism only, in Uber's world it would langley or usecret
	AccessKeyID     string `yaml:"aws_access_key_id"`
	AccessSecretKey string `yaml:"aws_secret_access_key"`
	SessionToken    string `yaml:"aws_session_token"`
}

func (c Config) applyDefaults() Config {
	if c.UploadPartSize == 0 {
		c.UploadPartSize = int64(64 * memsize.MB)
	}
	if c.DownloadPartSize == 0 {
		c.DownloadPartSize = int64(64 * memsize.MB)
	}
	if c.UploadConcurrency == 0 {
		c.UploadConcurrency = 10
	}
	if c.DownloadConcurrency == 0 {
		c.DownloadConcurrency = 10
	}
	if c.BufferGuard == 0 {
		c.BufferGuard = 10 * datasize.MB
	}
	if c.RootDirectory == "" {
		c.RootDirectory = "/"
	}
	return c
}
