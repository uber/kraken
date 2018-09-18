package hdfsbackend

import (
	"fmt"
	"io"
	"path"
	"regexp"
	"sync"

	"code.uber.internal/infra/kraken/core"
	"code.uber.internal/infra/kraken/lib/backend/hdfsbackend/webhdfs"
	"code.uber.internal/infra/kraken/lib/backend/namepath"
	"code.uber.internal/infra/kraken/utils/httputil"
	"code.uber.internal/infra/kraken/utils/log"

	"github.com/satori/go.uuid"
)

// Client is a backend.Client for HDFS.
type Client struct {
	config  Config
	pather  namepath.Pather
	webhdfs webhdfs.Client
}

// NewClient returns a new Client.
func NewClient(config Config) (*Client, error) {
	webhdfs, err := webhdfs.NewClient(config.WebHDFS, config.NameNodes, config.UserName)
	if err != nil {
		return nil, err
	}
	return NewClientWithWebHDFS(config, webhdfs)
}

// NewClientWithWebHDFS returns a new Client with custom webhdfs. Useful for
// testing.
func NewClientWithWebHDFS(config Config, webhdfs webhdfs.Client) (*Client, error) {
	config.applyDefaults()
	pather, err := namepath.New(config.RootDirectory, config.NamePath)
	if err != nil {
		return nil, fmt.Errorf("namepath: %s", err)
	}
	return &Client{config, pather, webhdfs}, nil
}

// Stat returns blob info for name.
func (c *Client) Stat(name string) (*core.BlobInfo, error) {
	path, err := c.pather.BlobPath(name)
	if err != nil {
		return nil, fmt.Errorf("blob path: %s", err)
	}
	fs, err := c.webhdfs.GetFileStatus(path)
	if err != nil {
		return nil, err
	}
	return core.NewBlobInfo(fs.Length), nil
}

// Download downloads name into dst.
func (c *Client) Download(name string, dst io.Writer) error {
	path, err := c.pather.BlobPath(name)
	if err != nil {
		return fmt.Errorf("blob path: %s", err)
	}
	return c.webhdfs.Open(path, dst)
}

// Upload uploads src to name.
func (c *Client) Upload(name string, src io.Reader) error {
	uploadPath := path.Join(c.config.RootDirectory, c.config.UploadDirectory, uuid.NewV4().String())
	blobPath, err := c.pather.BlobPath(name)
	if err != nil {
		return fmt.Errorf("blob path: %s", err)
	}
	if err := c.webhdfs.Create(uploadPath, src); err != nil {
		return err
	}
	return c.webhdfs.Rename(uploadPath, blobPath)
}

var (
	_ignoreRegex = regexp.MustCompile(
		"^.+/repositories/.+/(_layers|_uploads|_manifests/(revisions|tags/.+/index)).*")
	_stopRegex = regexp.MustCompile("^.+/repositories/.+/_manifests$")
)

// List lists names which start with prefix.
func (c *Client) List(prefix string) ([]string, error) {
	root := path.Join(c.pather.BasePath(), prefix)

	var wg sync.WaitGroup
	listJobs := make(chan string, c.config.ListConcurrency)
	errc := make(chan error, c.config.ListConcurrency)

	wg.Add(1)
	listJobs <- root

	go func() {
		wg.Wait()
		close(listJobs)
	}()

	var mu sync.Mutex
	var files []string

L:
	for {
		select {
		case err := <-errc:
			// Stop early on error.
			return nil, err
		case dir, ok := <-listJobs:
			if !ok {
				break L
			}
			go func() {
				defer wg.Done()

				contents, err := c.webhdfs.ListFileStatus(dir)
				if err != nil {
					if !httputil.IsNotFound(err) {
						errc <- err
					}
					return
				}

				for _, fs := range contents {
					p := path.Join(dir, fs.PathSuffix)

					// TODO(codyg): This is an ugly hack to avoid walking through non-tags
					// during Docker catalog. Ideally, only tags are located in the repositories
					// directory, however in WBU2 HDFS, there are blobs here as well. At some
					// point, we must migrate the data into a structure which cleanly divides
					// blobs and tags (like we do in S3).
					if _ignoreRegex.MatchString(p) {
						continue
					}

					// TODO(codyg): Another ugly hack to speed up catalog performance by stopping
					// early when we hit tags...
					if _stopRegex.MatchString(p) {
						p = path.Join(p, "tags/dummy/current/link")
						fs.Type = "FILE"
					}

					if fs.Type == "DIRECTORY" {
						wg.Add(1)
						listJobs <- p
						continue
					}

					name, err := c.pather.NameFromBlobPath(p)
					if err != nil {
						log.With("path", p).Errorf("Error converting blob path into name: %s", err)
						continue
					}
					mu.Lock()
					files = append(files, name)
					mu.Unlock()
				}
			}()
		}
	}

	return files, nil
}
