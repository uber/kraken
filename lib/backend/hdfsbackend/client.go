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
package hdfsbackend

import (
	"errors"
	"fmt"
	"io"
	"path"
	"regexp"
	"sync"

	"github.com/uber/kraken/core"
	"github.com/uber/kraken/lib/backend"
	"github.com/uber/kraken/lib/backend/hdfsbackend/webhdfs"
	"github.com/uber/kraken/lib/backend/namepath"
	"github.com/uber/kraken/utils/httputil"
	"github.com/uber/kraken/utils/log"

	"github.com/satori/go.uuid"
	"gopkg.in/yaml.v2"
)

const _hdfs = "hdfs"

func init() {
	backend.Register(_hdfs, &factory{})
}

type factory struct{}

func (f *factory) Create(
	confRaw interface{}, authConfRaw interface{}) (backend.Client, error) {

	confBytes, err := yaml.Marshal(confRaw)
	if err != nil {
		return nil, errors.New("marshal hdfs config")
	}
	var config Config
	if err := yaml.Unmarshal(confBytes, &config); err != nil {
		return nil, errors.New("unmarshal hdfs config")
	}
	return NewClient(config)
}

// Client is a backend.Client for HDFS.
type Client struct {
	config  Config
	pather  namepath.Pather
	webhdfs webhdfs.Client
}

// Option allows setting optional Client parameters.
type Option func(*Client)

// WithWebHDFS configures a Client with a custom webhdfs implementation.
func WithWebHDFS(w webhdfs.Client) Option {
	return func(c *Client) { c.webhdfs = w }
}

// NewClient creates a new Client for HDFS.
func NewClient(config Config, opts ...Option) (*Client, error) {
	config.applyDefaults()
	if !path.IsAbs(config.RootDirectory) {
		return nil, errors.New("invalid config: root_directory must be absolute path")
	}
	pather, err := namepath.New(config.RootDirectory, config.NamePath)
	if err != nil {
		return nil, fmt.Errorf("namepath: %s", err)
	}
	webhdfs, err := webhdfs.NewClient(config.WebHDFS, config.NameNodes, config.UserName)
	if err != nil {
		return nil, err
	}
	client := &Client{config, pather, webhdfs}
	for _, opt := range opts {
		opt(client)
	}
	return client, nil
}

// Stat returns blob info for name.
func (c *Client) Stat(namespace, name string) (*core.BlobInfo, error) {
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
func (c *Client) Download(namespace, name string, dst io.Writer) error {
	path, err := c.pather.BlobPath(name)
	if err != nil {
		return fmt.Errorf("blob path: %s", err)
	}
	return c.webhdfs.Open(path, dst)
}

// Upload uploads src to name.
func (c *Client) Upload(namespace, name string, src io.Reader) error {
	uploadPath := path.Join(c.config.RootDirectory, c.config.UploadDirectory, uuid.NewV4().String())
	blobPath, err := c.pather.BlobPath(name)
	if err != nil {
		return fmt.Errorf("blob path: %s", err)
	}
	if err := c.webhdfs.Create(uploadPath, src); err != nil {
		return err
	}
	if err := c.webhdfs.Mkdirs(path.Dir(blobPath)); err != nil {
		return err
	}
	return c.webhdfs.Rename(uploadPath, blobPath)
}

var (
	_ignoreRegex = regexp.MustCompile(
		"^.+/repositories/.+/(_layers|_uploads|_manifests/(revisions|tags/.+/index)).*")
	_stopRegex = regexp.MustCompile("^.+/repositories/.+/_manifests$")
)

type listResult struct {
	dir  string
	list []webhdfs.FileStatus
	err  error
}

func (c *Client) lister(done <-chan struct{}, listJobs <-chan string, results chan<- listResult) {
	for {
		select {
		case <-done:
			return
		case dir := <-listJobs:
			l, err := c.webhdfs.ListFileStatus(dir)
			select {
			case <-done:
				return
			case results <- listResult{dir, l, err}:
			}
		}
	}
}

func (c *Client) sendAll(done <-chan struct{}, dirs []string, listJobs chan<- string) {
	for _, d := range dirs {
		select {
		case <-done:
			return
		case listJobs <- d:
		}
	}
}

// List lists names which start with prefix.
func (c *Client) List(prefix string, opts ...backend.ListOption) (*backend.ListResult, error) {
	options := backend.DefaultListOptions()
	for _, opt := range opts {
		opt(options)
	}

	if options.Paginated {
		return nil, errors.New("pagination not supported")
	}

	root := path.Join(c.pather.BasePath(), prefix)

	listJobs := make(chan string)
	results := make(chan listResult)
	done := make(chan struct{})

	var wg sync.WaitGroup

	for i := 0; i < c.config.ListConcurrency; i++ {
		wg.Add(1)
		go func() {
			c.lister(done, listJobs, results)
			wg.Done()
		}()
	}

	defer func() {
		close(done)
		if c.config.testing {
			// Waiting might be delayed if an early error is encountered but
			// other goroutines are waiting on a long http timeout. Thus, we
			// only wait for each spawned goroutine to exit during testing to
			// assert that no goroutines leak.
			wg.Wait()
		}
	}()

	var files []string

	// Pending tracks the number of directories which are pending exploration.
	// Invariant: there will be a result received for every increment made to
	// pending.
	pending := 1
	listJobs <- root

	for pending > 0 {
		res := <-results
		pending--
		if res.err != nil {
			if httputil.IsNotFound(res.err) {
				continue
			}
			return nil, res.err
		}
		var dirs []string
		for _, fs := range res.list {
			p := path.Join(res.dir, fs.PathSuffix)

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
				// Flat directory structures are common, so accumulate directories and send
				// them to the listers in a single goroutine (as opposed to a goroutine per
				// directory).
				dirs = append(dirs, p)
			} else {
				name, err := c.pather.NameFromBlobPath(p)
				if err != nil {
					log.With("path", p).Errorf("Error converting blob path into name: %s", err)
					continue
				}
				files = append(files, name)
			}
		}
		if len(dirs) > 0 {
			// We cannot send list jobs and receive results in the same thread, else
			// deadlock will occur.
			wg.Add(1)
			go func() {
				c.sendAll(done, dirs, listJobs)
				wg.Done()
			}()
			pending += len(dirs)
		}
	}

	return &backend.ListResult{
		Names: files,
	},  nil
}
