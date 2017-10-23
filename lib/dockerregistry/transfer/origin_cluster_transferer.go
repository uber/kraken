package transfer

import (
	"fmt"
	"io"
	"strings"
	"sync"

	"code.uber.internal/go-common.git/x/log"
	"code.uber.internal/infra/kraken/lib/dockerregistry/image"
	"code.uber.internal/infra/kraken/origin/blobclient"
	"code.uber.internal/infra/kraken/utils/errutil"
)

var _ ImageTransferer = (*OriginClusterTransferer)(nil)

// OriginClusterTransferer transfers blobs in a distributed system
type OriginClusterTransferer struct {
	originAddr string

	blobClientProvider blobclient.Provider
	manifestClient     ManifestClient

	// concurrency defines the number of concurrent downloads and uploads allowed
	// numWorkers ensures this concurrency
	concurrency int
	numWorkers  chan struct{}
}

// NewOriginClusterTransferer creates a new sharded blob transferer
func NewOriginClusterTransferer(
	concurrency int,
	trackerAddr string,
	originAddr string,
	blobClientProvider blobclient.Provider) *OriginClusterTransferer {
	manifestClient := &HTTPManifestClient{trackerAddr}
	return &OriginClusterTransferer{
		originAddr:         originAddr,
		blobClientProvider: blobClientProvider,
		manifestClient:     manifestClient,
		concurrency:        concurrency,
		numWorkers:         make(chan struct{}, concurrency),
	}
}

// Download tries to download blob from several locations and returns error if failed to download from all locations
func (t *OriginClusterTransferer) Download(digest string) (io.ReadCloser, error) {
	t.reserveWorker()
	defer t.releaseWorker()

	imageDigest := image.NewSHA256DigestFromHex(digest)
	client := t.blobClientProvider.Provide(t.originAddr)
	locs, err := client.Locations(imageDigest)
	if err != nil {
		return nil, fmt.Errorf("unable to get pull blob locations: %s", err)
	}

	// Download will succeed if at least one location has the data
	var errs []string
	for _, loc := range locs {
		client := t.blobClientProvider.Provide(loc)
		readCloser, err := client.GetBlob(imageDigest)
		if err != nil {
			errs = append(errs, fmt.Sprintf("failed to pull blob from %s: %s", loc, err))
			continue
		}
		return readCloser, nil
	}

	return nil, fmt.Errorf("failed to pull blob from all locations: %s", strings.Join(errs, ", "))
}

func (t *OriginClusterTransferer) pushBlob(d image.Digest, reader io.Reader, size int64, loc string) error {
	client := t.blobClientProvider.Provide(loc)

	if err := client.PushBlob(d, reader, size); err != nil && err != blobclient.ErrBlobExist {
		return fmt.Errorf("failed to push blob: %s", err)
	}

	return nil
}

type pushBlobError struct {
	d   image.Digest
	loc string
	err error
}

func (e pushBlobError) Error() string {
	return fmt.Sprintf("failed to push digest %q to location %s: %s", e.d, e.loc, e.err)
}

type uploadQuorumError struct {
	errs errutil.MultiError
}

func (e uploadQuorumError) Error() string {
	return fmt.Sprintf("failed to push blob to quorum of locations: %s", e.errs)
}

// Upload tries to upload blobs to multiple locations and returns error if
// a majority of locations failed to receive the blob
func (t *OriginClusterTransferer) Upload(digest string, reader io.Reader, size int64) error {
	t.reserveWorker()
	defer t.releaseWorker()

	imageDigest := image.NewSHA256DigestFromHex(digest)
	client := t.blobClientProvider.Provide(t.originAddr)
	locs, err := client.Locations(imageDigest)
	if err != nil {
		return fmt.Errorf("unable to get upload blob locations: %s", err)
	}

	// Blob locations could change if there is a change in origin config, so we should
	// compare locations before and after push, and put the missing origin locations to a retry queue
	defer func() {
		newlocs, err := client.Locations(imageDigest)
		if err != nil {
			log.Debugf("unable to get upload blob locations: %s", err)
		}

		m := make(map[string]struct{})
		for _, loc := range locs {
			m[loc] = struct{}{}
		}

		var missinglocs []string
		for _, newloc := range newlocs {
			_, ok := m[newloc]
			if !ok {
				missinglocs = append(missinglocs, newloc)
			}
		}

		// TODO (@evelynl): create a retry queue for new locations
		if missinglocs != nil {
			log.Errorf("missing blobs at locations: %s", strings.Join(missinglocs, ", "))
		}
	}()

	m := make(map[string]*io.PipeReader)
	var pipeWriters []*io.PipeWriter
	for _, loc := range locs {
		r, w := io.Pipe()
		m[loc] = r
		pipeWriters = append(pipeWriters, w)
	}

	go func() {
		var ioWriters []io.Writer
		for _, w := range pipeWriters {
			defer w.Close()
			ioWriters = append(ioWriters, w)
		}

		mw := io.MultiWriter(ioWriters...)
		io.Copy(mw, reader)
	}()

	var mu sync.Mutex
	var errs errutil.MultiError

	wg := sync.WaitGroup{}
	for loc, pipeR := range m {
		wg.Add(1)
		go func(loc string, reader io.Reader) {
			defer wg.Done()
			if err := t.pushBlob(imageDigest, reader, size, loc); err != nil {
				mu.Lock()
				errs = append(errs, pushBlobError{imageDigest, loc, err})
				mu.Unlock()
			}
		}(loc, pipeR)
	}
	wg.Wait()

	if errs == nil {
		return nil
	}

	// We return nil and log error when the push to majority of locations succeeded
	if len(errs) < len(locs)/2 {
		log.Errorf("failed to push blob to some locations: %s", errs)
		return nil
	}

	return uploadQuorumError{errs}
}

// GetManifest gets and saves manifest given addr, repo and tag
func (t *OriginClusterTransferer) GetManifest(repo, tag string) (io.ReadCloser, error) {
	t.reserveWorker()
	defer t.releaseWorker()

	readCloser, err := t.manifestClient.GetManifest(repo, tag)
	if err != nil {
		return nil, err
	}
	return readCloser, nil
}

// PostManifest posts manifest to addr given repo and tag
func (t *OriginClusterTransferer) PostManifest(repo, tag, manifest string, reader io.Reader) error {
	t.reserveWorker()
	defer t.releaseWorker()

	err := t.manifestClient.PostManifest(repo, tag, manifest, reader)
	if err != nil {
		return fmt.Errorf("failed to post manifest %s:%s: %s", repo, tag, err)
	}

	return nil
}

func (t *OriginClusterTransferer) reserveWorker() {
	t.numWorkers <- struct{}{}
}

func (t *OriginClusterTransferer) releaseWorker() {
	<-t.numWorkers
}
