package transfer

import (
	"fmt"
	"io"
	"math"
	"strings"
	"sync"

	"code.uber.internal/go-common.git/x/log"
	"code.uber.internal/infra/kraken/lib/dockerregistry/image"
	"code.uber.internal/infra/kraken/lib/dockerregistry/transfer/manifestclient"
	"code.uber.internal/infra/kraken/origin/blobclient"
	"code.uber.internal/infra/kraken/utils/errutil"
	"code.uber.internal/infra/kraken/utils/stringset"
)

var _ ImageTransferer = (*OriginClusterTransferer)(nil)

// OriginClusterTransferer transfers blobs in a distributed system
type OriginClusterTransferer struct {
	originResolver blobclient.ClusterResolver
	manifestClient manifestclient.Client

	// concurrency defines the number of concurrent downloads and uploads allowed
	// numWorkers ensures this concurrency
	concurrency int
	numWorkers  chan struct{}
}

// NewOriginClusterTransferer creates a new sharded blob transferer
func NewOriginClusterTransferer(
	concurrency int,
	originResolver blobclient.ClusterResolver,
	manifestClient manifestclient.Client) *OriginClusterTransferer {

	return &OriginClusterTransferer{
		originResolver: originResolver,
		manifestClient: manifestClient,
		concurrency:    concurrency,
		numWorkers:     make(chan struct{}, concurrency),
	}
}

// Download tries to download blob from several locations and returns error if failed to download from all locations
func (t *OriginClusterTransferer) Download(digest string) (io.ReadCloser, error) {
	t.reserveWorker()
	defer t.releaseWorker()

	d := image.NewSHA256DigestFromHex(digest)

	clients, err := t.originResolver.Resolve(d)
	if err != nil {
		return nil, fmt.Errorf("unable to resolve origin cluster: %s", err)
	}

	// Download will succeed if at least one location has the data
	var errs []error
	for _, client := range clients {
		blob, err := client.GetBlob(d)
		if err != nil {
			errs = append(errs, fmt.Errorf("failed to pull blob from %s: %s", client.Addr(), err))
			continue
		}
		return blob, nil
	}

	return nil, fmt.Errorf("failed to pull blob from all locations: %s", errutil.Join(errs))
}

func toLocations(clients []blobclient.Client) stringset.Set {
	locs := make(stringset.Set)
	for _, c := range clients {
		locs.Add(c.Addr())
	}
	return locs
}

func (t *OriginClusterTransferer) ensureNoLocationChanges(initLocs stringset.Set, d image.Digest) error {
	newClients, err := t.originResolver.Resolve(d)
	if err != nil {
		return fmt.Errorf("unable to resolve origin cluster: %s", err)
	}
	newLocs := toLocations(newClients)
	missingLocs := newLocs.Sub(initLocs)
	if len(missingLocs) > 0 {
		return fmt.Errorf("missing blobs at locations: %s", strings.Join(missingLocs.ToSlice(), ","))
	}
	return nil
}

// Splits src into n thread-safe readers which only buffer data as needed.
func split(src io.Reader, n int) []io.ReadCloser {
	var rs []io.ReadCloser
	var ws []io.WriteCloser
	for i := 0; i < n; i++ {
		r, w := io.Pipe()
		rs = append(rs, r)
		ws = append(ws, w)
	}
	// Exits after every returned ReadCloser is closed.
	go func() {
		var writers []io.Writer
		for _, w := range ws {
			defer w.Close()
			// Cannot cast []io.WriteCloser to []io.Writer (sigh).
			writers = append(writers, w)
		}
		io.Copy(io.MultiWriter(writers...), src)
	}()
	return rs
}

// Upload tries to upload blobs to multiple locations and returns error if
// a majority of locations failed to receive the blob
func (t *OriginClusterTransferer) Upload(digest string, blob io.Reader, size int64) error {
	t.reserveWorker()
	defer t.releaseWorker()

	d := image.NewSHA256DigestFromHex(digest)

	clients, err := t.originResolver.Resolve(d)
	if err != nil {
		return fmt.Errorf("unable to resolve origin cluster: %s", err)
	}

	// Blob locations could change if there is a change in origin config, so we should
	// compare locations before and after push, and put the missing origin locations to a retry queue
	defer func() {
		// TODO(evelynl): Create a retry queue for new locations.
		if err := t.ensureNoLocationChanges(toLocations(clients), d); err != nil {
			log.Errorf("Upload error: %s", err)
		}
	}()

	blobReaders := split(blob, len(clients))

	var mu sync.Mutex
	var errs []error

	var wg sync.WaitGroup
	for i, client := range clients {
		wg.Add(1)
		go func(client blobclient.Client, blob io.ReadCloser) {
			defer wg.Done()
			defer blob.Close()
			if err := client.PushBlob(d, blob, size); err != nil && err != blobclient.ErrBlobExist {
				mu.Lock()
				err = fmt.Errorf("failed to push digest %q to location %s: %s", d, client.Addr(), err)
				errs = append(errs, err)
				mu.Unlock()
			}
		}(client, blobReaders[i])
	}
	wg.Wait()

	if errs == nil {
		return nil
	}

	// We return nil and log error when the push to majority of locations succeeded
	if len(errs) < int(math.Ceil(float64(len(clients))/2)) {
		log.Errorf("failed to push blob to some locations: %s", errutil.Join(errs))
		return nil
	}

	return fmt.Errorf("failed to push blob to majority of locations: %s", errutil.Join(errs))
}

// GetManifest gets and saves manifest given addr, repo and tag
func (t *OriginClusterTransferer) GetManifest(repo, tag string) (io.ReadCloser, error) {
	t.reserveWorker()
	defer t.releaseWorker()

	m, err := t.manifestClient.GetManifest(repo, tag)
	if err != nil {
		return nil, err
	}
	return m, nil
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
