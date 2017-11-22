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
	"code.uber.internal/infra/kraken/lib/store"
	"code.uber.internal/infra/kraken/origin/blobclient"
	"code.uber.internal/infra/kraken/torlib"
	"code.uber.internal/infra/kraken/tracker/metainfoclient"
	"code.uber.internal/infra/kraken/utils/errutil"
	"code.uber.internal/infra/kraken/utils/stringset"
)

var _ ImageTransferer = (*OriginClusterTransferer)(nil)

// OriginClusterTransferer transfers blobs in a distributed system
type OriginClusterTransferer struct {
	config OriginClusterTransfererConfig

	originResolver blobclient.ClusterResolver
	manifestClient manifestclient.Client
	metaInfoClient metainfoclient.Client

	fs store.FileStore

	// numWorkers ensures this concurrency
	numWorkers chan struct{}
}

// NewOriginClusterTransferer creates a new sharded blob transferer
func NewOriginClusterTransferer(
	config OriginClusterTransfererConfig,
	originResolver blobclient.ClusterResolver,
	manifestClient manifestclient.Client,
	metaInfoClient metainfoclient.Client,
	fs store.FileStore) *OriginClusterTransferer {

	config = config.applyDefaults()

	return &OriginClusterTransferer{
		config:         config,
		originResolver: originResolver,
		manifestClient: manifestClient,
		metaInfoClient: metaInfoClient,
		fs:             fs,
		numWorkers:     make(chan struct{}, config.Concurrency),
	}
}

// Download tries to download blob from several locations and returns error if failed to download from all locations
func (t *OriginClusterTransferer) Download(name string) (store.FileReader, error) {
	t.reserveWorker()
	defer t.releaseWorker()

	blob, err := t.fs.GetCacheFileReader(name)
	if err == nil {
		return blob, nil
	}

	d := image.NewSHA256DigestFromHex(name)

	clients, err := t.originResolver.Resolve(d)
	if err != nil {
		return nil, fmt.Errorf("unable to resolve origin cluster: %s", err)
	}

	// Download will succeed if at least one location has the data
	var errs []error
	for _, client := range clients {
		r, err := client.GetBlob(d)
		if err != nil {
			errs = append(errs, fmt.Errorf("failed to pull blob from %s: %s", client.Addr(), err))
			continue
		}
		if err := t.fs.CreateCacheFile(name, r); err != nil {
			errs = append(errs, fmt.Errorf("create cache file: %s", err))
			continue
		}
		blob, err := t.fs.GetCacheFileReader(name)
		if err != nil {
			return nil, fmt.Errorf("wrote blob to cache but could not get reader: %s", err)
		}
		return blob, nil
	}

	return nil, fmt.Errorf("failed to pull blob from all locations: %s", errutil.Join(errs))
}

// uploadMetaInfo creates and uploads torrent metainfo for blob. No-ops if metainfo
// already exists for blob.
func (t *OriginClusterTransferer) uploadMetaInfo(d image.Digest, blobCloner store.FileReaderCloner) error {
	blob, err := blobCloner.Clone()
	if err != nil {
		return fmt.Errorf("clone blob io: %s", err)
	}
	defer blob.Close()

	mi, err := torlib.NewMetaInfoFromBlob(
		d.Hex(), blob, t.config.TorrentPieceLength, "docker", "kraken-proxy")
	if err != nil {
		return fmt.Errorf("create metainfo: %s", err)
	}
	if err := t.metaInfoClient.Upload(mi); err != nil && err != metainfoclient.ErrExists {
		return fmt.Errorf("post metainfo: %s", err)
	}
	return nil
}

func (t *OriginClusterTransferer) pushBlob(
	client blobclient.Client, d image.Digest, blobCloner store.FileReaderCloner, size int64) error {

	blob, err := blobCloner.Clone()
	if err != nil {
		return fmt.Errorf("clone blob io: %s", err)
	}
	defer blob.Close()

	if err := client.PushBlob(d, blob, size); err != nil && err != blobclient.ErrBlobExist {
		return fmt.Errorf("push blob: %s", err)
	}
	return nil
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

// Upload tries to upload blobs to multiple locations and returns error if
// a majority of locations failed to receive the blob
func (t *OriginClusterTransferer) Upload(name string, blobCloner store.FileReaderCloner, size int64) error {
	t.reserveWorker()
	defer t.releaseWorker()

	d := image.NewSHA256DigestFromHex(name)

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

	if err := t.uploadMetaInfo(d, blobCloner); err != nil {
		return fmt.Errorf("upload torrent: %s", err)
	}

	var mu sync.Mutex
	var errs []error

	var wg sync.WaitGroup
	for _, client := range clients {
		wg.Add(1)
		go func(client blobclient.Client) {
			defer wg.Done()
			if err := t.pushBlob(client, d, blobCloner, size); err != nil {
				mu.Lock()
				err = fmt.Errorf("failed to push digest %q to location %s: %s", d, client.Addr(), err)
				errs = append(errs, err)
				mu.Unlock()
			}
		}(client)
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
func (t *OriginClusterTransferer) PostManifest(repo, tag string, manifest io.Reader) error {
	t.reserveWorker()
	defer t.releaseWorker()

	err := t.manifestClient.PostManifest(repo, tag, manifest)
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
