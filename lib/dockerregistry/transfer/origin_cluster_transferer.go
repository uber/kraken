package transfer

import (
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"strings"
	"sync"

	"github.com/docker/distribution/uuid"

	"code.uber.internal/go-common.git/x/log"
	"code.uber.internal/infra/kraken/lib/dockerregistry/image"
	"code.uber.internal/infra/kraken/lib/store"
	"code.uber.internal/infra/kraken/origin/blobserver"
	"code.uber.internal/infra/kraken/utils"
)

var _ ImageTransferer = (*OriginClusterTransferer)(nil)

// OriginClusterTransferer transfers blobs in a distributed system
type OriginClusterTransferer struct {
	originAddr string

	blobClientProvider blobserver.ClientProvider
	manifestClient     ManifestClient

	// concurrency defines the number of concurrent downloads and uploads allowed
	// numWorkers ensures this concurrency
	concurrency int
	numWorkers  chan struct{}
	store       store.FileStore
}

// NewOriginClusterTransferer creates a new sharded blob transferer
func NewOriginClusterTransferer(
	concurrency int,
	store store.FileStore,
	trackerAddr string,
	originAddr string,
	blobClientProvider blobserver.ClientProvider) *OriginClusterTransferer {
	manifestClient := &HTTPManifestClient{trackerAddr}
	return &OriginClusterTransferer{
		originAddr:         originAddr,
		store:              store,
		blobClientProvider: blobClientProvider,
		manifestClient:     manifestClient,
		concurrency:        concurrency,
		numWorkers:         make(chan struct{}, concurrency),
	}
}

// Download tries to download blob from several locations and returns error if failed to download from all locations
func (sbt *OriginClusterTransferer) Download(digest string) error {
	sbt.reserveWorker()
	defer sbt.releaseWorker()

	imageDigest := image.NewSHA256DigestFromHex(digest)
	client := sbt.blobClientProvider.Provide(sbt.originAddr)
	locs, err := client.Locations(imageDigest)
	if err != nil {
		return fmt.Errorf("unable to get pull blob locations: %s", err)
	}

	// Download will succeed if at least one location has the data
	var errs []string
	for _, loc := range locs {
		client := sbt.blobClientProvider.Provide(loc)
		readCloser, err := client.GetBlob(imageDigest)
		if err != nil {
			errs = append(errs, fmt.Sprintf("failed to pull blob from %s: %s", loc, err))
			continue
		}
		defer readCloser.Close()
		err = sbt.saveBlob(readCloser, imageDigest)
		if err != nil {
			errs = append(errs, fmt.Sprintf("failed to save %s: %s", digest, err))
			// TODO (@evelynl): should it continue? If store is having an issue (for exmaple, running of the disk space),
			// this call will pull useless blobs from all origins.
			break
		} else {
			return nil
		}
	}

	return fmt.Errorf("failed to pull blob from all locations: %s", strings.Join(errs, ", "))
}

// Upload tries to upload blobs to multiple locations and returns error if majority of locations failed to receive the blob
func (sbt *OriginClusterTransferer) Upload(digest string) error {
	sbt.reserveWorker()
	defer sbt.releaseWorker()

	imageDigest := image.NewSHA256DigestFromHex(digest)
	client := sbt.blobClientProvider.Provide(sbt.originAddr)
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
		log.Errorf("missing blobs at locations: %s", strings.Join(missinglocs, ", "))
	}()

	var mu sync.Mutex
	var errs []string

	wg := sync.WaitGroup{}
	for _, loc := range locs {
		wg.Add(1)
		go func(loc string) {
			defer wg.Done()
			client := sbt.blobClientProvider.Provide(loc)
			reader, err := sbt.store.GetCacheFileReader(digest)
			if err != nil {
				mu.Lock()
				defer mu.Unlock()
				errs = append(errs, fmt.Sprintf("failed to get reader %s: %s", digest, err))
				return
			}
			defer reader.Close()
			err = client.PushBlob(imageDigest, reader)
			if err != nil {
				mu.Lock()
				defer mu.Unlock()
				errs = append(errs, fmt.Sprintf("failed to push blob to %s: %s", loc, err))
				return
			}
		}(loc)
	}
	wg.Wait()

	if len(errs) == 0 {
		return nil
	}

	// We return nil and log error when the push to majority of locations succeeded
	if len(errs) < len(locs)/2 {
		log.Errorf("failed to push blob to some locations: %s,", strings.Join(errs, ", "))
		return nil
	}

	return fmt.Errorf("failed to push blob to majority of locations: %s", strings.Join(errs, ", "))
}

// GetManifest gets and saves manifest given addr, repo and tag
func (sbt *OriginClusterTransferer) GetManifest(repo, tag string) (digest string, err error) {
	sbt.reserveWorker()
	defer sbt.releaseWorker()

	readCloser, err := sbt.manifestClient.GetManifest(repo, tag)
	if err != nil {
		return "", err
	}
	defer readCloser.Close()

	data, err := ioutil.ReadAll(readCloser)
	if err != nil {
		return "", fmt.Errorf("failed to read manifest: %s", err)
	}

	_, manifestDigest, err := utils.ParseManifestV2(data)
	if err != nil {
		return "", fmt.Errorf("failed to parse manifest for %s:%s: %s", repo, tag, err)
	}

	// Store manifest
	manifestDigestTemp := manifestDigest + "." + uuid.Generate().String()
	if err = sbt.store.CreateUploadFile(manifestDigestTemp, 0); err != nil {
		return "", fmt.Errorf("failed to create file %s: %s", manifestDigest, err)
	}

	writer, err := sbt.store.GetUploadFileReadWriter(manifestDigestTemp)
	if err != nil {
		return "", fmt.Errorf("failed to get writer %s: %s", manifestDigest, err)
	}

	_, err = writer.Write(data)
	if err != nil {
		return "", fmt.Errorf("failed to write %s: %s", manifestDigest, err)
	}
	writer.Close()

	err = sbt.store.MoveUploadFileToCache(manifestDigestTemp, manifestDigest)
	// It is ok if move fails on file exist error
	if err != nil && !os.IsExist(err) {
		return "", fmt.Errorf("failed to move %s to cache: %s", manifestDigest, err)
	}

	return manifestDigest, nil
}

// PostManifest posts manifest to addr given repo and tag
func (sbt *OriginClusterTransferer) PostManifest(repo, tag, manifest string) error {
	sbt.reserveWorker()
	defer sbt.releaseWorker()

	readCloser, err := sbt.store.GetCacheFileReader(manifest)
	if err != nil {
		return fmt.Errorf("failed to get reader for %s: %s", manifest, err)
	}
	defer readCloser.Close()

	err = sbt.manifestClient.PostManifest(repo, tag, manifest, readCloser)
	if err != nil {
		return fmt.Errorf("failed to post manifest %s:%s: %s", repo, tag, err)
	}

	return nil
}

func (sbt *OriginClusterTransferer) saveBlob(reader io.Reader, digest image.Digest) error {
	// Store layer with a tmp name and then move to cache
	// This allows multiple threads to pull the same blob
	tmp := fmt.Sprintf("%s.%s", digest.Hex(), uuid.Generate().String())
	if err := sbt.store.CreateUploadFile(tmp, 0); err != nil {
		return fmt.Errorf("failed to create upload file: %s", err)
	}
	w, err := sbt.store.GetUploadFileReadWriter(tmp)
	if err != nil {
		return fmt.Errorf("failed to get writer: %s", err)
	}
	defer w.Close()

	// Stream to file and verify content at the same time
	r := io.TeeReader(reader, w)

	verified, err := image.Verify(digest, r)
	if err != nil {
		return fmt.Errorf("failed to verify data: %s", err)
	}
	if !verified {
		// TODO: Delete tmp file on error
		return fmt.Errorf("failed to verify data: digests do not match")
	}

	if err := sbt.store.MoveUploadFileToCache(tmp, digest.Hex()); err != nil {
		if !os.IsExist(err) {
			return fmt.Errorf("failed to move upload file to cache: %s", err)
		}
		// Ignore if another thread is pulling the same blob because it is normal
	}
	return nil
}

func (sbt *OriginClusterTransferer) reserveWorker() {
	sbt.numWorkers <- struct{}{}
}

func (sbt *OriginClusterTransferer) releaseWorker() {
	<-sbt.numWorkers
}
