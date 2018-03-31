package blobrefresh

import (
	"errors"
	"fmt"

	"code.uber.internal/infra/kraken/core"
	"code.uber.internal/infra/kraken/lib/backend"
	"code.uber.internal/infra/kraken/lib/backend/backenderrors"
	"code.uber.internal/infra/kraken/lib/metainfogen"
	"code.uber.internal/infra/kraken/lib/store"
	"code.uber.internal/infra/kraken/utils/dedup"
	"code.uber.internal/infra/kraken/utils/log"

	"github.com/andres-erbsen/clock"
	"github.com/docker/distribution/uuid"
	"github.com/uber-go/tally"
)

// Refresher errors.
var (
	ErrPending     = errors.New("download is pending")
	ErrNotFound    = errors.New("blob not found")
	ErrWorkersBusy = errors.New("no workers available")
)

// PostHook runs after the blob has been downloaded within the context of the
// deduplicated request.
type PostHook interface {
	Run(d core.Digest)
}

// Refresher deduplicates blob downloads / metainfo generation. Refresher is not
// responsible for tracking whether blobs already exist on disk -- it only provides
// a method for downloading blobs in a deduplicated fashion.
type Refresher struct {
	stats             tally.Scope
	requests          *dedup.RequestCache
	fs                store.OriginFileStore
	backends          *backend.Manager
	metaInfoGenerator *metainfogen.Generator
}

// New creates a new Refresher.
func New(
	stats tally.Scope,
	fs store.OriginFileStore,
	backends *backend.Manager,
	metaInfoGenerator *metainfogen.Generator) *Refresher {

	stats = stats.Tagged(map[string]string{
		"module": "blobrefresh",
	})

	requests := dedup.NewRequestCache(dedup.RequestCacheConfig{}, clock.New())
	requests.SetNotFound(func(err error) bool { return err == backenderrors.ErrBlobNotFound })

	return &Refresher{stats, requests, fs, backends, metaInfoGenerator}
}

// Refresh kicks off a background goroutine to download the blob for d from the
// remote backend configured for namespace and generates metainfo for the blob.
// Returns ErrPending if an existing download for the blob is already running.
// Returns ErrNotFound if the blob is not found. Returns ErrWorkersBusy if no
// goroutines are available to run the download.
func (r *Refresher) Refresh(namespace string, d core.Digest, hooks ...PostHook) error {
	client, err := r.backends.GetClient(namespace)
	if err != nil {
		return fmt.Errorf("backend manager: %s", err)
	}
	id := namespace + ":" + d.Hex()
	err = r.requests.Start(id, func() error {
		timer := r.stats.Timer("download_remote_blob").Start()
		if err := r.download(client, d); err != nil {
			return err
		}
		timer.Stop()
		if err := r.metaInfoGenerator.Generate(d); err != nil {
			return fmt.Errorf("generate metainfo: %s", err)
		}
		log.With("blob", d.Hex()).Info("Blob successfully refreshed")
		for _, h := range hooks {
			h.Run(d)
		}
		return nil
	})
	switch err {
	case dedup.ErrRequestPending:
		return ErrPending
	case backenderrors.ErrBlobNotFound:
		return ErrNotFound
	case dedup.ErrWorkersBusy:
		return ErrWorkersBusy
	default:
		return err
	}
}

func (r *Refresher) download(client backend.Client, d core.Digest) error {
	u := uuid.Generate().String()
	if err := r.fs.CreateUploadFile(u, 0); err != nil {
		return fmt.Errorf("create upload file: %s", err)
	}
	f, err := r.fs.GetUploadFileReadWriter(u)
	if err != nil {
		return fmt.Errorf("get upload writer: %s", err)
	}
	if err := client.Download(d.Hex(), f); err != nil {
		return err
	}
	if _, err := f.Seek(0, 0); err != nil {
		return fmt.Errorf("seek: %s", err)
	}
	fd, err := core.NewDigester().FromReader(f)
	if err != nil {
		return fmt.Errorf("compute digest: %s", err)
	}
	if fd != d {
		return fmt.Errorf("invalid remote blob digest: got %s, expected %s", fd, d)
	}
	if err := r.fs.MoveUploadFileToCache(u, d.Hex()); err != nil {
		return fmt.Errorf("move upload file to cache: %s", err)
	}
	return nil
}
