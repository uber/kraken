package transfer

import (
	"code.uber.internal/infra/kraken/core"
	"code.uber.internal/infra/kraken/lib/store"
)

// Downloader defines an interface to download blobs
type Downloader interface {
	Download(namespace string, d core.Digest) (store.FileReader, error)
}

// Uploader defines an interface to upload blobs
type Uploader interface {
	Upload(namespace string, d core.Digest, blob store.FileReader) error
}

// TagManager defines an interface to get and post tags
type TagManager interface {
	GetTag(tag string) (core.Digest, error)
	PostTag(tag string, d core.Digest) error
	ListRepository(repo string) ([]string, error)
}

// ImageTransferer defines an interface that transfers images
type ImageTransferer interface {
	Downloader
	Uploader
	TagManager
}
