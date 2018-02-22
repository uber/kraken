package transfer

import (
	"code.uber.internal/infra/kraken/core"
	"code.uber.internal/infra/kraken/lib/store"
)

// Downloader defines an interface to download blobs
type Downloader interface {
	Download(name string) (store.FileReader, error)
}

// Uploader defines an interface to upload blobs
type Uploader interface {
	Upload(name string, blob store.FileReader, size int64) error
}

// TagManager defines an interface to get and post tags
type TagManager interface {
	GetTag(repo, tag string) (core.Digest, error)
	PostTag(repo, tag string, manifestDigest core.Digest) error
}

// ImageTransferer defines an interface that transfers images
type ImageTransferer interface {
	Downloader
	Uploader
	TagManager
}
