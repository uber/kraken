package transfer

import (
	"code.uber.internal/infra/kraken/core"
	"code.uber.internal/infra/kraken/lib/store"
)

// ImageTransferer defines an interface that transfers images
type ImageTransferer interface {
	Stat(namespace string, d core.Digest) (*core.BlobInfo, error)
	Download(namespace string, d core.Digest) (store.FileReader, error)
	Upload(namespace string, d core.Digest, blob store.FileReader) error

	GetTag(tag string) (core.Digest, error)
	PutTag(tag string, d core.Digest) error
	ListTags(prefix string) ([]string, error)
}
