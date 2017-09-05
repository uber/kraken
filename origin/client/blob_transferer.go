package client

import (
	"code.uber.internal/infra/kraken/lib/dockerregistry/image"
)

// BlobTransferer interface abstracts away a functional model
// of transferring blob data between two origin servers
type BlobTransferer interface {

	//PullBlob pulls content identified by digest to a
	//a local file storage
	PullBlob(digest image.Digest) error

	//PushBlob pushes content identified by digest to a
	//a remote files torage
	PushBlob(digest image.Digest) error

	//CheckBlobExists checks is digest item exists
	//on a remote server
	CheckBlobExists(digest image.Digest) (bool, error)
}
