package transfer

// Downloader defines an interface to download blobs
type Downloader interface {
	Download(digest string) error
}

// Uploader defines an interface to upload blobs
type Uploader interface {
	Upload(digest string) error
}

// ManifestManager defines an interface to get and post manifest
type ManifestManager interface {
	GetManifest(repo, tag string) (digest string, err error)
	PostManifest(repo, tag, digest string) error
}

// ImageTransferer defines an interface that transfers images
type ImageTransferer interface {
	Downloader
	Uploader
	ManifestManager
}
