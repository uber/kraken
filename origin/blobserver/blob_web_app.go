package blobserver

import (
	"hash"
	"net/http"

	"code.uber.internal/infra/kraken/client/store"
	"code.uber.internal/infra/kraken/lib/hrw"
	hashcfg "code.uber.internal/infra/kraken/origin/config"

	"github.com/pressly/chi"
	"github.com/spaolacci/murmur3"
)

// InitializeAPI instantiates a new web-app for the origin
func InitializeAPI(storeConfig *store.Config, hashConfig hashcfg.HashConfig) http.Handler {
	r := chi.NewRouter()
	webApp := NewBlobWebApp(storeConfig, hashConfig)

	// Check data blob
	r.Head("/blobs/:digest", webApp.CheckBlob)

	// Pulling data blob
	r.Get("/blobs/:digest", webApp.GetBlob)

	// Delete data blob
	r.Delete("/blobs/:digest", webApp.DeleteBlob)

	// Pushing data blob
	r.Post("/blobs/uploads", webApp.PostUpload)
	r.Patch("/blobs/uploads/:uuid", webApp.PatchUpload)
	r.Put("/blobs/uploads/:uuid", webApp.PutUpload)

	return r
}

// NewBlobWebApp initializes a new BlobWebApp obj.
func NewBlobWebApp(storeConfig *store.Config, hashConfig hashcfg.HashConfig) *BlobWebApp {
	if len(hashConfig.HashNodes) == 0 {
		panic("Hashstate has zero length: `0 any_operation X = 0`")
	}

	// Initalize hashing state
	hashState := hrw.NewRendezvousHash(
		func() hash.Hash { return murmur3.New64() },
		hrw.UInt64ToFloat64)

	// Add all configured nodes to a hashing state
	for _, node := range hashConfig.HashNodes {
		hashState.AddNode(node.Label, node.Weight)
	}

	ls, err := store.NewLocalStore(storeConfig, true)
	if err != nil {
		panic("Could not create local store for blob web app")
	}

	return &BlobWebApp{
		hashConfig: hashConfig,
		hashState:  hashState,
		localStore: ls,
	}
}

// BlobWebApp defines a web-app that serves blob data for agent.
type BlobWebApp struct {
	hashConfig hashcfg.HashConfig

	hashState  *hrw.RendezvousHash
	localStore *store.LocalStore
}

// CheckBlob checks if blob data exists.
func (app BlobWebApp) CheckBlob(writer http.ResponseWriter, request *http.Request) {
	p := NewPipeline(request.Context(), app.hashConfig, app.hashState, app.localStore)
	p.AddRequestHandler(parseDigestHandler)
	p.AddRequestHandler(redirectByDigestHandler)
	p.AddRequestHandler(ensureDigestExistsHandler)
	p.AddResponseHandler(okHandler)
	p.Run(writer, request)
}

// GetBlob returns blob data for given digest.
func (app BlobWebApp) GetBlob(writer http.ResponseWriter, request *http.Request) {
	p := NewPipeline(request.Context(), app.hashConfig, app.hashState, app.localStore)
	p.AddRequestHandler(parseDigestHandler)
	p.AddRequestHandler(redirectByDigestHandler)
	p.AddResponseHandler(downloadBlobHandler)
	p.AddResponseHandler(okOctetStreamHandler)
	p.Run(writer, request)
}

// DeleteBlob removes blob data.
func (app BlobWebApp) DeleteBlob(writer http.ResponseWriter, request *http.Request) {
	p := NewPipeline(request.Context(), app.hashConfig, app.hashState, app.localStore)
	p.AddRequestHandler(parseDigestHandler)
	p.AddRequestHandler(deleteBlobHandler)
	p.AddResponseHandler(acceptedHandler)
	p.Run(writer, request)
}

// PostUpload start upload process for a blob.
// it returns a UUID, which is needed for subsequent uploads of this blob.
func (app BlobWebApp) PostUpload(writer http.ResponseWriter, request *http.Request) {
	p := NewPipeline(request.Context(), app.hashConfig, app.hashState, app.localStore)
	p.AddRequestHandler(parseDigestFromQueryHandler)
	p.AddRequestHandler(redirectByDigestHandler)
	p.AddRequestHandler(ensureDigestNotExistsHandler)
	p.AddRequestHandler(createUploadHandler)
	p.AddResponseHandler(returnUploadLocationHandler)
	p.Run(writer, request)
}

// PatchUpload upload a chunk of the blob.
func (app BlobWebApp) PatchUpload(writer http.ResponseWriter, request *http.Request) {
	p := NewPipeline(request.Context(), app.hashConfig, app.hashState, app.localStore)
	p.AddRequestHandler(parseDigestFromQueryHandler)
	p.AddRequestHandler(redirectByDigestHandler)
	p.AddRequestHandler(parseUUIDHandler)
	p.AddRequestHandler(parseContentRangeHandler)
	p.AddRequestHandler(ensureDigestNotExistsHandler)
	p.AddRequestHandler(uploadBlobChunkHandler)
	p.AddResponseHandler(returnUploadLocationHandler)
	p.Run(writer, request)
}

// PutUpload commits the upload.
func (app BlobWebApp) PutUpload(writer http.ResponseWriter, request *http.Request) {
	p := NewPipeline(request.Context(), app.hashConfig, app.hashState, app.localStore)
	p.AddRequestHandler(parseDigestFromQueryHandler)
	p.AddRequestHandler(redirectByDigestHandler)
	p.AddRequestHandler(parseUUIDHandler)
	p.AddRequestHandler(parseContentRangeHandler)
	p.AddRequestHandler(commitUploadHandler)
	p.AddResponseHandler(createdHandler)
	p.Run(writer, request)
}
