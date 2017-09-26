package blobserver

import (
	"hash"
	"net/http"

	"code.uber.internal/infra/kraken/client/store"
	"code.uber.internal/infra/kraken/lib/hrw"
	"code.uber.internal/infra/kraken/origin/client"
	hashcfg "code.uber.internal/infra/kraken/origin/config"

	"github.com/pressly/chi"
	"github.com/spaolacci/murmur3"
)

// InitializeAPI instantiates a new web-app for the origin
func InitializeAPI(storeConfig *store.Config, hashConfig hashcfg.HashConfig) http.Handler {
	r := chi.NewRouter()
	webApp := NewBlobWebApp(storeConfig, hashConfig)

	// General Blobs CRUD interface
	r.Route("/blobs", func(r chi.Router) {
		// Check data blob
		r.Head("/:digest", webApp.CheckBlob)

		// Pulling data blob
		r.Get("/:digest", webApp.GetBlob)

		// Delete data blob
		r.Delete("/:digest", webApp.DeleteBlob)

		// Pushing data blob
		r.Route("/uploads", func(r chi.Router) {
			r.Post("/", webApp.PostUpload)
			r.Patch("/:uuid", webApp.PatchUpload)
			r.Put("/:uuid", webApp.PutUpload)
		})
	})

	// General repair interface
	r.Route("/repair", func(r chi.Router) {
		// Repair data blob
		r.Post("/shard/:shardID", webApp.RepairBlob)

		// Repair a single data blob item
		r.Post("/:digest", webApp.RepairBlob)
	})
	return r
}

// NewBlobWebApp initializes a new BlobWebApp obj.
func NewBlobWebApp(storeConfig *store.Config, hashConfig hashcfg.HashConfig) *BlobWebApp {
	if len(hashConfig.HashNodes) == 0 {
		panic("Hashstate has zero length: `0 any_operation X = 0`")
	}

	// initalize hashing state
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
		hashConfig:          hashConfig,
		hashState:           hashState,
		localStore:          ls,
		blobTransferFactory: client.NewBlobAPIClient,
	}
}

// BlobWebApp defines a web-app that serves blob data for agent.
type BlobWebApp struct {
	hashConfig hashcfg.HashConfig

	hashState           *hrw.RendezvousHash
	localStore          *store.LocalStore
	blobTransferFactory client.BlobTransferFactory
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

// RepairBlob runs blob repair by shard ID, ensuring all the digests of a shard
// are synced properly to target nodes
func (app BlobWebApp) RepairBlob(writer http.ResponseWriter, request *http.Request) {
	p := NewPipeline(request.Context(), app.hashConfig, app.hashState, app.localStore)
	p.AddRequestHandler(parseRepairBlobHandler)
	p.AddResponseHandler(repairBlobStreamHandler)
	p.Run(writer, request)

}
