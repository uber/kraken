package blobserver

import (
	"net/http"

	"code.uber.internal/infra/kraken/client/store"
	"code.uber.internal/infra/kraken/configuration"

	"github.com/pressly/chi"
)

// InitializeAPI instantiates a new web-app for the origin
func InitializeAPI(c *configuration.Config) http.Handler {
	r := chi.NewRouter()
	webApp := NewBlobWebApp(c)

	// Pulling data blob
	r.Get("/blobs/:digest", webApp.GetBlob)

	// Pushing data blob
	r.Post("/blobs/uploads", webApp.PostUpload)
	r.Patch("/blobs/uploads/:uuid", webApp.PatchUpload)
	r.Put("/blobs/uploads/:uuid", webApp.PutUpload)

	return r
}

// NewBlobWebApp initializes a new BlobWebApp obj.
func NewBlobWebApp(c *configuration.Config) *BlobWebApp {
	return &BlobWebApp{
		localStore: store.NewLocalStore(c),
	}
}

// BlobWebApp defines a web-app that serves blob data for agent.
type BlobWebApp struct {
	localStore *store.LocalStore
}

// GetBlob returns blob data for given digest.
func (app BlobWebApp) GetBlob(writer http.ResponseWriter, request *http.Request) {
	p := NewPipeline(request.Context(), app.localStore)
	p.AddRequestHandler(parseDigestHandler)
	p.AddResponseHandler(downloadBlobHandler)
	p.AddResponseHandler(okOctetStreamHandler)
	p.Run(writer, request)
}

// PostUpload start upload process for a blob.
// it returns a UUID, which is needed for subsequent uploads of this blob.
func (app BlobWebApp) PostUpload(writer http.ResponseWriter, request *http.Request) {
	p := NewPipeline(request.Context(), app.localStore)
	p.AddRequestHandler(parseDigestFromQueryHandler)
	p.AddRequestHandler(ensureDigestNotExistsHandler)
	p.AddRequestHandler(createUploadHandler)
	p.AddResponseHandler(returnUploadLocationHandler)
	p.Run(writer, request)
}

// PatchUpload upload a chunk of the blob.
func (app BlobWebApp) PatchUpload(writer http.ResponseWriter, request *http.Request) {
	p := NewPipeline(request.Context(), app.localStore)
	p.AddRequestHandler(parseUUIDHandler)
	p.AddRequestHandler(parseContentRangeHandler)
	p.AddRequestHandler(parseDigestFromQueryHandler)
	p.AddRequestHandler(ensureDigestNotExistsHandler)
	p.AddRequestHandler(uploadBlobChunkHandler)
	p.AddResponseHandler(returnUploadLocationHandler)
	p.Run(writer, request)
}

// PutUpload commits the upload.
func (app BlobWebApp) PutUpload(writer http.ResponseWriter, request *http.Request) {
	p := NewPipeline(request.Context(), app.localStore)
	p.AddRequestHandler(parseUUIDHandler)
	p.AddRequestHandler(parseContentRangeHandler)
	p.AddRequestHandler(parseDigestFromQueryHandler)
	p.AddRequestHandler(commitUploadHandler)
	p.AddResponseHandler(returnUploadLocationHandler)
	p.Run(writer, request)
}

// DeleteBlob removes blob data.
func (app BlobWebApp) DeleteBlob(writer http.ResponseWriter, request *http.Request) {
	p := NewPipeline(request.Context(), app.localStore)
	p.AddRequestHandler(parseDigestHandler)
	p.AddRequestHandler(deleteBlobHandler)
	p.AddResponseHandler(acceptedHandler)
	p.Run(writer, request)
}
