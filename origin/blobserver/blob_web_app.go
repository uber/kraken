package blobserver

import (
	"errors"
	"hash"
	"net/http"

	"code.uber.internal/infra/kraken/lib/hrw"
	"code.uber.internal/infra/kraken/lib/store"
	"code.uber.internal/infra/kraken/origin/client"

	"github.com/pressly/chi"
	"github.com/spaolacci/murmur3"
)

// BlobWebApp defines a web-app that serves blob data for agent.
type BlobWebApp struct {
	config              Config
	label               string
	hostname            string
	labelToHostname     map[string]string
	hashState           *hrw.RendezvousHash
	localStore          *store.LocalStore
	blobTransferFactory client.BlobTransferFactory
}

// New initializes a new BlobWebApp obj.
func New(
	config Config,
	hostname string,
	localStore *store.LocalStore,
	blobTransferFactory client.BlobTransferFactory) (*BlobWebApp, error) {

	if len(config.HashNodes) == 0 {
		return nil, errors.New("no hash nodes configured")
	}

	hashState := hrw.NewRendezvousHash(
		func() hash.Hash { return murmur3.New64() },
		hrw.UInt64ToFloat64)
	labelToHostname := make(map[string]string, len(config.HashNodes))
	for h, node := range config.HashNodes {
		hashState.AddNode(node.Label, node.Weight)
		labelToHostname[node.Label] = h
	}

	label := config.HashNodes[hostname].Label

	// TODO(codyg): Remove this once context is vanquished.
	config.Label = label
	config.Hostname = hostname
	config.LabelToHostname = labelToHostname

	return &BlobWebApp{
		config:              config,
		label:               label,
		hostname:            hostname,
		labelToHostname:     labelToHostname,
		hashState:           hashState,
		localStore:          localStore,
		blobTransferFactory: client.NewBlobAPIClient,
	}, nil
}

// Handler returns an http handler for the blob server.
func (app BlobWebApp) Handler() http.Handler {
	r := chi.NewRouter()

	r.Route("/blobs", func(r chi.Router) {
		r.Head("/:digest", app.CheckBlob)

		r.Get("/:digest", app.GetBlob)

		r.Delete("/:digest", app.DeleteBlob)

		r.Route("/uploads", func(r chi.Router) {
			r.Post("/", app.PostUpload)
			r.Patch("/:uuid", app.PatchUpload)
			r.Put("/:uuid", app.PutUpload)
		})
	})

	r.Route("/repair", func(r chi.Router) {
		r.Post("/shard/:shardID", app.RepairBlob)

		r.Post("/:digest", app.RepairBlob)
	})

	return r
}

// CheckBlob checks if blob data exists.
func (app BlobWebApp) CheckBlob(writer http.ResponseWriter, request *http.Request) {
	p := NewPipeline(request.Context(), app.config, app.hashState, app.localStore)
	p.AddRequestHandler(parseDigestHandler)
	p.AddRequestHandler(redirectByDigestHandler)
	p.AddRequestHandler(ensureDigestExistsHandler)
	p.AddResponseHandler(okHandler)
	p.Run(writer, request)
}

// GetBlob returns blob data for given digest.
func (app BlobWebApp) GetBlob(writer http.ResponseWriter, request *http.Request) {
	p := NewPipeline(request.Context(), app.config, app.hashState, app.localStore)
	p.AddRequestHandler(parseDigestHandler)
	p.AddRequestHandler(redirectByDigestHandler)
	p.AddResponseHandler(downloadBlobHandler)
	p.AddResponseHandler(okOctetStreamHandler)
	p.Run(writer, request)
}

// DeleteBlob removes blob data.
func (app BlobWebApp) DeleteBlob(writer http.ResponseWriter, request *http.Request) {
	p := NewPipeline(request.Context(), app.config, app.hashState, app.localStore)
	p.AddRequestHandler(parseDigestHandler)
	p.AddRequestHandler(deleteBlobHandler)
	p.AddResponseHandler(acceptedHandler)
	p.Run(writer, request)
}

// PostUpload start upload process for a blob.
// it returns a UUID, which is needed for subsequent uploads of this blob.
func (app BlobWebApp) PostUpload(writer http.ResponseWriter, request *http.Request) {
	p := NewPipeline(request.Context(), app.config, app.hashState, app.localStore)
	p.AddRequestHandler(parseDigestFromQueryHandler)
	p.AddRequestHandler(redirectByDigestHandler)
	p.AddRequestHandler(ensureDigestNotExistsHandler)
	p.AddRequestHandler(createUploadHandler)
	p.AddResponseHandler(returnUploadLocationHandler)
	p.Run(writer, request)
}

// PatchUpload upload a chunk of the blob.
func (app BlobWebApp) PatchUpload(writer http.ResponseWriter, request *http.Request) {
	p := NewPipeline(request.Context(), app.config, app.hashState, app.localStore)
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
	p := NewPipeline(request.Context(), app.config, app.hashState, app.localStore)
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
	p := NewPipeline(request.Context(), app.config, app.hashState, app.localStore)
	p.AddRequestHandler(parseRepairBlobHandler)
	p.AddResponseHandler(repairBlobStreamHandler)
	p.Run(writer, request)
}
