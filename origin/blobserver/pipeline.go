package blobserver

import (
	"context"
	"fmt"
	"net/http"

	"code.uber.internal/infra/kraken/lib/hrw"
	"code.uber.internal/infra/kraken/lib/store"

	"code.uber.internal/go-common.git/x/log"
)

// RequestHandler parses request and update context.
type RequestHandler func(context.Context, *http.Request) (context.Context, *ServerResponse)

// ResponseHandler get information from context and update response.
type ResponseHandler func(context.Context, http.ResponseWriter) (context.Context, *ServerResponse)

// Pipeline is in charge of processing requests. It's composed of a series of handlers.
type Pipeline struct {
	ctx              context.Context
	requestHandlers  []RequestHandler
	responseHandlers []ResponseHandler
}

// NewPipeline initialized a new pipeline.
func NewPipeline(ctx context.Context, config Config, hashState *hrw.RendezvousHash, localStore *store.LocalStore) *Pipeline {
	ctx = context.WithValue(ctx, ctxKeyHashConfig, config)
	ctx = context.WithValue(ctx, ctxKeyHashState, hashState)
	ctx = context.WithValue(ctx, ctxKeyLocalStore, localStore)
	return &Pipeline{
		ctx:              ctx,
		requestHandlers:  make([]RequestHandler, 1),
		responseHandlers: make([]ResponseHandler, 1),
	}
}

// AddRequestHandler appends a RequestHandler to the end of handler list.
func (p *Pipeline) AddRequestHandler(h RequestHandler) {
	p.requestHandlers = append(p.requestHandlers, h)
}

// AddResponseHandler appends a ResponseHandler to the end of handler list.
func (p *Pipeline) AddResponseHandler(h ResponseHandler) {
	p.responseHandlers = append(p.responseHandlers, h)
}

// Run applies handlers on the request.
func (p *Pipeline) Run(writer http.ResponseWriter, request *http.Request) {
	var resp *ServerResponse
	for _, h := range p.requestHandlers {
		if p.ctx, resp = h(p.ctx, request); resp != nil {
			writer.WriteHeader(resp.GetStatusCode())
			if resp.Error() != "" {
				log.Errorf(resp.Error())
				fmt.Fprintf(writer, resp.Error())
			}
			return
		}
	}
	for _, h := range p.responseHandlers {
		if p.ctx, resp = h(p.ctx, writer); resp != nil {
			writer.WriteHeader(resp.GetStatusCode())
			if resp.Error() != "" {
				log.Errorf(resp.Error())
				fmt.Fprintf(writer, resp.Error())
			}
			return
		}
	}
}
