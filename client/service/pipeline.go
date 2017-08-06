package service

import (
	"context"
	"fmt"
	"net/http"

	"code.uber.internal/infra/kraken/client/store"

	"code.uber.internal/go-common.git/x/log"
)

// RequestHandler parses request and update context.
type RequestHandler func(context.Context, *http.Request) (context.Context, *ServerError)

// ResponseHandler get information from context and update response.
type ResponseHandler func(context.Context, http.ResponseWriter) (context.Context, *ServerError)

// Pipeline is in charge of processing requests. It's composed of a series of handlers.
type Pipeline struct {
	ctx              context.Context
	requestHandlers  []RequestHandler
	responseHandlers []ResponseHandler
}

// NewPipeline initialized a new pipeline.
func NewPipeline(ctx context.Context, localStore *store.LocalStore) *Pipeline {
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
	var err *ServerError
	for _, h := range p.requestHandlers {
		if p.ctx, err = h(p.ctx, request); err != nil {
			log.Errorf(err.Error())
			writer.WriteHeader(err.StatusCode())
			fmt.Fprintf(writer, err.Error())
			return
		}
	}
	for _, h := range p.responseHandlers {
		if p.ctx, err = h(p.ctx, writer); err != nil {
			log.Errorf(err.Error())
			writer.WriteHeader(err.StatusCode())
			fmt.Fprintf(writer, err.Error())
			return
		}
	}
}
