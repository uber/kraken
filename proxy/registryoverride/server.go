// Copyright (c) 2016-2019 Uber Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package registryoverride

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/go-chi/chi"
	"github.com/uber/kraken/build-index/tagclient"
	"github.com/uber/kraken/utils/handler"
	"github.com/uber/kraken/utils/listener"
	"github.com/uber/kraken/utils/log"
	"github.com/uber/kraken/utils/stringset"
)

// Server overrides Docker registry endpoints.
type Server struct {
	config    Config
	tagClient tagclient.Client
}

// NewServer creates a new Server.
func NewServer(config Config, tagClient tagclient.Client) *Server {
	return &Server{config, tagClient}
}

// Handler returns a handler for s.
func (s *Server) Handler() http.Handler {
	r := chi.NewRouter()
	r.Get("/v2/_catalog", handler.Wrap(s.catalogHandler))
	return r
}

// ListenAndServe is a blocking call which runs s.
func (s *Server) ListenAndServe() error {
	log.Infof("Starting registry override server on %s", s.config.Listener)
	return listener.Serve(s.config.Listener, s.Handler())
}

type catalogResponse struct {
	Repositories []string `json:"repositories"`
}

// catalogHandler handles catalog request.
// https://docs.docker.com/registry/spec/api/#pagination for more reference.
func (s *Server) catalogHandler(w http.ResponseWriter, r *http.Request) error {
	limitQ := "n"
	offsetQ := "last"

	// Build request for ListWithPagination.
	var filter tagclient.ListFilter
	u := r.URL
	q := u.Query()
	for k, v := range q {
		if len(v) != 1 {
			return handler.Errorf(
				"invalid query %s:%s", k, v).Status(http.StatusBadRequest)
		}
		switch k {
		case limitQ:
			limitCount, err := strconv.Atoi(v[0])
			if err != nil {
				return handler.Errorf(
					"invalid limit %s: %s", v, err).Status(http.StatusBadRequest)
			}
			if limitCount == 0 {
				return handler.Errorf(
					"invalid limit %d", limitCount).Status(http.StatusBadRequest)
			}
			filter.Limit = limitCount
		case offsetQ:
			filter.Offset = v[0]
		default:
			return handler.Errorf("invalid query %s", k).Status(http.StatusBadRequest)
		}
	}

	// List with pagination.
	listResp, err := s.tagClient.ListWithPagination("", filter)
	if err != nil {
		return handler.Errorf("list: %s", err)
	}
	repos := stringset.New()
	for _, tag := range listResp.Result {
		parts := strings.Split(tag, ":")
		if len(parts) != 2 {
			log.With("tag", tag).Errorf("Invalid tag format, expected repo:tag")
			continue
		}
		repos.Add(parts[0])
	}

	// Build Link for response.
	offset, err := listResp.GetOffset()
	if err != nil && err != io.EOF {
		return handler.Errorf("invalid offset %s", err)
	}
	if offset != "" {
		nextUrl, err := url.Parse(u.String())
		if err != nil {
			return handler.Errorf(
				"invalid url string: %s", err).Status(http.StatusBadRequest)
		}
		val, err := url.ParseQuery(nextUrl.RawQuery)
		if err != nil {
			return handler.Errorf(
				"invalid url string: %s", err).Status(http.StatusBadRequest)
		}
		val.Set(offsetQ, offset)
		nextUrl.RawQuery = val.Encode()

		// Set header (https://docs.docker.com/registry/spec/api/#pagination),
		// except the host and scheme.
		// Link: <<url>?n=2&last=b>; rel="next"
		w.Header().Set("Link", fmt.Sprintf("%s; rel=\"next\"", nextUrl.String()))
	}

	resp := catalogResponse{Repositories: repos.ToSlice()}
	if err := json.NewEncoder(w).Encode(&resp); err != nil {
		return handler.Errorf("json encode: %s", err)
	}
	return nil
}
