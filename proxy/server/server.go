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
package server

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/pressly/chi"
	"github.com/uber-go/tally"
	"github.com/uber/kraken/core"
	"github.com/uber/kraken/lib/middleware"
	"github.com/uber/kraken/origin/blobclient"
	"github.com/uber/kraken/utils/dockerutil"
	"github.com/uber/kraken/utils/handler"
	"github.com/uber/kraken/utils/httputil"
	"github.com/uber/kraken/utils/log"
	"net/http"
	_ "net/http/pprof" // Registers /debug/pprof endpoints in http.DefaultServeMux.
	"regexp"
	"time"
)

const manifestPattern = `^application/vnd.docker.distribution.manifest.v\d\+(json|prettyjws)`

// Server defines the proxy HTTP server.
type Server struct {
	stats         tally.Scope
	clusterClient blobclient.ClusterClient
}

// New creates a new Server.
func New(
	stats tally.Scope,
	client blobclient.ClusterClient) *Server {

	return &Server{stats, client}
}

// Handler returns the HTTP handler.
func (s *Server) Handler() http.Handler {
	r := chi.NewRouter()

	r.Use(middleware.StatusCounter(s.stats))
	r.Use(middleware.LatencyTimer(s.stats))

	r.Get("/health", handler.Wrap(s.healthHandler))

	r.Post("/notifications", handler.Wrap(s.preheat))

	// Serves /debug/pprof endpoints.
	r.Mount("/", http.DefaultServeMux)

	return r
}

// preheat notifies origins to cache the blob related to the image
func (s *Server) preheat(w http.ResponseWriter, r *http.Request) error {
	var notification Notification
	if err := json.NewDecoder(r.Body).Decode(&notification); err != nil {
		return handler.Errorf("decode body: %s", err)
	}

	events, _ := filterEvents(&notification)
	for _, event := range events {
		namespace := event.Target.Repository
		digest := event.Target.Digest

		log.Infof("deal push image event: %s:%s", namespace, digest)
		d, err := core.ParseSHA256Digest(digest)
		if err != nil {
			log.Errorf("Error parse digest %s:%s : %s", namespace, digest, err)
			continue
		}
		buf := &bytes.Buffer{}
		// there is a gap between registry finish uploading manifest and send notification
		// retry when not found
		i := 1
	MANIFEST:
		if err := s.clusterClient.DownloadBlob(namespace, d, buf); err != nil {
			if err == blobclient.ErrBlobNotFound && i < 3 {
				interval := 500 * i
				time.Sleep(time.Duration(interval) * time.Millisecond)
				i++
				goto MANIFEST
			} else {
				log.Errorf("download blob %s:%s : %s", namespace, digest, err)
				continue
			}
		}
		manifest, _, err := dockerutil.ParseManifestV2(buf)
		if err != nil {
			log.Errorf("parse manifest %s:%s : %s", namespace, digest, err)
			continue
		}

		for _, desc := range manifest.References() {
			d, err := core.ParseSHA256Digest(string(desc.Digest))
			if err != nil {
				log.Errorf("parse digest %s:%s : %s", namespace, string(desc.Digest), err)
				continue
			}
			go func() {
				log.Infof("trigger origin cache: %s:%+v", namespace, d)
				_, err = s.clusterClient.GetMetaInfo(namespace, d)
				if err != nil && !httputil.IsAccepted(err) {
					log.Errorf("notify origin cache %s:%s failed: %s", namespace, digest, err)
				}
			}()
		}
	}
	return nil
}

// filterEvents pick out the push manifest events
func filterEvents(notification *Notification) ([]*Event, error) {
	var events []*Event
	for _, event := range notification.Events {
		isManifest, err := regexp.MatchString(manifestPattern, event.Target.MediaType)
		if err != nil {
			continue
		}

		if !isManifest {
			continue
		}

		if event.Action == "push" {
			events = append(events, &event)
			continue
		}
	}
	return events, nil
}

func (s *Server) healthHandler(w http.ResponseWriter, r *http.Request) error {
	fmt.Fprintln(w, "OK")
	return nil
}
