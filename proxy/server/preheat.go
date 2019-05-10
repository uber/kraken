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
	"net/http"
	_ "net/http/pprof" // Registers /debug/pprof endpoints in http.DefaultServeMux.
	"github.com/uber/kraken/core"
	"github.com/uber/kraken/utils/handler"
	"github.com/uber/kraken/origin/blobclient"
	"encoding/json"
	"regexp"
	"bytes"
	"github.com/uber/kraken/utils/log"
	"github.com/uber/kraken/utils/dockerutil"
	"github.com/uber/kraken/utils/httputil"
	"time"
	"fmt"
	"github.com/docker/distribution"
)

const manifestPattern = `^application/vnd.docker.distribution.manifest.v\d\+(json|prettyjws)`

// Interface defines the interface of http handler
type Interface interface {
	Handle(w http.ResponseWriter, r *http.Request) error
}

type PreheatHandler struct {
	clusterClient blobclient.ClusterClient
}

// NewPreheatHandler creates a new preheat handler.
func NewPreheatHandler(client blobclient.ClusterClient) Interface {
	return &PreheatHandler{client}
}

// Handle notifies origins to cache the blob related to the image
func (ph *PreheatHandler) Handle(w http.ResponseWriter, r *http.Request) error {
	var notification Notification
	if err := json.NewDecoder(r.Body).Decode(&notification); err != nil {
		return handler.Errorf("decode body: %s", err)
	}

	events := filterEvents(&notification)
	for _, event := range events {
		namespace := event.Target.Repository
		digest := event.Target.Digest

		log.Infof("deal push image event: %s:%s", namespace, digest)
		err := ph.process(namespace, digest)
		if err != nil {
			log.Errorf("preheat %s:%s failed, %s", namespace, digest, err)
		}
	}
	return nil
}

func (ph *PreheatHandler) process(namespace, digest string) error {
	manifest, err := ph.fetchManifest(namespace, digest)
	if err != nil {
		return err
	}
	for _, desc := range manifest.References() {
		d, err := core.ParseSHA256Digest(string(desc.Digest))
		if err != nil {
			log.Errorf("parse digest %s:%s : %s", namespace, string(desc.Digest), err)
			continue
		}
		go func() {
			log.Infof("trigger origin cache: %s:%+v", namespace, d)
			_, err = ph.clusterClient.GetMetaInfo(namespace, d)
			if err != nil && !httputil.IsAccepted(err) {
				log.Errorf("notify origin cache %s:%s failed: %s", namespace, digest, err)
			}
		}()
	}
	return nil
}

func (ph *PreheatHandler) fetchManifest(namespace, digest string) (distribution.Manifest, error) {
	d, err := core.ParseSHA256Digest(digest)
	if err != nil {
		return nil, fmt.Errorf("Error parse digest: %s ", err)
	}

	buf := &bytes.Buffer{}
	// there may be a gap between registry finish uploading manifest and send notification
	// see https://github.com/docker/distribution/issues/2625
	// retry when not found, retry 3 times with interval 100ms 200ms 400ms
	interval := 100 * time.Millisecond
	for i := 0; i < 4; i++ {
		if i != 0 {
			time.Sleep(interval)
			interval = interval * 2
		}
		if err := ph.clusterClient.DownloadBlob(namespace, d, buf); err == nil {
			break
		} else if err == blobclient.ErrBlobNotFound {
			continue
		} else {
			return nil, fmt.Errorf("download manifest failed: %s", err)
		}
	}
	if buf.Len() == 0 {
		return nil, fmt.Errorf("manifest not found")
	}

	manifest, _, err := dockerutil.ParseManifestV2(buf)
	if err != nil {
		return nil, fmt.Errorf("Error parse manifest: %s ", err)
	}
	return manifest, nil
}

// filterEvents pick out the push manifest events
func filterEvents(notification *Notification) []*Event {
	events := []*Event{}

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
	return events
}
