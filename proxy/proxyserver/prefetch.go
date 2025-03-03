package proxyserver

import (
	"context"
	"encoding/json"
	"github.com/uber/kraken/lib/containerruntime"
	"github.com/uber/kraken/origin/blobclient"
	"github.com/uber/kraken/utils/handler"
	"github.com/uber/kraken/utils/log"
	"net/http"
	"strings"
)

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

// PrefetchHandler defines the handler of preheat.
type PrefetchHandler struct {
	clusterClient    blobclient.ClusterClient
	containerRuntime containerruntime.Factory
}

// PrefetchBody defines the body of preheat.
type PrefetchBody struct {
	Tag string `json:"tag"`
}

// NewPrefetchHandler creates a new preheat handler.
func NewPrefetchHandler(client blobclient.ClusterClient, containerRuntime containerruntime.Factory) *PrefetchHandler {
	return &PrefetchHandler{client, containerRuntime}
}

// Handle notifies origins to cache the blob related to the image.
func (ph *PrefetchHandler) Handle(w http.ResponseWriter, r *http.Request) error {
	var prefetchBody PrefetchBody
	if err := json.NewDecoder(r.Body).Decode(&prefetchBody); err != nil {
		return handler.Errorf("decode body: %s", err)
	}
	split := strings.Split(prefetchBody.Tag, ":")
	log.Infof("Tag: %s", split[1])
	log.Infof("Repo: %s", split[0])
	return ph.preloadTagHandler(w, split[1], split[0])
}

// preloadTagHandler triggers docker daemon to download specified docker image.
func (ph *PrefetchHandler) preloadTagHandler(w http.ResponseWriter, tag string, repo string) error {
	if err := ph.containerRuntime.DockerClient().
		PullImage(context.Background(), repo, tag); err != nil {
		return handler.Errorf("docker pull: %s", err)
	}
	return nil
}
