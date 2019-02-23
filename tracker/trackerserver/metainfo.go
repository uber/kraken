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
package trackerserver

import (
	"fmt"
	"net/http"

	"github.com/uber/kraken/utils/handler"
	"github.com/uber/kraken/utils/httputil"
)

func (s *Server) getMetaInfoHandler(w http.ResponseWriter, r *http.Request) error {
	namespace, err := httputil.ParseParam(r, "namespace")
	if err != nil {
		return err
	}
	d, err := httputil.ParseDigest(r, "digest")
	if err != nil {
		return handler.Errorf("parse digest: %s", err).Status(http.StatusBadRequest)
	}

	timer := s.stats.Timer("get_metainfo").Start()
	mi, err := s.originCluster.GetMetaInfo(namespace, d)
	if err != nil {
		if serr, ok := err.(httputil.StatusError); ok {
			// Propagate errors received from origin.
			return handler.Errorf("origin: %s", serr.ResponseDump).Status(serr.Status)
		}
		return err
	}
	timer.Stop()

	b, err := mi.Serialize()
	if err != nil {
		return fmt.Errorf("serialize metainfo: %s", err)
	}
	w.Header().Set("Content-Type", "application/json")
	w.Write(b)
	return nil
}
