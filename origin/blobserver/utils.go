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
package blobserver

import (
	"net/http"
	"os"
	"strconv"
	"strings"

	"github.com/uber/kraken/core"
	"github.com/uber/kraken/lib/store"
	"github.com/uber/kraken/utils/handler"
)

// parseContentRange parses start / end integers from a Content-Range header.
func parseContentRange(h http.Header) (start, end int64, err error) {
	contentRange := h.Get("Content-Range")
	if len(contentRange) == 0 {
		return 0, 0, handler.Errorf("no Content-Range header").Status(http.StatusBadRequest)
	}
	parts := strings.Split(contentRange, "-")
	if len(parts) != 2 {
		return 0, 0, handler.Errorf(
			"cannot parse Content-Range header %q: expected format \"start-end\"", contentRange).
			Status(http.StatusBadRequest)
	}
	start, err = strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return 0, 0, handler.Errorf(
			"cannot parse start of range in Content-Range header %q: %s", contentRange, err).
			Status(http.StatusBadRequest)
	}
	end, err = strconv.ParseInt(parts[1], 10, 64)
	if err != nil {
		return 0, 0, handler.Errorf(
			"cannot parse end of range in Content-Range header %q: %s", contentRange, err).
			Status(http.StatusBadRequest)
	}
	// Note, no need to check for negative because the "-" would cause the
	// Split check to fail.
	return start, end, nil
}

// blobExists returns true if cas has a cached blob for d.
func blobExists(cas *store.CAStore, d core.Digest) (bool, error) {
	if _, err := cas.GetCacheFileStat(d.Hex()); err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, handler.Errorf("cache file stat: %s", err)
	}
	return true, nil
}

func setUploadLocation(w http.ResponseWriter, uid string) {
	w.Header().Set("Location", uid)
}

func setContentLength(w http.ResponseWriter, n int) {
	w.Header().Set("Content-Length", strconv.Itoa(n))
}

func setOctetStreamContentType(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/octet-stream-v1")
}
