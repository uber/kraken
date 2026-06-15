// Copyright (c) 2016-2019 Uber Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package backend

import "io"

// RangeDownloader is an optional Client capability for fetching only a byte
// range of a blob. Backends that support it let a cold origin lazily stream
// individual pieces instead of downloading the whole blob.
type RangeDownloader interface {
	// DownloadRange downloads length bytes of name starting at offset into dst.
	DownloadRange(namespace, name string, dst io.Writer, offset, length int64) error
}

// AsRangeDownloader returns c as a RangeDownloader if it supports ranged
// downloads, unwrapping the throttle wrapper. ok is false when the backend
// does not support ranged downloads, in which case callers should fall back to
// a whole-blob download.
func AsRangeDownloader(c Client) (RangeDownloader, bool) {
	if tc, ok := c.(*ThrottledClient); ok {
		c = tc.Client
	}
	rd, ok := c.(RangeDownloader)
	return rd, ok
}
