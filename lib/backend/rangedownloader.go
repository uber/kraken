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

// RangeDownloader is optionally implemented by backend clients that support
// efficient byte-range fetches without downloading the full blob.
type RangeDownloader interface {
	DownloadRange(namespace, name string, dst io.Writer, offset, length int64) error
}

// AsRangeDownloader returns a RangeDownloader from c, unwrapping ThrottledClient
// if needed. Returns (nil, false) when the underlying client lacks range support.
func AsRangeDownloader(c Client) (RangeDownloader, bool) {
	if rd, ok := c.(RangeDownloader); ok {
		return rd, true
	}
	if tc, ok := c.(*ThrottledClient); ok {
		if rd, ok := tc.Client.(RangeDownloader); ok {
			return rd, true
		}
	}
	return nil, false
}
