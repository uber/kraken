// Copyright (c) 2016-2019 Uber Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package gcsbackend

import (
	"io"

	"cloud.google.com/go/storage"
	"google.golang.org/api/iterator"
)

// GCS defines the operations we use in the GCS api. Useful for mocking.
type GCS interface {
	ObjectAttrs(objectName string) (*storage.ObjectAttrs, error)
	Download(objectName string, w io.Writer) (int64, error)
	Upload(objectName string, r io.Reader) (int64, error)
	GetObjectIterator(prefix string) iterator.Pageable
}
