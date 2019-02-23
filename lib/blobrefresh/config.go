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
package blobrefresh

import "github.com/c2h5oh/datasize"

// Config defines Refresher configuration.
type Config struct {
	// Limits the size of blobs which origin will accept. A 0 size limit means
	// blob size is unbounded.
	SizeLimit datasize.ByteSize `yaml:"size_limit"`
}
