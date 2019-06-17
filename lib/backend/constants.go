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
package backend

import (
	"github.com/uber/kraken/utils/memsize"

	"github.com/c2h5oh/datasize"
)

const (
	DefaultPartSize    int64             = int64(64 * memsize.MB)
	DefaultBufferGuard datasize.ByteSize = 10 * datasize.MB
	DefaultConcurrency int               = 10
	DefaultListMaxKeys int               = 250
)
