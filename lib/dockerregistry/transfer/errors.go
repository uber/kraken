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
package transfer

import "errors"

// ErrBlobNotFound is returned when a blob is not found by transferer.
var ErrBlobNotFound = errors.New("blob not found")

// ErrTagNotFound is returned when a tag is not found by transferer.
var ErrTagNotFound = errors.New("tag not found")
