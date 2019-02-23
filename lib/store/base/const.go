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
package base

// DefaultShardIDLength is the number of bytes of file digest to be used for shard ID.
// For every byte (2 HEX char), one more level of directories will be created.
const DefaultShardIDLength = 2

// DefaultDirPermission is the default permission for new directories.
const DefaultDirPermission = 0775

// DefaultDataFileName is the name of the actual blob data file.
const DefaultDataFileName = "data"
