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
package webhdfs

// FileStatus defines FILESTATUS response body.
type FileStatus struct {
	PathSuffix string `json:"pathSuffix"`
	Type       string `json:"type"`
	Length     int64  `json:"length"`
}

type fileStatusResponse struct {
	FileStatus FileStatus `json:"FileStatus"`
}

type listStatusResponse struct {
	FileStatuses struct {
		FileStatus []FileStatus `json:"FileStatus"`
	} `json:"FileStatuses"`
}
