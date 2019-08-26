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
package tagmodels

import (
	"fmt"
	"io"
	"net/url"
)

const (
	// Filters.
	LimitQ  string = "limit"
	OffsetQ string = "offset"
)

// List Response with pagination. Models tagserver reponse to list and
// listRepository.
type ListResponse struct {
	Links struct {
		Next string `json:"next"`
		Self string `json:"self"`
	}
	Size   int      `json:"size"`
	Result []string `json:"result"`
}

// GetOffset returns offset token from the ListResponse struct.
// Returns token if present, io.EOF if Next is empty, error otherwise.
func (resp ListResponse) GetOffset() (string, error) {
	if resp.Links.Next == "" {
		return "", io.EOF
	}

	nextUrl, err := url.Parse(resp.Links.Next)
	if err != nil {
		return "", err
	}
	val, err := url.ParseQuery(nextUrl.RawQuery)
	if err != nil {
		return "", err
	}
	offset := val.Get(OffsetQ)
	if offset == "" {
		return "", fmt.Errorf("invalid offset in %s", resp.Links.Next)
	}
	return offset, nil
}
