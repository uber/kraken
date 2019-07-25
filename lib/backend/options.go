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
package backend

// ListOptions defines the options which can be specified
// when listing names. It is used to enable pagination in list requests.
type ListOptions struct {
	Paginated bool
	MaxKeys int
	ContinuationToken string
}

// DefaultListOptions defines the defaults for list operations.
func DefaultListOptions() *ListOptions {
	return &ListOptions{
		Paginated: false,
		MaxKeys: DefaultListMaxKeys,
		ContinuationToken: "",
	}
}

// ListOption is used to configure list calls via variadic functional options.
type ListOption func(*ListOptions)

// ListWithPagination configures the list command to use pagination.
func ListWithPagination() ListOption {
	return func(opt *ListOptions) {
		opt.Paginated = true
	}
}

// ListWithMaxKeys configures the list command to return a max
// number of keys if pagination is enabled.
func ListWithMaxKeys(max int) ListOption {
	return func(opt *ListOptions) {
		opt.MaxKeys = max
	}
}

// ListWithContinuationToken configures the list command return
// results starting at the continuation token if pagination is enabled.
func ListWithContinuationToken(token string) ListOption {
	return func(opt *ListOptions) {
		opt.ContinuationToken = token
	}
}
