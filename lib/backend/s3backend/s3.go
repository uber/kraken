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
package s3backend

import (
	"io"

	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

// S3 defines the operations we use in the s3 api. Useful for mocking.
type S3 interface {
	HeadObject(input *s3.HeadObjectInput) (*s3.HeadObjectOutput, error)

	Download(
		w io.WriterAt,
		input *s3.GetObjectInput,
		options ...func(*s3manager.Downloader)) (n int64, err error)

	Upload(
		input *s3manager.UploadInput,
		options ...func(*s3manager.Uploader)) (*s3manager.UploadOutput, error)

	ListObjectsV2Pages(input *s3.ListObjectsV2Input, fn func(*s3.ListObjectsV2Output, bool) bool) error
}

type join struct {
	s3iface.S3API
	*s3manager.Downloader
	*s3manager.Uploader
}

var _ S3 = (*join)(nil)
