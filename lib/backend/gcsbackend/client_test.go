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
package gcsbackend

import (
	"bytes"
	"testing"
	"fmt"
	"strings"
	"strconv"

	"github.com/uber/kraken/core"
	"github.com/uber/kraken/mocks/lib/backend/gcsbackend"
	"github.com/uber/kraken/utils/mockutil"
	"github.com/uber/kraken/utils/randutil"
	"github.com/uber/kraken/utils/rwutil"

	"cloud.google.com/go/storage"
	"google.golang.org/api/iterator"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

type clientMocks struct {
	config   Config
	userAuth UserAuthConfig
	gcs      *mockgcsbackend.MockGCS
}

func newClientMocks(t *testing.T) (*clientMocks, func()) {
	ctrl := gomock.NewController(t)

	var auth AuthConfig
	auth.GCS.AccessBlob = "access_blob"

	return &clientMocks{
		config: Config{
			Username:      "test-user",
			Location:      "test-location",
			Bucket:        "test-bucket",
			NamePath:      "identity",
			RootDirectory: "/root",
			ListMaxKeys:   5,
		},
		userAuth: UserAuthConfig{"test-user": auth},
		gcs:       mockgcsbackend.NewMockGCS(ctrl),
	}, ctrl.Finish
}

func (m *clientMocks) new() *Client {
	c, err := NewClient(m.config, m.userAuth, WithGCS(m.gcs))
	if err != nil {
		panic(err)
	}
	return c
}

func TestClientFactory(t *testing.T) {
	require := require.New(t)

	config := Config{
		Username:      "test-user",
		Location:      "test-region",
		Bucket:        "test-bucket",
		NamePath:      "identity",
		RootDirectory: "/root",
	}
	var auth AuthConfig
	auth.GCS.AccessBlob = "access_blob"
	userAuth := UserAuthConfig{"test-user": auth}
	f := factory{}
	_, err := f.Create(config, userAuth)
	fmt.Println(err.Error())
	require.True(strings.Contains(err.Error(), "invalid gcs credentials"))
}

func TestClientStat(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newClientMocks(t)
	defer cleanup()

	client := mocks.new()

	var objectAttrs storage.ObjectAttrs
	objectAttrs.Size = 100

	mocks.gcs.EXPECT().ObjectAttrs("/root/test").Return(&objectAttrs, nil)

	info, err := client.Stat(core.NamespaceFixture(), "test")
	require.NoError(err)
	require.Equal(core.NewBlobInfo(100), info)
}

func TestClientDownload(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newClientMocks(t)
	defer cleanup()

	client := mocks.new()
	data := randutil.Text(32)

	mocks.gcs.EXPECT().Download(
		"/root/test",
		mockutil.MatchWriter(data),
	).Return(int64(len(data)), nil)

	w := make(rwutil.PlainWriter, len(data))
	require.NoError(client.Download(core.NamespaceFixture(), "test", w))
	require.Equal(data, []byte(w))
}

func TestClientUpload(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newClientMocks(t)
	defer cleanup()

	client := mocks.new()

	data := randutil.Text(32)
	dataReader := bytes.NewReader(data)

	mocks.gcs.EXPECT().Upload(
		"/root/test",
		gomock.Any(),
	).Return(int64(len(data)), nil)

	require.NoError(client.Upload(core.NamespaceFixture(), "test", dataReader))
}

func Alphabets() *AlphaIterator {
	it := &AlphaIterator{}
	it.pageInfo, it.nextFunc = iterator.NewPageInfo(
		it.next,
		func() int { return len(it.elems) },
		func() interface{} { e := it.elems; it.elems = nil; return e })
	return it
}

type AlphaIterator struct {
	pageInfo *iterator.PageInfo
	nextFunc func() error
	elems    []string
}

func (it *AlphaIterator) PageInfo() *iterator.PageInfo {
	return it.pageInfo
}

func (it *AlphaIterator) next(pageSize int, pageToken string) (string, error) {
	// A simple implementation that ignores the pageToken
	for i := 0; i < pageSize; i++ {
		it.elems = append(it.elems, "test/" + strconv.Itoa(i))
	}
	return "", nil
}

func TestClientList(t *testing.T) {
	require := require.New(t)

	mocks, cleanup := newClientMocks(t)
	defer cleanup()

	client := mocks.new()

	mocks.gcs.EXPECT().GetObjectIterator(
		"/root/test",
	).Return(Alphabets())

	names, err := client.List("test")
	require.NoError(err)
	require.Equal([]string{"test/0", "test/1", "test/2", "test/3", "test/4"}, names)
}
