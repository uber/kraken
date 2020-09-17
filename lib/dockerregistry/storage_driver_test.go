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
package dockerregistry

import (
	"context"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"testing"
	"time"

	"github.com/docker/distribution/registry/storage/driver"
	"github.com/docker/distribution/uuid"
	"github.com/stretchr/testify/require"
	"github.com/uber/kraken/core"
	"github.com/uber/kraken/utils/randutil"
)

func TestStorageDriverGetContent(t *testing.T) {
	td, cleanup := newTestDriver()
	defer cleanup()

	sd, testImage := td.setup()

	var sa startedAtMetadata
	if err := td.cas.GetUploadFileMetadata(testImage.upload, &sa); err != nil {
		log.Panic(err)
	}
	uploadTime, err := sa.Serialize()
	if err != nil {
		log.Panic(err)
	}
	newBlobPath := genBlobDataPath(core.DigestFixture().Hex())

	testCases := []struct {
		input string
		data  []byte
		err   error
	}{
		{genLayerLinkPath(testImage.layer1.Digest.Hex()), []byte(testImage.layer1.Digest.String()), nil},
		{genUploadStartedAtPath(testImage.upload), uploadTime, nil},
		{genUploadHashStatesPath(testImage.upload), []byte(hashStateContent), nil},
		{genManifestTagCurrentLinkPath(testImage.repo, testImage.tag, testImage.manifest), []byte("sha256:" + testImage.manifest), nil},
		{genManifestRevisionLinkPath(testImage.repo, testImage.manifest), []byte("sha256:" + testImage.manifest), nil},
		{genBlobDataPath(testImage.layer1.Digest.Hex()), testImage.layer1.Content, nil},
		{newBlobPath, nil, driver.PathNotFoundError{DriverName: "kraken", Path: newBlobPath}},
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("GetContent %s", tc.input), func(t *testing.T) {
			require := require.New(t)
			data, err := sd.GetContent(contextFixture(), tc.input)
			if tc.err == nil {
				require.NoError(err)
				return
			}
			require.Equal(tc.data, data)
			require.Equal(tc.err, err)
		})
	}
}

func TestStorageDriverReader(t *testing.T) {
	td, cleanup := newTestDriver()
	defer cleanup()

	sd, testImage := td.setup()
	newBlobPath := genBlobDataPath(core.DigestFixture().Hex())

	testCases := []struct {
		input string
		data  []byte
		err   error
	}{
		{genUploadDataPath(testImage.upload), []byte(uploadContent), nil},
		{genBlobDataPath(testImage.layer1.Digest.Hex()), testImage.layer1.Content, nil},
		{newBlobPath, nil, driver.PathNotFoundError{DriverName: "kraken", Path: newBlobPath}},
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("GetReader %s", tc.input), func(t *testing.T) {
			require := require.New(t)
			reader, err := sd.Reader(contextFixture(), tc.input, 0)
			if tc.err != nil {
				require.Equal(tc.err, err)
				return
			}
			data, err := ioutil.ReadAll(reader)
			require.Equal(tc.data, data)
			require.Equal(tc.err, err)
		})
	}
}

func TestStorageDriverPutContent(t *testing.T) {
	td, cleanup := newTestDriver()
	defer cleanup()

	sd, testImage := td.setup()

	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	imageID := r.Int()
	repo := fmt.Sprintf("repo-%d", imageID)
	tag := fmt.Sprintf("tag-%d", imageID)
	upload := fmt.Sprintf("upload-%d", imageID)

	testCases := []struct {
		inputPath    string
		inputContent []byte
		err          error
	}{
		{genUploadStartedAtPath(upload), nil, nil},
		{genUploadHashStatesPath(testImage.upload), []byte{}, nil},
		{genLayerLinkPath(testImage.layer1.Digest.Hex()), nil, nil},
		{genBlobDataPath(testImage.layer1.Digest.Hex()), testImage.layer1.Content, nil},
		{genManifestRevisionLinkPath(repo, testImage.manifest), nil, nil},
		{genManifestTagShaLinkPath(repo, tag, testImage.manifest), nil, nil},
		{genManifestTagCurrentLinkPath(repo, tag, testImage.manifest), nil, nil},
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("PutContent %s", tc.inputPath), func(t *testing.T) {
			require.Equal(t, tc.err, sd.PutContent(contextFixture(), tc.inputPath, tc.inputContent))
		})
	}

	// TODO (@evelynl): check content written
}

func TestStorageDriverWriter(t *testing.T) {
	td, cleanup := newTestDriver()
	defer cleanup()

	sd, testImage := td.setup()
	newUploadPath := genUploadDataPath(uuid.Generate().String())

	testCases := []struct {
		input string
		data  []byte
		err   error
	}{
		{genUploadDataPath(testImage.upload), []byte(uploadContent), nil},
		{newUploadPath, nil, driver.PathNotFoundError{DriverName: "kraken", Path: newUploadPath}},
		{genBlobDataPath(testImage.layer1.Digest.Hex()), nil, InvalidRequestError{genBlobDataPath(testImage.layer1.Digest.Hex())}},
	}

	content := []byte("this is a test for upload writer")
	for _, tc := range testCases {
		t.Run(fmt.Sprintf("GetWriter %s", tc.input), func(t *testing.T) {
			require := require.New(t)
			w, err := sd.Writer(contextFixture(), tc.input, false)
			require.Equal(tc.err, err)
			if err != nil {
				return
			}
			w.Write(content)
			w.Close()
			r, err := sd.Reader(contextFixture(), tc.input, 0)
			require.NoError(err)
			defer r.Close()
			data, err := ioutil.ReadAll(r)
			require.NoError(err)
			require.Equal(content, data)
		})
	}
}

func TestStorageDriverStat(t *testing.T) {
	td, cleanup := newTestDriver()
	defer cleanup()

	sd, testImage := td.setup()
	newManifestPath := genManifestTagCurrentLinkPath(string(randutil.Text(4)), string(randutil.Text(4)), core.DigestFixture().Hex())

	testCases := []struct {
		input string
		path  string
		size  int64
		err   error
	}{
		{genBlobDataPath(testImage.layer1.Digest.Hex()), testImage.layer1.Digest.Hex(), int64(len(testImage.layer1.Content)), nil},
		{genUploadDataPath(testImage.upload), testImage.upload, int64(len(uploadContent)), nil},
		{newManifestPath, "", 0, driver.PathNotFoundError{DriverName: "kraken", Path: newManifestPath}},
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("GetStat %s", tc.input), func(t *testing.T) {
			require := require.New(t)
			fi, err := sd.Stat(contextFixture(), tc.input)
			require.Equal(tc.err, err)
			if err != nil {
				return
			}
			require.Equal(tc.path, fi.Path())
			require.Equal(tc.size, fi.Size())
		})
	}
}

func TestStorageDriverList(t *testing.T) {
	td, cleanup := newTestDriver()
	defer cleanup()

	sd, testImage := td.setup()

	testCases := []struct {
		input string
		list  []string
		err   error
	}{
		{genUploadHashStatesPath(testImage.upload), []string{genUploadHashStatesPath(testImage.upload)}, nil},
		{genManifestListPath(testImage.repo), []string{testImage.tag}, nil},
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("List %s", tc.input), func(t *testing.T) {
			require := require.New(t)
			list, err := sd.List(contextFixture(), tc.input)
			require.Equal(tc.err, err)
			require.Equal(tc.list, list)
		})
	}
}

func TestStorageDriverMove(t *testing.T) {
	require := require.New(t)

	td, cleanup := newTestDriver()
	defer cleanup()

	sd, testImage := td.setup()

	d, err := core.NewDigester().FromBytes([]byte(uploadContent))
	require.NoError(err)

	require.NoError(sd.Move(context.TODO(), genUploadDataPath(testImage.upload), genBlobDataPath(d.Hex())))

	reader, err := td.cas.GetCacheFileReader(d.Hex())
	require.NoError(err)
	data, err := ioutil.ReadAll(reader)
	require.NoError(err)
	require.Equal(uploadContent, string(data))
}
