package dockerregistry

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestStorageDriverGetContent(t *testing.T) {
	sd, testImage, cleanup := genStorageDriver()
	defer cleanup()

	uploadTime, err := sd.store.GetUploadFileStartedAt(testImage.upload)
	if err != nil {
		log.Panic(err)
	}

	testCases := []struct {
		input string
		data  []byte
		err   error
	}{
		{genLayerLinkPath(testImage.layers[0]), []byte("sha256:" + testImage.layers[0]), nil},
		{genUploadStartedAtPath(testImage.upload), uploadTime, nil},
		{genUploadHashStatesPath(testImage.upload), []byte(hashStateContent), nil},
		{genManifestTagCurrentLinkPath(testImage.repo, testImage.tag, testImage.manifest), []byte("sha256:" + testImage.manifest), nil},
		{genManifestRevisionLinkPath(testImage.repo, testImage.manifest), []byte("sha256:" + testImage.manifest), nil},
		{genBlobDataPath(testImage.layers[0]), []byte(layerContent), nil},
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("GetContent %s", tc.input), func(t *testing.T) {
			require := require.New(t)
			data, err := sd.GetContent(context.Background(), tc.input)
			require.Equal(tc.data, data)
			if tc.err == nil {
				require.NoError(err)
				return
			}
			require.Equal(tc.err, err)
		})
	}
}

func TestStorageDriverReader(t *testing.T) {
	require := require.New(t)
	sd, testImage, cleanup := genStorageDriver()
	defer cleanup()

	testCases := []struct {
		input string
		data  []byte
		err   error
	}{
		{genUploadDataPath(testImage.upload), []byte(uploadContent), nil},
		{genBlobDataPath(testImage.layers[0]), []byte(layerContent), nil},
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("GetReader %s", tc.input), func(t *testing.T) {
			reader, err := sd.Reader(context.Background(), tc.input, 0)
			data, err := ioutil.ReadAll(reader)
			require.Equal(tc.data, data)
			require.Equal(tc.err, err)
		})
	}
}

func TestStorageDriverPutContent(t *testing.T) {
	require := require.New(t)
	sd, testImage, cleanup := genStorageDriver()
	defer cleanup()

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
		{genLayerLinkPath(testImage.layers[0]), nil, nil},
		{genBlobDataPath(testImage.layers[0]), []byte("test putcontent"), nil},
		{genManifestRevisionLinkPath(repo, testImage.manifest), nil, nil},
		{genManifestTagShaLinkPath(repo, tag, testImage.manifest), nil, nil},
		{genManifestTagCurrentLinkPath(repo, tag, testImage.manifest), nil, nil},
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("PutContent %s", tc.inputPath), func(t *testing.T) {
			require.Equal(tc.err, sd.PutContent(context.Background(), tc.inputPath, tc.inputContent))
		})
	}

	// TODO (@evelynl): check content written
}

func TestStorageDriverWriter(t *testing.T) {
	require := require.New(t)
	sd, testImage, cleanup := genStorageDriver()
	defer cleanup()

	testCases := []struct {
		input string
		data  []byte
		err   error
	}{
		{genUploadDataPath(testImage.upload), []byte(uploadContent), nil},
		{genBlobDataPath(testImage.layers[0]), nil, InvalidRequestError{genBlobDataPath(testImage.layers[0])}},
	}

	content := []byte("this is a test for upload writer")
	for _, tc := range testCases {
		t.Run(fmt.Sprintf("GetWriter %s", tc.input), func(t *testing.T) {
			w, err := sd.Writer(context.Background(), tc.input, false)
			require.Equal(tc.err, err)
			if err != nil {
				return
			}
			w.Write(content)
			w.Close()
			r, err := sd.Reader(context.Background(), tc.input, 0)
			require.NoError(err)
			defer r.Close()
			data, err := ioutil.ReadAll(r)
			require.NoError(err)
			require.Equal(content, data)
		})
	}
}

func TestStorageDriverStat(t *testing.T) {
	require := require.New(t)
	sd, testImage, cleanup := genStorageDriver()
	defer cleanup()

	testCases := []struct {
		input string
		path  string
		size  int64
		err   error
	}{
		{genBlobDataPath(testImage.layers[0]), testImage.layers[0], int64(len(layerContent)), nil},
		{genUploadDataPath(testImage.upload), testImage.upload, int64(len(uploadContent)), nil},
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("GetStat %s", tc.input), func(t *testing.T) {
			fi, err := sd.Stat(context.Background(), tc.input)
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
	require := require.New(t)
	sd, testImage, cleanup := genStorageDriver()
	defer cleanup()

	testCases := []struct {
		input string
		list  []string
		err   error
	}{
		{genUploadHashStatesPath(testImage.upload), []string{genUploadHashStatesPath(testImage.upload)}, nil},
		{genManifestTagCurrentLinkPath(testImage.repo, testImage.tag, testImage.manifest), []string{testImage.tag}, nil},
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("List %s", tc.input), func(t *testing.T) {
			list, err := sd.List(context.Background(), tc.input)
			require.Equal(tc.err, err)
			require.Equal(tc.list, list)
		})
	}
}

func TestStorageDriverMove(t *testing.T) {
	require := require.New(t)
	sd, testImage, cleanup := genStorageDriver()
	defer cleanup()

	hasher := sha256.New()
	hasher.Write([]byte(time.Now().String()))
	sha := hex.EncodeToString(hasher.Sum(nil))

	testCases := []struct {
		fromPath string
		toPath   string
		err      error
	}{
		{genUploadDataPath(testImage.upload), genBlobDataPath(sha), nil},
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("Move %s to %s", tc.fromPath, tc.toPath), func(t *testing.T) {
			require.Equal(tc.err, sd.Move(context.Background(), tc.fromPath, tc.toPath))
		})
	}

	reader, err := sd.store.GetCacheFileReader(sha)
	require.NoError(err)
	data, err := ioutil.ReadAll(reader)
	require.NoError(err)
	require.Equal(uploadContent, string(data))
}
