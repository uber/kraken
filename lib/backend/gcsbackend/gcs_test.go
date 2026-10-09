// Copyright (c) 2016-2019 Uber Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package gcsbackend

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"cloud.google.com/go/storage"
	"github.com/stretchr/testify/require"
	"github.com/uber/kraken/lib/backend/backenderrors"
	"github.com/uber/kraken/utils/closers"
	"github.com/uber/kraken/utils/randutil"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

const _testBucket = "test-bucket"

// fakeGCS is an in-memory implementation of the subset of the GCS API which
// GCSImpl exercises: object metadata, media download (including ranged reads
// issued by the parallel downloader), multipart upload and object listing.
type fakeGCS struct {
	t      *testing.T
	server *httptest.Server

	mu      sync.Mutex
	objects map[string][]byte
	// sessions maps a resumable upload id onto the object name it writes.
	sessions map[string]string

	// delay is slept before serving a media download.
	delay time.Duration
}

func newFakeGCS(t *testing.T) *fakeGCS {
	f := &fakeGCS{
		t:        t,
		objects:  make(map[string][]byte),
		sessions: make(map[string]string),
	}
	f.server = httptest.NewServer(http.HandlerFunc(f.handle))
	t.Cleanup(f.server.Close)
	return f
}

func (f *fakeGCS) put(name string, b []byte) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.objects[name] = b
}

func (f *fakeGCS) get(name string) ([]byte, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	b, ok := f.objects[name]
	return b, ok
}

func (f *fakeGCS) names() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	var names []string
	for name := range f.objects {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// client returns a GCSImpl talking to the fake server.
func (f *fakeGCS) client(config Config) *GCSImpl {
	config.Bucket = _testBucket
	config.applyDefaults()

	sClient, err := storage.NewClient(
		context.Background(),
		option.WithEndpoint(f.server.URL),
		option.WithoutAuthentication())
	require.NoError(f.t, err)

	g, err := NewGCS(context.Background(), sClient, &config)
	require.NoError(f.t, err)
	f.t.Cleanup(func() { closers.Close(g) })
	return g
}

func (f *fakeGCS) handle(w http.ResponseWriter, r *http.Request) {
	switch {
	case strings.HasPrefix(r.URL.Path, "/upload/storage/v1/b/"+_testBucket+"/o"):
		f.handleUpload(w, r)
	case r.URL.Path == "/b/"+_testBucket+"/o":
		f.handleList(w, r)
	case strings.HasPrefix(r.URL.Path, "/b/"+_testBucket+"/o/"):
		f.handleAttrs(w, r)
	case strings.HasPrefix(r.URL.Path, "/"+_testBucket+"/"):
		f.handleDownload(w, r)
	default:
		http.Error(w, "unexpected request: "+r.URL.Path, http.StatusNotImplemented)
	}
}

func (f *fakeGCS) handleAttrs(w http.ResponseWriter, r *http.Request) {
	name := strings.TrimPrefix(r.URL.Path, "/b/"+_testBucket+"/o/")
	b, ok := f.get(name)
	if !ok {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	f.writeObjectJSON(w, name, len(b))
}

func (f *fakeGCS) handleDownload(w http.ResponseWriter, r *http.Request) {
	if f.delay > 0 {
		time.Sleep(f.delay)
	}
	name := strings.TrimPrefix(r.URL.Path, "/"+_testBucket+"/")
	b, ok := f.get(name)
	if !ok {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	// ServeContent handles the Range headers the parallel downloader sends.
	http.ServeContent(w, r, name, time.Time{}, bytes.NewReader(b))
}

// handleUpload serves both the multipart upload used for small objects and
// the resumable, chunked upload used once ChunkSize is exceeded.
func (f *fakeGCS) handleUpload(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query()
	if id := query.Get("upload_id"); id != "" {
		f.handleResumableChunk(w, r, id)
		return
	}
	if query.Get("uploadType") == "resumable" {
		f.startResumableUpload(w, r)
		return
	}
	f.handleMultipartUpload(w, r)
}

func (f *fakeGCS) startResumableUpload(w http.ResponseWriter, r *http.Request) {
	name := r.URL.Query().Get("name")

	f.mu.Lock()
	id := strconv.Itoa(len(f.sessions) + 1)
	f.sessions[id] = name
	f.objects[name] = nil
	f.mu.Unlock()

	w.Header().Set("Location", fmt.Sprintf(
		"%s/upload/storage/v1/b/%s/o?upload_id=%s", f.server.URL, _testBucket, id))
	f.writeObjectJSON(w, name, 0)
}

func (f *fakeGCS) handleResumableChunk(w http.ResponseWriter, r *http.Request, id string) {
	f.mu.Lock()
	name, ok := f.sessions[id]
	f.mu.Unlock()
	if !ok {
		http.Error(w, "unknown upload id", http.StatusNotFound)
		return
	}

	chunk, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	f.mu.Lock()
	f.objects[name] = append(f.objects[name], chunk...)
	size := len(f.objects[name])
	f.mu.Unlock()

	// A "*" total signals more chunks are coming. Clients set
	// X-GUploader-No-308, so "resume incomplete" is signalled with a 200 and
	// an override header rather than a real 308.
	contentRange := r.Header.Get("Content-Range")
	if strings.HasSuffix(contentRange, "/*") {
		w.Header().Set("X-Http-Status-Code-Override", "308")
		w.Header().Set("Range", fmt.Sprintf("bytes=0-%d", size-1))
		w.WriteHeader(http.StatusOK)
		return
	}
	f.writeObjectJSON(w, name, size)
}

func (f *fakeGCS) handleMultipartUpload(w http.ResponseWriter, r *http.Request) {
	name := r.URL.Query().Get("name")
	_, params, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	mr := multipart.NewReader(r.Body, params["boundary"])
	var media []byte
	for {
		part, err := mr.NextPart()
		if err == io.EOF {
			break
		}
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		// The first part is the object metadata, the second is the content.
		media, err = io.ReadAll(part)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
	}
	f.put(name, media)
	f.writeObjectJSON(w, name, len(media))
}

func (f *fakeGCS) handleList(w http.ResponseWriter, r *http.Request) {
	prefix := r.URL.Query().Get("prefix")
	start := r.URL.Query().Get("pageToken")
	maxResults, err := strconv.Atoi(r.URL.Query().Get("maxResults"))
	if err != nil {
		http.Error(w, "invalid maxResults", http.StatusBadRequest)
		return
	}

	var matches []string
	for _, name := range f.names() {
		if strings.HasPrefix(name, prefix) && name >= start {
			matches = append(matches, name)
		}
	}
	nextPageToken := ""
	if maxResults > 0 && len(matches) > maxResults {
		nextPageToken = matches[maxResults]
		matches = matches[:maxResults]
	}

	items := make([]map[string]interface{}, len(matches))
	for i, name := range matches {
		b, _ := f.get(name)
		items[i] = f.objectJSON(name, len(b))
	}
	f.writeJSON(w, map[string]interface{}{
		"kind":          "storage#objects",
		"items":         items,
		"nextPageToken": nextPageToken,
	})
}

func (f *fakeGCS) objectJSON(name string, size int) map[string]interface{} {
	return map[string]interface{}{
		"kind":         "storage#object",
		"bucket":       _testBucket,
		"name":         name,
		"size":         strconv.Itoa(size),
		"storageClass": "STANDARD",
		"updated":      "2024-06-01T00:00:00Z",
	}
}

func (f *fakeGCS) writeObjectJSON(w http.ResponseWriter, name string, size int) {
	f.writeJSON(w, f.objectJSON(name, size))
}

func (f *fakeGCS) writeJSON(w http.ResponseWriter, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	require.NoError(f.t, json.NewEncoder(w).Encode(v))
}

// writerAt collects writes at arbitrary offsets, as the parallel downloader
// performs them out of order.
type writerAt struct {
	mu sync.Mutex
	b  []byte
}

func (w *writerAt) WriteAt(p []byte, off int64) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if end := int(off) + len(p); end > len(w.b) {
		w.b = append(w.b, make([]byte, end-len(w.b))...)
	}
	return copy(w.b[off:], p), nil
}

func (w *writerAt) bytes() []byte {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.b
}

func TestGCSImplObjectAttrs(t *testing.T) {
	require := require.New(t)

	fake := newFakeGCS(t)
	fake.put("some/blob", randutil.Text(64))

	g := fake.client(Config{})

	attrs, err := g.ObjectAttrs("some/blob")
	require.NoError(err)
	require.Equal(_testBucket, attrs.Bucket)
	require.Equal("some/blob", attrs.Name)
	require.Equal(int64(64), attrs.Size)
}

func TestGCSImplObjectAttrsNotFound(t *testing.T) {
	require := require.New(t)

	fake := newFakeGCS(t)
	g := fake.client(Config{})

	_, err := g.ObjectAttrs("no/such/blob")
	require.ErrorIs(err, storage.ErrObjectNotExist)
	require.True(isObjectNotFound(err))
}

func TestGCSImplDownload(t *testing.T) {
	require := require.New(t)

	fake := newFakeGCS(t)
	data := randutil.Text(128)
	fake.put("some/blob", data)

	g := fake.client(Config{})

	var w writerAt
	n, err := g.Download("some/blob", &w)
	require.NoError(err)
	require.Equal(int64(len(data)), n)
	require.Equal(data, w.bytes())
}

// TestGCSImplDownloadMultipleParts covers the parallel, sharded download path:
// every part must land at its own offset and the whole object must be
// reassembled.
func TestGCSImplDownloadMultipleParts(t *testing.T) {
	require := require.New(t)

	fake := newFakeGCS(t)
	// 10 parts, plus a partial one.
	const partSize = 1024
	data := randutil.Text(10*partSize + 13)
	fake.put("some/blob", data)

	g := fake.client(Config{DownloadPartSize: partSize, DownloadConcurrency: 4})

	var w writerAt
	n, err := g.Download("some/blob", &w)
	require.NoError(err)
	require.Equal(int64(len(data)), n)
	require.Equal(data, w.bytes())
}

func TestGCSImplDownloadNotFound(t *testing.T) {
	require := require.New(t)

	fake := newFakeGCS(t)
	g := fake.client(Config{})

	var w writerAt
	_, err := g.Download("no/such/blob", &w)
	require.Equal(backenderrors.ErrBlobNotFound, err)
}

func TestGCSImplDownloadTimeout(t *testing.T) {
	require := require.New(t)

	fake := newFakeGCS(t)
	fake.put("some/blob", randutil.Text(64))
	fake.delay = 2 * time.Second

	g := fake.client(Config{DownloadTimeoutSeconds: 1})

	var w writerAt
	_, err := g.Download("some/blob", &w)
	require.Error(err)
	require.Contains(err.Error(), context.DeadlineExceeded.Error())
}

func TestGCSImplUpload(t *testing.T) {
	require := require.New(t)

	fake := newFakeGCS(t)
	g := fake.client(Config{})

	data := randutil.Text(128)
	n, err := g.Upload("some/blob", bytes.NewReader(data))
	require.NoError(err)
	require.Equal(int64(len(data)), n)

	uploaded, ok := fake.get("some/blob")
	require.True(ok)
	require.Equal(data, uploaded)
}

// TestGCSImplUploadLargerThanChunkSize guards against regressing to a copy
// which stops after a single chunk.
func TestGCSImplUploadLargerThanChunkSize(t *testing.T) {
	require := require.New(t)

	fake := newFakeGCS(t)
	const chunkSize = 256 * 1024 // Minimum chunk size accepted by GCS.
	g := fake.client(Config{UploadChunkSize: chunkSize})

	data := randutil.Text(chunkSize + 1024)
	n, err := g.Upload("some/blob", bytes.NewReader(data))
	require.NoError(err)
	require.Equal(int64(len(data)), n)

	uploaded, ok := fake.get("some/blob")
	require.True(ok)
	require.Equal(data, uploaded)
}

func TestGCSImplNextPage(t *testing.T) {
	require := require.New(t)

	fake := newFakeGCS(t)
	for i := 0; i < 5; i++ {
		fake.put(fmt.Sprintf("some/blob/%d", i), randutil.Text(8))
	}
	fake.put("other/blob", randutil.Text(8))

	g := fake.client(Config{})

	var names []string
	token := ""
	for {
		pager := iterator.NewPager(g.GetObjectIterator("some/blob"), 2, token)
		page, next, err := g.NextPage(pager)
		require.NoError(err)
		names = append(names, page...)
		if next == "" {
			break
		}
		token = next
	}
	require.Equal([]string{
		"some/blob/0",
		"some/blob/1",
		"some/blob/2",
		"some/blob/3",
		"some/blob/4",
	}, names)
}

func TestGCSImplClose(t *testing.T) {
	require := require.New(t)

	fake := newFakeGCS(t)
	data := randutil.Text(64)
	fake.put("some/blob", data)

	sClient, err := storage.NewClient(
		context.Background(),
		option.WithEndpoint(fake.server.URL),
		option.WithoutAuthentication())
	require.NoError(err)

	config := Config{Bucket: _testBucket}
	config.applyDefaults()
	g, err := NewGCS(context.Background(), sClient, &config)
	require.NoError(err)

	var w writerAt
	_, err = g.Download("some/blob", &w)
	require.NoError(err)

	require.NoError(g.Close())
}

func TestGCSImplCloseNilFields(t *testing.T) {
	require.NoError(t, (&GCSImpl{}).Close())
}
