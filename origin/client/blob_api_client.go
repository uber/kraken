package client

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/http/httputil"
	"time"

	"code.uber.internal/infra/kraken/lib/dockerregistry/image"
	"code.uber.internal/infra/kraken/lib/store"
)

const (
	timeoutInSeconds    = 300
	pushChunkSize       = 50000 * 1024 // 50 MB
	checkBlobURL        = "http://%s/blobs/%s"
	pullBlobURL         = "http://%s/blobs/%s"
	pushBlobStartURL    = "http://%s/blobs/uploads?digest=%s"
	pushBlobChunkURL    = "http://%s/%s?digest=%s"
	pushBlobCompleteURL = "http://%s/%s?digest=%s"
)

// BlobAPIClient pulls and pushes file blobs using origin blob API.
// It doesn't know about sharding strategy and cannot handle redirect.
type BlobAPIClient struct {
	address    string
	httpClient http.Client
	blobStore  store.FileStore
}

//BlobTransferFactory a blob api file creation pointer
type BlobTransferFactory func(string, store.FileStore) BlobTransferer

// NewBlobAPIClient initiate and returns a new BlobAPIClient object.
func NewBlobAPIClient(address string, blobStore store.FileStore) BlobTransferer {
	return &BlobAPIClient{
		address:    address,
		httpClient: http.Client{Timeout: timeoutInSeconds * time.Second},
		blobStore:  blobStore,
	}
}

// TODO: pull and verify concurrently
func (cli *BlobAPIClient) verifyBlob(digest image.Digest, reader io.ReadCloser) (bool, error) {
	defer reader.Close()

	digester := image.NewDigester()
	computedDigest, err := digester.FromReader(reader)
	if err != nil {
		return false, err
	}

	return *computedDigest == digest, nil
}

// PullBlob pulls a file blob from origin server.
func (cli *BlobAPIClient) PullBlob(digest image.Digest) error {
	url := fmt.Sprintf(pullBlobURL, cli.address, digest.Hex())
	resp, err := cli.httpClient.Get(url)
	if err != nil {
		return err
	}

	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		respDump, errDump := httputil.DumpResponse(resp, true)
		if errDump != nil {
			return errDump
		}
		return fmt.Errorf("%s", respDump)
	}

	// Store layer
	// TODO: handle concurrent download of the same file
	if err := cli.blobStore.CreateDownloadFile(digest.Hex(), 0); err != nil {
		return err
	}
	w, err := cli.blobStore.GetDownloadFileReadWriter(digest.Hex())
	if err != nil {
		return err
	}
	io.Copy(w, resp.Body)
	if err := w.Close(); err != nil {
		return err
	}

	// Verify
	verified, err := cli.verifyBlob(digest, w)
	if err != nil {
		return err
	}
	if !verified {
		return fmt.Errorf("Data cannot be verified")
	}

	return cli.blobStore.MoveDownloadFileToCache(digest.Hex())
}

// PushBlob pushes a file blob to remote server.
func (cli *BlobAPIClient) PushBlob(digest image.Digest) error {
	exist, err := cli.CheckBlobExists(digest)
	if err != nil {
		return err
	}
	if exist {
		return nil
	}

	// Push start
	location, err := cli.pushStart(digest)
	if err != nil {
		return err
	}

	return cli.pushBlobContent(location, digest)
}

// CheckBlobExists verifies if a blob exists on remote.
func (cli *BlobAPIClient) CheckBlobExists(digest image.Digest) (bool, error) {
	url := fmt.Sprintf(checkBlobURL, cli.address, digest.Hex())
	resp, err := cli.httpClient.Head(url)
	if err != nil {
		return false, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusOK {
		return true, nil
	}

	return false, nil
}

// pushStart starts a blob upload. It returns a UUID to identify the blob.
func (cli *BlobAPIClient) pushStart(digest image.Digest) (string, error) {
	url := fmt.Sprintf(pushBlobStartURL, cli.address, digest)
	req, err := http.NewRequest("POST", url, bytes.NewReader([]byte{}))
	req.ContentLength = 0

	resp, err := cli.httpClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusAccepted {
		respDump, errDump := ioutil.ReadAll(resp.Body)
		if errDump != nil {
			return "", errDump
		}
		return "", fmt.Errorf("Reponse: %s %s %s", url, resp.Status, respDump)
	}

	location := resp.Header.Get("Location")
	if location == "" {
		return "", fmt.Errorf("Empty upload URL")
	}

	return location, nil
}

// pushBlobContent upload the file blob content in chunks.
func (cli *BlobAPIClient) pushBlobContent(location string, digest image.Digest) error {
	reader, err := cli.blobStore.GetCacheFileReader(digest.Hex())
	if err != nil {
		return err
	}
	info, err := cli.blobStore.GetCacheFileStat(digest.Hex())
	if err != nil {
		return err
	}
	size := info.Size()

	var start int64
	var endIncluded int64
	var newLocation string
	start = 0
	endIncluded = start + pushChunkSize - 1
	if endIncluded > size-1 {
		endIncluded = size - 1
	}

	// Push layer chunks
	for start < size {
		newLocation, err = cli.pushBlobChunk(location, digest, reader, start, endIncluded)
		if err != nil {
			return err
		}

		start = endIncluded + 1
		endIncluded = start + pushChunkSize - 1
		if endIncluded > size-1 {
			endIncluded = size - 1
		}
		location = newLocation
	}

	// End layer push
	url := fmt.Sprintf(pushBlobCompleteURL, cli.address, newLocation, digest)
	req, err := http.NewRequest("PUT", url, bytes.NewReader([]byte{}))
	req.Header.Set("Content-Length", fmt.Sprintf("%d", 0))
	newResp, err := cli.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer newResp.Body.Close()

	if newResp.StatusCode != http.StatusCreated {
		respDump, errDump := ioutil.ReadAll(newResp.Body)
		if errDump != nil {
			err = errDump
		} else {
			err = fmt.Errorf("Unable to end uploading: %s %s %s", url, newResp.Status, respDump)
		}
		return err
	}

	return nil
}

func (cli *BlobAPIClient) pushBlobChunk(location string, digest image.Digest, reader io.Reader, start, endIncluded int64) (string, error) {
	url := fmt.Sprintf(pushBlobChunkURL, cli.address, location, digest.Hex())
	chunckSize := endIncluded + 1 - start
	req, err := http.NewRequest("PATCH", url, reader)
	req.Header.Set("Content-Length", fmt.Sprintf("%d", chunckSize))
	req.Header.Set("Content-Range", fmt.Sprintf("%d-%d", start, endIncluded))

	resp, err := cli.httpClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 202 {
		respDump, errDump := httputil.DumpResponse(resp, true)
		if errDump != nil {
			err = errDump
		} else {
			err = fmt.Errorf("Reponse: %s %s", url, respDump)
		}

		return "", err
	}

	// New upload location for the next upload
	newLoc := resp.Header.Get("Location")
	if newLoc == "" {
		return "", fmt.Errorf("Upload location is empty")
	}

	return newLoc, nil
}
