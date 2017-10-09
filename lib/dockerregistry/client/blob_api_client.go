package client

import (
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"

	"code.uber.internal/infra/kraken/lib/dockerregistry/image"
	"code.uber.internal/infra/kraken/lib/store"
	"code.uber.internal/infra/kraken/utils"
	"code.uber.internal/infra/kraken/utils/httputil"

	"github.com/docker/distribution/uuid"
)

// BlobAPIClient pulls and pushes file blobs using origin blob API.
// It doesn't know about sharding strategy and cannot handle redirect.
type BlobAPIClient struct {
	config    *Config
	blobStore store.FileStore
}

// NewBlobAPIClient initiate and returns a new BlobAPIClient object.
func NewBlobAPIClient(config *Config, blobStore store.FileStore) *BlobAPIClient {
	return &BlobAPIClient{
		config:    config,
		blobStore: blobStore,
	}
}

// GetManifest queries tracker for manifest
func (cli *BlobAPIClient) GetManifest(repo, tag string) (string, error) {
	name := fmt.Sprintf("%s:%s", repo, tag)
	url := "http://" + cli.config.TrackerAddr + "/manifest/" + url.QueryEscape(name)

	resp, err := httputil.Send("GET", url, httputil.SendTimeout(cli.config.RequestTimeout))
	if err != nil {
		return "", fmt.Errorf("failed to send request to %s: %s", url, err)
	}
	defer resp.Body.Close()

	data, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("failed to read response to %s: %s", url, err)
	}

	_, manifestDigest, err := utils.ParseManifestV2(data)
	if err != nil {
		return "", fmt.Errorf("failed to parse manifest for %s: %s", name, err)
	}

	// Store manifest
	manifestDigestTemp := manifestDigest + "." + uuid.Generate().String()
	if err = cli.blobStore.CreateUploadFile(manifestDigestTemp, 0); err != nil {
		return "", fmt.Errorf("failed to create upload file %s: %s", manifestDigest, err)
	}

	writer, err := cli.blobStore.GetUploadFileReadWriter(manifestDigestTemp)
	if err != nil {
		return "", fmt.Errorf("failed to get writer for upload file %s: %s", manifestDigest, err)
	}

	_, err = writer.Write(data)
	if err != nil {
		return "", fmt.Errorf("failed to write %s: %s", manifestDigest, err)
	}
	writer.Close()

	err = cli.blobStore.MoveUploadFileToCache(manifestDigestTemp, manifestDigest)
	// It is ok if move fails on file exist error
	if err != nil && !os.IsExist(err) {
		return "", fmt.Errorf("failed to move upload file %s to cache: %s", manifestDigest, err)
	}

	return manifestDigest, nil
}

// PostManifest saves manifest specified by the tag it referred in a tracker
func (cli *BlobAPIClient) PostManifest(repo, tag, manifest string) error {
	reader, err := cli.blobStore.GetCacheFileReader(manifest)
	if err != nil {
		return fmt.Errorf("failed to get reader for %s: %s", manifest, err)
	}

	name := fmt.Sprintf("%s:%s", repo, tag)
	url := "http://" + cli.config.TrackerAddr + "/manifest/" + url.QueryEscape(name)
	resp, err := httputil.Send("POST", url, httputil.SendBody(reader), httputil.SendTimeout(cli.config.RequestTimeout))
	if err != nil {
		return fmt.Errorf("failed to send post request to %s: %s", url, err)
	}
	defer resp.Body.Close()

	return nil
}

// PullBlob pulls a file blob from origin server.
func (cli *BlobAPIClient) PullBlob(digest image.Digest) error {
	url := fmt.Sprintf("http://%s/blobs/%s", cli.config.OriginAddr, digest.Hex())
	resp, err := httputil.Send("GET", url, httputil.SendTimeout(cli.config.RequestTimeout))
	if err != nil {
		return fmt.Errorf("failed to send get request to %s: %s", url, err)
	}
	defer resp.Body.Close()

	// Store layer with a tmp name and then move to cache
	// This allows multiple threads to pull the same blob
	tmp := fmt.Sprintf("%s.%s", digest.Hex(), uuid.Generate().String())
	if err := cli.blobStore.CreateUploadFile(tmp, 0); err != nil {
		return fmt.Errorf("failed to create upload file: %s", err)
	}
	w, err := cli.blobStore.GetUploadFileReadWriter(tmp)
	if err != nil {
		return fmt.Errorf("failed to get writer: %s", err)
	}
	defer w.Close()

	// Stream to file and verify content at the same time
	r := io.TeeReader(resp.Body, w)

	verified, err := cli.verifyBlob(digest, r)
	if err != nil {
		return fmt.Errorf("failed to verify data: %s", err)
	}
	if !verified {
		// TODO: Delete tmp file on error
		return fmt.Errorf("failed to verify data: digests do not match")
	}

	if err := cli.blobStore.MoveUploadFileToCache(tmp, digest.Hex()); err != nil {
		if !os.IsExist(err) {
			return fmt.Errorf("failed to move upload file to cache: %s", err)
		}
		// Ignore if another thread is pulling the same blob because it is normal
	}

	return nil
}

// PushBlob pushes a file blob to remote server.
func (cli *BlobAPIClient) PushBlob(digest image.Digest) error {
	exist, err := cli.CheckBlobExists(digest)
	if err != nil {
		return fmt.Errorf("failed to check blob exists: %s", err)
	}
	if exist {
		return nil
	}

	location, err := cli.pushStart(digest)
	if err != nil {
		return fmt.Errorf("failed to start push: %s", err)
	}

	if err := cli.pushBlobContent(location, digest); err != nil {
		return fmt.Errorf("failed to push content: %s", err)
	}

	return nil
}

// CheckBlobExists verifies if a blob exists on remote.
func (cli *BlobAPIClient) CheckBlobExists(digest image.Digest) (bool, error) {
	url := fmt.Sprintf("http://%s/blobs/%s", cli.config.OriginAddr, digest.Hex())
	resp, err := httputil.Send(
		"HEAD",
		url,
		httputil.SendTimeout(cli.config.RequestTimeout),
		httputil.SendAcceptedCodes(http.StatusOK, http.StatusNotFound))
	if err != nil {
		return false, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusOK {
		return true, nil
	}

	return false, nil
}

func (cli *BlobAPIClient) verifyBlob(digest image.Digest, reader io.Reader) (bool, error) {
	digester := image.NewDigester()
	computedDigest, err := digester.FromReader(reader)
	if err != nil {
		return false, err
	}

	return computedDigest == digest, nil
}

// pushStart starts a blob upload. It returns a UUID to identify the blob.
func (cli *BlobAPIClient) pushStart(digest image.Digest) (string, error) {
	url := fmt.Sprintf("http://%s/blobs/uploads?digest=%s", cli.config.OriginAddr, digest)
	headers := make(map[string]string)
	headers["Content-Length"] = "0"

	resp, err := httputil.Send(
		"POST",
		url,
		httputil.SendTimeout(cli.config.RequestTimeout),
		httputil.SendAcceptedCodes(http.StatusAccepted),
		httputil.SendHeaders(headers))
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	location := resp.Header.Get("Location")
	if location == "" {
		return "", errors.New("empty upload URL")
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
	c := blobChunk{size, cli.config.ChunkSize}
	n, err := c.numChunks()
	if err != nil {
		return err
	}

	for i := 0; i < n; i++ {
		start, end, err := c.getChunkStartEnd(i)
		if err != nil {
			return err
		}
		// Update location to use in the next request
		location, err = cli.pushBlobChunk(location, digest, reader, start, end)
	}

	// End layer push
	url := fmt.Sprintf("http://%s/%s?digest=%s", cli.config.OriginAddr, location, digest)
	headers := make(map[string]string)
	headers["Content-Length"] = "0"

	resp, err := httputil.Send(
		"PUT",
		url,
		httputil.SendTimeout(cli.config.RequestTimeout),
		httputil.SendAcceptedCodes(http.StatusCreated),
		httputil.SendHeaders(headers))
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	return nil
}

func (cli *BlobAPIClient) pushBlobChunk(location string, digest image.Digest, reader io.Reader, start, end int64) (string, error) {
	url := fmt.Sprintf("http://%s/%s?digest=%s", cli.config.OriginAddr, location, digest.Hex())
	chunckSize := end - start
	headers := make(map[string]string)
	headers["Content-Length"] = fmt.Sprintf("%d", chunckSize)
	headers["Content-Range"] = fmt.Sprintf("%d-%d", start, end-1)

	resp, err := httputil.Send(
		"PATCH",
		url,
		httputil.SendBody(reader),
		httputil.SendTimeout(cli.config.RequestTimeout),
		httputil.SendAcceptedCodes(http.StatusAccepted),
		httputil.SendHeaders(headers))
	if err != nil {
		return "", err
	}

	// New upload location for the next upload
	newLoc := resp.Header.Get("Location")
	if newLoc == "" {
		return "", errors.New("upload location is empty")
	}

	return newLoc, nil
}
