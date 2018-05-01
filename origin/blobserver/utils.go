package blobserver

import (
	"fmt"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"

	"code.uber.internal/infra/kraken/core"
	"code.uber.internal/infra/kraken/lib/store"
	"code.uber.internal/infra/kraken/origin/blobclient"
	"code.uber.internal/infra/kraken/utils/handler"
	"github.com/pressly/chi"
)

// parseNamespace parses a namespace from a url path parameter,
// e.g. "/namespace/:namespace".
func parseNamespace(r *http.Request) (string, error) {
	ns, err := url.PathUnescape(chi.URLParam(r, "namespace"))
	if err != nil {
		return "", handler.Errorf("path unescape namespace: %s", err).Status(http.StatusBadRequest)
	}
	return ns, nil
}

// parseDigest parses a digest from a url path parameter, e.g. "/blobs/:digest".
func parseDigest(r *http.Request) (d core.Digest, err error) {
	raw, err := url.PathUnescape(chi.URLParam(r, "digest"))
	if err != nil {
		return d, handler.Errorf("path unescape digest: %s", err).Status(http.StatusBadRequest)
	}
	d, err = core.NewDigestFromString(raw)
	if err != nil {
		return d, handler.Errorf("parse digest: %s", err).Status(http.StatusBadRequest)
	}
	return d, nil
}

// parseContentRange parses start / end integers from a Content-Range header.
func parseContentRange(h http.Header) (start, end int64, err error) {
	contentRange := h.Get("Content-Range")
	if len(contentRange) == 0 {
		return 0, 0, handler.Errorf("no Content-Range header").Status(http.StatusBadRequest)
	}
	parts := strings.Split(contentRange, "-")
	if len(parts) != 2 {
		return 0, 0, handler.Errorf(
			"cannot parse Content-Range header %q: expected format \"start-end\"", contentRange).
			Status(http.StatusBadRequest)
	}
	start, err = strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return 0, 0, handler.Errorf(
			"cannot parse start of range in Content-Range header %q: %s", contentRange, err).
			Status(http.StatusBadRequest)
	}
	end, err = strconv.ParseInt(parts[1], 10, 64)
	if err != nil {
		return 0, 0, handler.Errorf(
			"cannot parse end of range in Content-Range header %q: %s", contentRange, err).
			Status(http.StatusBadRequest)
	}
	// Note, no need to check for negative because the "-" would cause the
	// Split check to fail.
	return start, end, nil
}

// blobExists returns true if fs has a cached blob for d.
func blobExists(fs store.OriginFileStore, d core.Digest) (bool, error) {
	if _, err := fs.GetCacheFileStat(d.Hex()); err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, handler.Errorf("cache file stat: %s", err)
	}
	return true, nil
}

// transferBlob transfer blob d from fs to client.
func transferBlob(fs store.OriginFileStore, d core.Digest, client blobclient.Client) error {
	f, err := fs.GetCacheFileReader(d.Hex())
	if err != nil {
		return fmt.Errorf("get cache reader: %s", err)
	}
	if err := client.TransferBlob(d, f); err != nil {
		return fmt.Errorf("push blob: %s", err)
	}
	return nil
}

func setUploadLocation(w http.ResponseWriter, uid string) {
	w.Header().Set("Location", uid)
}

func setContentLength(w http.ResponseWriter, n int) {
	w.Header().Set("Content-Length", strconv.Itoa(n))
}

func setOctetStreamContentType(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/octet-stream-v1")
}
