package tagserver

import (
	"net/http"
	"net/url"

	"code.uber.internal/infra/kraken/core"
	"code.uber.internal/infra/kraken/utils/handler"
	"github.com/pressly/chi"
)

func parseTag(r *http.Request) (string, error) {
	tag, err := url.PathUnescape(chi.URLParam(r, "tag"))
	if err != nil {
		return "", handler.Errorf("path unescape tag: %s", err).Status(http.StatusBadRequest)
	}
	return tag, nil
}

func parseDigest(r *http.Request) (string, error) {
	d := chi.URLParam(r, "digest")
	if err := core.CheckSHA256Digest(d); err != nil {
		return "", handler.Errorf("invalid sha256 digest: %s", err).Status(http.StatusBadRequest)
	}
	return d, nil
}
