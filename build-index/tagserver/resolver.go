package tagserver

import (
	"bytes"
	"net/http"

	"code.uber.internal/infra/kraken/core"
	"code.uber.internal/infra/kraken/lib/backend"
	"code.uber.internal/infra/kraken/lib/backend/backenderrors"
	"code.uber.internal/infra/kraken/utils/handler"
	"code.uber.internal/infra/kraken/utils/log"
)

type tagResolver struct {
	backends *backend.Manager
}

func (r *tagResolver) Resolve(key interface{}) (interface{}, error) {
	tag := key.(string)
	log.With("tag", tag).Info("Resolving tag")
	client, err := r.backends.GetClient(tag)
	if err != nil {
		return core.Digest{}, handler.Errorf("backend manager: %s", err)
	}
	var b bytes.Buffer
	if err := client.Download(tag, &b); err != nil {
		if err == backenderrors.ErrBlobNotFound {
			return core.Digest{}, handler.ErrorStatus(http.StatusNotFound)
		}
		return core.Digest{}, handler.Errorf("backend client: %s", err)
	}
	d, err := core.ParseSHA256Digest(b.String())
	if err != nil {
		return core.Digest{}, handler.Errorf("parse digest: %s", err)
	}
	return d, nil
}
