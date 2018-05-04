package tagserver

import (
	"bytes"
	"net/http"

	"code.uber.internal/infra/kraken/lib/backend"
	"code.uber.internal/infra/kraken/lib/backend/backenderrors"
	"code.uber.internal/infra/kraken/utils/handler"
	"code.uber.internal/infra/kraken/utils/log"
)

type tagResolver struct {
	backends *backend.Manager
}

func (r *tagResolver) Resolve(key interface{}) (interface{}, error) {
	name := key.(string)
	log.With("tag", name).Info("Resolving tag")
	client, err := r.backends.GetClient(name)
	if err != nil {
		return "", handler.Errorf("backend manager: %s", err)
	}
	var b bytes.Buffer
	if err := client.Download(name, &b); err != nil {
		if err == backenderrors.ErrBlobNotFound {
			return "", handler.ErrorStatus(http.StatusNotFound)
		}
		return "", handler.Errorf("backend client: %s", err)
	}
	return b.String(), nil
}
