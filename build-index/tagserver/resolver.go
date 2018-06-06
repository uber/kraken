package tagserver

import (
	"bytes"
	"fmt"
	"net/http"

	"code.uber.internal/infra/kraken/build-index/tagclient"
	"code.uber.internal/infra/kraken/core"
	"code.uber.internal/infra/kraken/lib/backend"
	"code.uber.internal/infra/kraken/lib/backend/backenderrors"
	"code.uber.internal/infra/kraken/lib/persistedretry/tagreplication"
	"code.uber.internal/infra/kraken/utils/dedup"
	"code.uber.internal/infra/kraken/utils/errutil"
	"code.uber.internal/infra/kraken/utils/handler"
	"code.uber.internal/infra/kraken/utils/log"
)

var _ dedup.Resolver = (*tagResolver)(nil)

type tagContext struct {
	local bool
}

type tagResolver struct {
	backends *backend.Manager

	// For falling back to other remotes if backends are not reachable.
	remotes  tagreplication.Remotes
	provider tagclient.Provider
}

// Resolve downloads tag from backends. If backends returns any error
// it falls back to remote build indexes.
func (r *tagResolver) Resolve(ctx interface{}, key interface{}) (interface{}, error) {
	tc := ctx.(tagContext)
	tag := key.(string)

	d, err := r.resolveFromBackends(tag)
	if err != nil && !tc.local {
		log.With("tag", tag).Warnf(
			"Failed to resolve tag from backends: %s. Resolve from remotes.", err)
		d, err = r.resolveFromRemotes(tag)
		if err != nil {
			return core.Digest{}, err
		}
	}

	return d, err
}

func (r *tagResolver) resolveFromRemotes(tag string) (core.Digest, error) {
	candidates := r.remotes.Match(tag)

	var errs []error
	notFound := true
	for _, remote := range candidates {
		client := r.provider.Provide(remote)
		d, err := client.GetLocal(tag)
		if err == nil {
			return d, nil
		} else if err != tagclient.ErrNotFound {
			notFound = false
			errs = append(errs, fmt.Errorf("remote client: %s", err))
		}
	}

	if notFound {
		return core.Digest{}, handler.ErrorStatus(http.StatusNotFound)
	}
	return core.Digest{}, handler.Errorf("get tag from remotes: %s", errutil.Join(errs))
}

func (r *tagResolver) resolveFromBackends(tag string) (core.Digest, error) {
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
