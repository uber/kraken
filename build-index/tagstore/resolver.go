package tagstore

import (
	"bytes"
	"fmt"
	"io"

	"code.uber.internal/infra/kraken/build-index/tagclient"
	"code.uber.internal/infra/kraken/core"
	"code.uber.internal/infra/kraken/lib/backend"
	"code.uber.internal/infra/kraken/lib/backend/backenderrors"
	"code.uber.internal/infra/kraken/lib/persistedretry/tagreplication"
	"code.uber.internal/infra/kraken/utils/errutil"
	"code.uber.internal/infra/kraken/utils/log"
)

type resolveContext struct {
	fallback bool
}

type tagResolver struct {
	fs       FileStore
	backends *backend.Manager

	// For falling back to other remotes if backends are not reachable.
	remotes           tagreplication.Remotes
	tagClientProvider tagclient.Provider
}

// source defines a place where we can resolve tags from.
type source struct {
	name    string
	resolve func(tag string) (core.Digest, error)
}

// Resolve downloads tag from backends. If backends returns any error
// it falls back to remote build indexes.
func (r *tagResolver) Resolve(ctx, key interface{}) (interface{}, error) {
	rctx := ctx.(resolveContext)
	tag := key.(string)

	sources := []source{
		{"disk", r.resolveFromDisk},
		{"backend", r.resolveFromBackends},
	}
	// We must be able to disable fallback to prevent cycles when resolving
	// from remotes.
	if rctx.fallback {
		// XXX: Falling back to a remote build-index means that we will be
		// caching tags that may not exist in our backend storage.
		sources = append(sources, source{"remote", r.resolveFromRemotes})
	}

	var d core.Digest
	var err error
	for i, src := range sources {
		d, err = src.resolve(tag)
		if err != nil {
			next := i + 1
			if next < len(sources) {
				log.With("tag", tag).Warnf(
					"Failed to resolve tag from %s: %s. Attempting to resolve from %s next.",
					src.name, err, sources[next].name)
			}
			continue
		}
		break
	}

	return d, err
}

func (r *tagResolver) resolveFromDisk(tag string) (core.Digest, error) {
	f, err := r.fs.GetCacheFileReader(tag)
	if err != nil {
		return core.Digest{}, fmt.Errorf("store: %s", err)
	}
	defer f.Close()
	var b bytes.Buffer
	if _, err := io.Copy(&b, f); err != nil {
		return core.Digest{}, fmt.Errorf("copy: %s", err)
	}
	d, err := core.ParseSHA256Digest(b.String())
	if err != nil {
		return core.Digest{}, fmt.Errorf("parse digest: %s", err)
	}
	return d, nil
}

func (r *tagResolver) resolveFromBackends(tag string) (core.Digest, error) {
	backendClient, err := r.backends.GetClient(tag)
	if err != nil {
		return core.Digest{}, fmt.Errorf("backend manager: %s", err)
	}
	var b bytes.Buffer
	if err := backendClient.Download(tag, &b); err != nil {
		if err == backenderrors.ErrBlobNotFound {
			return core.Digest{}, ErrTagNotFound
		}
		return core.Digest{}, fmt.Errorf("backend client: %s", err)
	}
	d, err := core.ParseSHA256Digest(b.String())
	if err != nil {
		return core.Digest{}, fmt.Errorf("parse digest: %s", err)
	}
	if err := writeTagToDisk(tag, d, r.fs); err != nil {
		log.With("tag", tag).Errorf("Error writing tag to disk: %s", err)
	}
	return d, nil
}

func (r *tagResolver) resolveFromRemotes(tag string) (core.Digest, error) {
	var errs []error
	notFound := true
	candidates := r.remotes.Match(tag)
	for _, remote := range candidates {
		tagClient := r.tagClientProvider.Provide(remote)
		d, err := tagClient.GetLocal(tag)
		if err == nil {
			if err := writeTagToDisk(tag, d, r.fs); err != nil {
				log.With("tag", tag).Errorf("Error writing tag to disk: %s", err)
			}
			return d, nil
		} else if err != tagclient.ErrTagNotFound {
			notFound = false
			errs = append(errs, fmt.Errorf("remote client: %s", err))
		}
	}
	if notFound {
		return core.Digest{}, ErrTagNotFound
	}
	return core.Digest{}, fmt.Errorf("get tag from remotes: %s", errutil.Join(errs))
}
