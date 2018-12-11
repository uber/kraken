package tagtype

import "github.com/uber/kraken/core"

type defaultResolver struct{}

// Resolve always returns d as the sole dependency of tag.
func (r *defaultResolver) Resolve(tag string, d core.Digest) (core.DigestList, error) {
	return core.DigestList{d}, nil
}
