package tagtype

import (
	"errors"
	"fmt"
	"regexp"

	"code.uber.internal/infra/kraken/core"
	"code.uber.internal/infra/kraken/origin/blobclient"
)

var errNamespaceNotFound = errors.New("no matches for namespace")

// Config defines the namespace and the type of resolver associated with it.
type Config struct {
	Namespace string `yaml:"namespace"`
	Type      string `yaml:"type"`
}

// DependencyResolver returns a list of blob dependencies for a tag->digest mapping.
type DependencyResolver interface {
	Resolve(tag string, d core.Digest) (core.DigestList, error)
}

type subResolver struct {
	regexp   *regexp.Regexp
	resolver DependencyResolver
}

// Map is a DependencyResolver which maps tag patterns to sub resolvers.
type Map struct {
	subResolvers []*subResolver
}

// NewMap creates a new Map.
func NewMap(configs []Config, originClient blobclient.ClusterClient) (*Map, error) {
	if len(configs) == 0 {
		return nil, fmt.Errorf("no config specified")
	}
	var subResolvers []*subResolver
	for _, config := range configs {
		re, err := regexp.Compile(config.Namespace)
		if err != nil {
			return nil, fmt.Errorf("regexp: %s", err)
		}
		var sr *subResolver
		switch config.Type {
		case "docker":
			sr = &subResolver{re, &dockerResolver{originClient}}
		case "default":
			sr = &subResolver{re, &defaultResolver{}}
		default:
			return nil, fmt.Errorf("type %s is undefined", config.Type)
		}
		subResolvers = append(subResolvers, sr)
	}
	return &Map{subResolvers}, nil
}

// Resolve executes the sub resolver configured for tag.
func (m *Map) Resolve(tag string, d core.Digest) (core.DigestList, error) {
	for _, sr := range m.subResolvers {
		if sr.regexp.MatchString(tag) {
			return sr.resolver.Resolve(tag, d)
		}
	}
	return nil, errNamespaceNotFound
}
