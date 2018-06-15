package tagtype

import (
	"errors"
	"fmt"
	"regexp"

	"code.uber.internal/infra/kraken/core"
	"code.uber.internal/infra/kraken/origin/blobclient"
)

// ErrNamespaceNotFound is returned when manager is unable to find checker for given namespace.
var ErrNamespaceNotFound = errors.New("no matches for namespace")

// Config defines the namespace and the type of checker associated with it.
type Config struct {
	Namespace string `yaml:"namespace"`
	Type      string `yaml:"type"`
}

// DependencyResolver defines an interface for resolving dependencies for a tag.
type DependencyResolver interface {
	Resolve(tag string, d core.Digest) (core.DigestList, error)
}

// Manager defines an interface for managing dependency resolver based on tag type.
type Manager interface {
	GetDependencyResolver(tag string) (DependencyResolver, error)
}

type resolver struct {
	depResolver DependencyResolver
	regexp      *regexp.Regexp
}

type manager struct {
	resolvers []resolver
}

// NewManager creates new tag type manager.
func NewManager(configs []Config, originClient blobclient.ClusterClient) (Manager, error) {
	var resolvers []resolver

	if len(configs) == 0 {
		return nil, fmt.Errorf("no config specified")
	}

	for _, config := range configs {
		re, err := regexp.Compile(config.Namespace)
		if err != nil {
			return nil, fmt.Errorf("regexp: %s", err)
		}

		var r resolver
		switch config.Type {
		case "docker":
			r = resolver{NewDockerResolver(originClient), re}
		case "default":
			r = resolver{NewDefaultResolver(originClient), re}
		default:
			return nil, fmt.Errorf("type %s is undefined", config.Type)
		}
		resolvers = append(resolvers, r)
	}
	return &manager{resolvers}, nil
}

// GetDependencyResolver returns the resolver to given tag.
func (m *manager) GetDependencyResolver(tag string) (DependencyResolver, error) {
	for _, r := range m.resolvers {
		if r.regexp.MatchString(tag) {
			return r.depResolver, nil
		}
	}
	return nil, ErrNamespaceNotFound
}
