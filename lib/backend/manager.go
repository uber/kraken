package backend

import (
	"errors"
	"fmt"
	"regexp"

	"code.uber.internal/infra/kraken/lib/backend/hdfsbackend"
	"code.uber.internal/infra/kraken/lib/backend/httpbackend"
	"code.uber.internal/infra/kraken/lib/backend/originbackend"
	"code.uber.internal/infra/kraken/lib/backend/s3backend"
	"code.uber.internal/infra/kraken/lib/backend/testfs"
)

// Manager errors.
var (
	ErrNamespaceNotFound = errors.New("no matches for namespace")
)

type backend struct {
	regexp *regexp.Regexp
	client Client
}

func newBackend(namespace string, c Client) (*backend, error) {
	re, err := regexp.Compile(namespace)
	if err != nil {
		return nil, fmt.Errorf("regexp: %s", err)
	}
	return &backend{
		regexp: re,
		client: c,
	}, nil
}

// Manager manages backend clients for namespace regular expressions.
type Manager struct {
	backends []*backend
}

// NewManager creates a new Manager.
func NewManager(configs []Config, auth AuthConfig) (*Manager, error) {
	var backends []*backend
	for _, config := range configs {
		var c Client
		var err error
		switch config.Backend {
		case "s3":
			c, err = s3backend.NewClient(config.S3, auth.S3)
		case "hdfs":
			c, err = hdfsbackend.NewClient(config.HDFS)
		case "http":
			c, err = httpbackend.NewClient(config.HTTP)
		case "origin":
			c, err = originbackend.NewClient(config.Origin)
		case "testfs":
			c, err = testfs.NewClient(config.TestFS)
		default:
			return nil, fmt.Errorf(
				"unknown backend for namespace %s: %s", config.Namespace, config.Backend)
		}
		if err != nil {
			return nil, fmt.Errorf("new client for backend %s: %s", config.Backend, err)
		}
		b, err := newBackend(config.Namespace, c)
		if err != nil {
			return nil, fmt.Errorf("new backend for namespace %s: %s", config.Namespace, err)
		}
		backends = append(backends, b)
	}
	return &Manager{backends}, nil
}

// Register dynamically registers a namespace with a provided client. Register
// should be primarily used for testing purposes -- normally, namespaces should
// be statically configured and provided upon construction of the Manager.
func (m *Manager) Register(namespace string, c Client) error {
	for _, b := range m.backends {
		if b.regexp.String() == namespace {
			return fmt.Errorf("namespace %s already exists", namespace)
		}
	}
	b, err := newBackend(namespace, c)
	if err != nil {
		return fmt.Errorf("new backend: %s", err)
	}
	m.backends = append(m.backends, b)
	return nil
}

// GetClient matches namespace to the configured Client. Returns ErrNamespaceNotFound
// if no clients match namespace.
func (m *Manager) GetClient(namespace string) (Client, error) {
	if namespace == "" {
		return nil, errors.New("namespace is empty")
	}
	if namespace == NoopNamespace {
		return NoopClient{}, nil
	}
	for _, b := range m.backends {
		if b.regexp.MatchString(namespace) {
			return b.client, nil
		}
	}
	return nil, ErrNamespaceNotFound
}
