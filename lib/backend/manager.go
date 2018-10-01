package backend

import (
	"errors"
	"fmt"
	"regexp"

	"code.uber.internal/infra/kraken/lib/backend/hdfsbackend"
	"code.uber.internal/infra/kraken/lib/backend/httpbackend"
	"code.uber.internal/infra/kraken/lib/backend/s3backend"
	"code.uber.internal/infra/kraken/lib/backend/terrablobbackend"
	"code.uber.internal/infra/kraken/lib/backend/testfs"
	"code.uber.internal/infra/kraken/utils/bandwidth"
	"code.uber.internal/infra/kraken/utils/log"
)

// Manager errors.
var (
	ErrNamespaceNotFound = errors.New("no matches for namespace")
)

// Available backends.
const (
	_s3        = "s3"
	_hdfs      = "hdfs"
	_http      = "http"
	_terrablob = "terrablob"
	_testfs    = "testfs"
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
		config = config.applyDefaults()
		var c Client
		var err error
		switch config.Backend {
		case _s3:
			c, err = s3backend.NewClient(config.S3, auth.S3)
		case _hdfs:
			c, err = hdfsbackend.NewClient(config.HDFS)
		case _http:
			c, err = httpbackend.NewClient(config.HTTP)
		case _terrablob:
			c, err = terrablobbackend.NewClient(config.TerraBlob)
		case _testfs:
			c, err = testfs.NewClient(config.TestFS)
		default:
			return nil, fmt.Errorf(
				"unknown backend for namespace %s: %s", config.Namespace, config.Backend)
		}
		if err != nil {
			return nil, fmt.Errorf("new client for backend %s: %s", config.Backend, err)
		}
		if config.Bandwidth.Enable {
			l, err := bandwidth.NewLimiter(config.Bandwidth)
			if err != nil {
				return nil, fmt.Errorf("bandwidth: %s", err)
			}
			c = throttle(c, l)
		}
		b, err := newBackend(config.Namespace, c)
		if err != nil {
			return nil, fmt.Errorf("new backend for namespace %s: %s", config.Namespace, err)
		}
		backends = append(backends, b)
	}
	return &Manager{backends}, nil
}

// AdjustBandwidth adjusts bandwidth limits across all throttled clients to the
// originally configured bandwidth divided by denominator.
func (m *Manager) AdjustBandwidth(denominator int) error {
	for _, b := range m.backends {
		tc, ok := b.client.(*throttledClient)
		if !ok {
			continue
		}
		if err := tc.adjustBandwidth(denominator); err != nil {
			return err
		}
		log.With(
			"namespace", b.regexp.String(),
			"ingress", tc.ingressLimit(),
			"egress", tc.egressLimit(),
			"denominator", denominator).Info("Adjusted backend bandwidth")
	}
	return nil
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
