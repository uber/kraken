package backend

import (
	"fmt"

	"code.uber.internal/infra/kraken/lib/backend/hdfsbackend"
	"code.uber.internal/infra/kraken/lib/backend/proxybackend"
	"code.uber.internal/infra/kraken/lib/backend/s3backend"
	"code.uber.internal/infra/kraken/lib/backend/testfs"
)

// Manager manages backend clients for namespaces.
type Manager struct {
	clients map[string]Client
}

// NewManager creates a new Manager.
func NewManager(namespaces NamespaceConfig, auth AuthNamespaceConfig) (*Manager, error) {
	clients := make(map[string]Client)
	for ns, config := range namespaces {
		var c Client
		var err error

		switch config.Backend {
		case "s3_dockerblob":
			c, err = s3backend.NewDockerBlobClient(config.S3, auth[ns].S3, ns)
		case "s3_dockertag":
			c, err = s3backend.NewDockerTagClient(config.S3, auth[ns].S3, ns)
		case "hdfs_dockerblob":
			c, err = hdfsbackend.NewDockerBlobClient(config.HDFS)
		case "hdfs_dockertag":
			c, err = hdfsbackend.NewDockerTagClient(config.HDFS)
		case "proxy_dockertag":
			c, err = proxybackend.NewDockerTagClient(config.Proxy)
		case "testfs":
			c, err = testfs.NewClient(config.TestFS)
		default:
			return nil, fmt.Errorf("unknown backend for namespace %s: %s", ns, config.Backend)
		}
		if err != nil {
			return nil, fmt.Errorf("new client for backend %s: %s", config.Backend, err)
		}
		clients[ns] = c
	}
	return &Manager{clients}, nil
}

// Register dynamically registers a namespace with a provided client. Register
// should be primarily used for testing purposes -- namespaces should almost
// always be statically configured and provided upon construction of the Manager.
func (m *Manager) Register(namespace string, c Client) error {
	if _, ok := m.clients[namespace]; ok {
		return fmt.Errorf("namespace %s already exists", namespace)
	}
	m.clients[namespace] = c
	return nil
}

// GetClient returns the configured Client for the given namespace.
func (m *Manager) GetClient(namespace string) (Client, error) {
	c, ok := m.clients[namespace]
	if !ok {
		return nil, fmt.Errorf("namespace %s not found", namespace)
	}
	return c, nil
}
