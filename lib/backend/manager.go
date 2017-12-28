package backend

import (
	"errors"
	"fmt"
)

// Manager manages backend clients for namespaces.
type Manager struct {
	clients map[string]Client
}

// NewManager creates a new Manager.
func NewManager(namespaces NamespaceConfig) (*Manager, error) {
	clients := make(map[string]Client)
	for ns, config := range namespaces {
		var c Client
		var err error
		switch config.Backend {
		// FIXME: Add client initialization here.
		case "fixme":
			err = errors.New("fixme")
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

// GetClient returns the configured Client for the given namespace.
func (m *Manager) GetClient(namespace string) (Client, error) {
	c, ok := m.clients[namespace]
	if !ok {
		return nil, fmt.Errorf("namespace %s not found", namespace)
	}
	return c, nil
}
