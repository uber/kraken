package tagclient

import (
	"crypto/tls"
)

// Provider maps addresses into Clients.
type Provider interface {
	Provide(addr string) Client
}

type provider struct{ tls *tls.Config }

// NewProvider creates a Provider which wraps NewSingleClient.
func NewProvider(config *tls.Config) Provider { return provider{config} }

func (p provider) Provide(addr string) Client {
	return NewSingleClient(addr, p.tls)
}
