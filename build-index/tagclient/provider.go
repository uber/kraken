package tagclient

// Provider maps addresses into Clients.
type Provider interface {
	Provide(addr string) Client
}

type provider struct{}

// NewProvider creates a Provider which wraps NewSingleClient.
func NewProvider() Provider { return provider{} }

func (p provider) Provide(addr string) Client { return NewSingleClient(addr) }
