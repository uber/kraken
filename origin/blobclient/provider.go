package blobclient

// Provider defines an interface for creating Client scoped to an origin addr.
type Provider interface {
	Provide(addr string) Client
}

// HTTPProvider provides HTTPClients.
type HTTPProvider struct{}

// NewProvider returns a new HTTPProvider.
func NewProvider() HTTPProvider {
	return HTTPProvider{}
}

// Provide implements ClientProvider's Provide.
// TODO(codyg): Make this return error.
func (p HTTPProvider) Provide(addr string) Client {
	return New(addr)
}
