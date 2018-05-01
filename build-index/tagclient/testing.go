package tagclient

import "log"

// TestProvider is a testing utility for mapping addresses to mock clients.
type TestProvider struct {
	clients map[string]Client
}

// NewTestProvider creates a new TestProvider.
func NewTestProvider() *TestProvider {
	return &TestProvider{make(map[string]Client)}
}

// Register sets c as the client of addr.
func (p *TestProvider) Register(addr string, c Client) {
	p.clients[addr] = c
}

// Provide selects the registered client of addr.
func (p *TestProvider) Provide(addr string) Client {
	c, ok := p.clients[addr]
	if !ok {
		log.Panicf("addr %s not found", addr)
	}
	return c
}
