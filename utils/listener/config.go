package listener

import "fmt"

// Config defines listener configuration.
type Config struct {
	// Net is the network to listen on, e.g. unix, tcp, etc.
	Net string `yaml:"net"`

	// Addr is the address to listen on.
	Addr string `yaml:"addr"`
}

func (c Config) String() string {
	return fmt.Sprintf("%s:%s", c.Net, c.Addr)
}
