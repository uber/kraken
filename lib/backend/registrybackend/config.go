package registrybackend

import "github.com/uber/kraken/lib/backend/registrybackend/security"

// Config defines the registry address and security options.
type Config struct {
	Address  string          `yaml:"address"`
	Security security.Config `yaml:"security"`
}
