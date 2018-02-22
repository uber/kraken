package proxybackend

import "errors"

// Config defines configuration for docker registry proxy clients.
type Config struct {
	Addr string `yaml:"addr"` // Remote docker registry proxy address.
}

func (c Config) applyDefaults() (Config, error) {
	if c.Addr == "" {
		return Config{}, errors.New("addr required")
	}

	return c, nil
}
