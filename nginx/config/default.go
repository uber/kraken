package config

import (
	"fmt"
)

var _nameToDefaultTmpl = map[string]string{
	"base":               BaseTmpl,
	"kraken-agent":       AgentTmpl,
	"kraken-origin":      OriginTmpl,
	"kraken-build-index": BuildIndexTmpl,
	"kraken-tracker":     TrackerTmpl,
	"kraken-proxy":       ProxyTmpl,
}

// GetDefaultTemplate returns the tmpl given name.
func GetDefaultTemplate(name string) (string, error) {
	if tmpl, ok := _nameToDefaultTmpl[name]; ok {
		return tmpl, nil
	}
	return "", fmt.Errorf("name not found")
}
