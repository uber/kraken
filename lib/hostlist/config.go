package hostlist

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"strings"
	"time"

	"code.uber.internal/infra/kraken/utils/stringset"
)

// Config defines a list of hosts using either a DNS record or a static list of
// addresses. If present, a DNS record always takes precedence over a static
// list.
type Config struct {
	// DNS record from which to resolve host names.
	DNS string `yaml:"dns"`

	// Statically configured host names.
	Static []string `yaml:"static"`

	// TTL defines how long resolved host lists are cached for.
	TTL time.Duration `yaml:"ttl"`
}

func (c *Config) applyDefaults() {
	if c.TTL == 0 {
		c.TTL = 5 * time.Second
	}
}

func (c *Config) snapshot(port int) (stringset.Set, error) {
	names, err := c.resolve()
	if err != nil {
		return nil, fmt.Errorf("resolve: %s", err)
	}
	addrs, err := attachPortIfMissing(names, port)
	if err != nil {
		return nil, fmt.Errorf("attach port to resolved names: %s", err)
	}
	localNames, err := getLocalNames()
	if err != nil {
		return nil, fmt.Errorf("get local names: %s", err)
	}
	localAddrs, err := attachPortIfMissing(localNames, port)
	if err != nil {
		return nil, fmt.Errorf("attach port to local names: %s", err)
	}
	return addrs.Sub(localAddrs), nil
}

func (c *Config) resolve() (stringset.Set, error) {
	if c.DNS == "" {
		return stringset.FromSlice(c.Static), nil
	}
	var r net.Resolver
	addrs, err := r.LookupHost(context.Background(), c.DNS)
	if err != nil {
		return nil, fmt.Errorf("resolve dns: %s", err)
	}
	if len(addrs) == 0 {
		return nil, errors.New("dns record empty")
	}
	return stringset.FromSlice(addrs), nil
}

func getLocalNames() (stringset.Set, error) {
	result := make(stringset.Set)

	// Add all local non-loopback ips.
	ifaces, err := net.Interfaces()
	if err != nil {
		return nil, fmt.Errorf("interfaces: %s", err)
	}
	for _, i := range ifaces {
		addrs, err := i.Addrs()
		if err != nil {
			return nil, fmt.Errorf("addrs of %v: %s", i, err)
		}
		for _, addr := range addrs {
			ip := net.ParseIP(addr.String()).To4()
			if ip == nil {
				continue
			}
			if ip.IsLoopback() {
				continue
			}
			result.Add(ip.String())
		}
	}

	// Add local hostname just to be safe.
	hostname, err := os.Hostname()
	if err != nil {
		return nil, fmt.Errorf("hostname: %s", err)
	}
	result.Add(hostname)

	return result, nil
}

func attachPortIfMissing(names stringset.Set, port int) (stringset.Set, error) {
	result := make(stringset.Set)
	for name := range names {
		parts := strings.Split(name, ":")
		switch len(parts) {
		case 1:
			// Name is in 'host' format -- attach port.
			name = fmt.Sprintf("%s:%d", parts[0], port)
		case 2:
			// No-op, name is already in "ip:port" format.
		default:
			return nil, fmt.Errorf("invalid name format: %s, expected 'host' or 'ip:port'", name)
		}
		result.Add(name)
	}
	return result, nil
}
