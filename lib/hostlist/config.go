package hostlist

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"strings"

	"code.uber.internal/infra/kraken/utils/netutil"
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
}

// Build resolves c into a set of addresses in 'ip:port' format. Build is very
// flexible in what host strings are accepted. Names missing a port suffix will
// have the provided port attached. Hosts with a port suffix will be untouched.
// Either ip addresses or host names are allowed.
//
// Build also strips the local machine from the resolved address list, if present.
// The local machine is identified by both its hostname and ip address, concatenated
// with the provided port.
//
// An error is returned if a DNS record is supplied and resolves to an empty list
// of addresses.
func (c Config) Build(port int) (stringset.Set, error) {
	names, err := c.resolve()
	if err != nil {
		return nil, err
	}
	addrs, err := attachPortIfMissing(names, port)
	if err != nil {
		return nil, err
	}
	hostname, err := os.Hostname()
	if err != nil {
		return nil, fmt.Errorf("hostname: %s", err)
	}
	ip, err := netutil.GetIP(hostname)
	if err != nil {
		return nil, fmt.Errorf("get ip of %s: %s", hostname, err)
	}
	addrs = filter(addrs, fmt.Sprintf("%s:%d", hostname, port))
	addrs = filter(addrs, fmt.Sprintf("%s:%d", ip, port))
	return stringset.FromSlice(addrs), nil
}

func (c Config) resolve() ([]string, error) {
	if c.DNS == "" {
		return c.Static, nil
	}
	var r net.Resolver
	addrs, err := r.LookupHost(context.Background(), c.DNS)
	if err != nil {
		return nil, fmt.Errorf("resolve dns: %s", err)
	}
	if len(addrs) == 0 {
		return nil, errors.New("dns record empty")
	}
	return addrs, nil
}

func attachPortIfMissing(names []string, port int) ([]string, error) {
	var result []string
	for _, name := range names {
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
		result = append(result, name)
	}
	return result, nil
}

func filter(addrs []string, x string) []string {
	var result []string
	for _, a := range addrs {
		if a == x {
			continue
		}
		result = append(result, a)
	}
	return result
}
