package netutil

import (
	"errors"
	"fmt"
	"net"
	"time"
)

func min(a, b time.Duration) time.Duration {
	if a < b {
		return a
	}
	return b
}

// WithRetry executes f maxRetries times until it returns non-nil error, sleeping
// for the given delay between retries with exponential backoff until maxDelay is
// reached.
func WithRetry(maxRetries uint, delay time.Duration, maxDelay time.Duration, f func() error) error {
	var retries uint
	for {
		err := f()
		if err == nil {
			return nil
		}
		if retries > maxRetries {
			return err
		}
		time.Sleep(min(delay*(1<<retries), maxDelay))
		retries++
	}
}

// GetIP looks up the ip of host.
func GetIP(host string) (net.IP, error) {
	ips, err := net.LookupIP(host)
	if err != nil {
		return nil, fmt.Errorf("net: %s", err)
	}
	for _, ip := range ips {
		if ip == nil || ip.IsLoopback() {
			continue
		}
		return ip, nil
	}
	return nil, errors.New("no ips found")
}

// GetLocalIP returns the ip address of the local machine.
func GetLocalIP() (string, error) {
	ifaces, err := net.Interfaces()
	if err != nil {
		return "", fmt.Errorf("interfaces: %s", err)
	}
	for _, i := range ifaces {
		if i.Name != "eth0" {
			continue
		}
		addrs, err := i.Addrs()
		if err != nil {
			return "", fmt.Errorf("addrs: %s", err)
		}
		for _, addr := range addrs {
			var ip net.IP
			switch v := addr.(type) {
			case *net.IPNet:
				ip = v.IP
			case *net.IPAddr:
				ip = v.IP
			}
			if ip == nil || ip.IsLoopback() {
				continue
			}
			ip = ip.To4()
			if ip == nil {
				continue
			}
			return ip.String(), nil
		}
	}
	return "", errors.New("no ip found")
}
