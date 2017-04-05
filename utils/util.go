package utils

import (
	"fmt"
	"net"
	"os/exec"
	"strings"
)

// GetHostIP returns the host ip
func GetHostIP() (string, error) {
	out, err := exec.Command("bash", "-c", "ifconfig eth0 | grep 'inet addr:' | cut -d: -f2 | awk '{ print $1}'").Output()
	name := strings.Trim(string(out[:]), "\n")
	if err != nil {
		return "", err
	}
	return name, nil
}

// GetHostName returns host name
func GetHostName() (string, error) {
	out, err := exec.Command("bash", "-c", "echo $HOSTNAME").Output()
	name := strings.Trim(string(out[:]), "\n")
	if err != nil {
		return "", err
	}
	return name, nil
}

// GetIP returns ip given hostname
func GetIP(host string) (net.IP, error) {
	ips, err := net.LookupIP(host)
	if err != nil {
		return nil, err
	}

	for _, ip := range ips {
		if ip == nil || ip.IsLoopback() {
			continue
		}
		return ip, nil
	}
	return nil, fmt.Errorf("Error getting ip for %s", host)
}

// CompareByteArray compares two bytes array
func CompareByteArray(d1 []byte, d2 []byte) bool {
	if len(d1) != len(d2) {
		return false
	}

	n := len(d1)
	for i := 0; i < n; i++ {
		if d1[i] != d2[i] {
			return false
		}
	}

	return true
}
