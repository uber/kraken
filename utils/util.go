package utils

import (
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"os/exec"
	"strings"
)

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

// GetLocalIP returns the non loopback local IP of the host
func GetLocalIP() (string, error) {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return "", err
	}
	for _, address := range addrs {
		// check the address type and if it is not a loopback the display it
		if ipnet, ok := address.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			if ipnet.IP.To4() != nil {
				return ipnet.IP.String(), nil
			}
		}
	}
	return "", errors.New("Could not found any IPv4 network interface")
}

// IPtoInt32 converts net.IP address to int32
func IPtoInt32(ip net.IP) int32 {
	if len(ip) == 16 {
		return int32(binary.BigEndian.Uint32(ip[12:16]))
	}
	return int32(binary.BigEndian.Uint32(ip))
}

// Int32toIP converts int32  to net.IP
func Int32toIP(i32 int32) net.IP {
	ip := make(net.IP, 4)
	binary.BigEndian.PutUint32(ip, uint32(i32))
	return ip
}
