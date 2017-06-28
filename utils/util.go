package utils

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math/rand"
	"net"
	"os/exec"
	"strconv"
	"strings"
	"time"

	"github.com/docker/distribution"
	"github.com/docker/distribution/manifest/schema2"
)

func init() {
	rand.Seed(time.Now().UnixNano())
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

// GetLocalIP returns the first non loopback intreface's IP
func GetLocalIP() (string, error) {
	ifis, err := net.Interfaces()
	if err != nil {
		return "", err
	}
	for _, ifi := range ifis {
		// TODO: Extend for Running and UP maybe? (@igor)
		if ifi.Flags&net.FlagLoopback != 0 {
			continue
		}
		addrs, err := ifi.Addrs()
		if err != nil {
			return "", err
		}

		for _, a := range addrs {
			ipnet, _ := a.(*net.IPNet)
			if ipnet.IP.To4() != nil {
				return ipnet.IP.String(), nil
			}
		}
	}
	return "", errors.New("Could not find any IPv4 network interface")
}

// AddrIP extracts IP address from a addr:port string
func AddrIP(addr string) (net.IP, error) {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return nil, err
	}
	return net.ParseIP(host), nil
}

// AddrPort extracts port from a addr:port string
func AddrPort(addr string) (int, error) {
	_, port, err := net.SplitHostPort(addr)
	if err != nil {
		return -1, err
	}
	i64, err := strconv.ParseInt(port, 0, 0)
	if err != nil {
		return -1, err
	}
	return int(i64), nil
}

// AddrIPPort extracts IP address and port from a addr:port string
func AddrIPPort(addr string) (net.IP, int, error) {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return nil, -1, err
	}
	i64, err := strconv.ParseInt(port, 0, 0)
	if err != nil {
		return nil, -1, err
	}

	return net.ParseIP(host), int(i64), nil
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

// ParseManifestV2 returns a parsed v2 manifest and its digest
func ParseManifestV2(data []byte) (distribution.Manifest, string, error) {
	manifest, descriptor, err := distribution.UnmarshalManifest(schema2.MediaTypeManifest, data)
	if err != nil {
		return nil, "", err
	}
	deserializedManifest, ok := manifest.(*schema2.DeserializedManifest)
	if !ok {
		return nil, "", fmt.Errorf("Unable to deserialize manifest")
	}
	version := deserializedManifest.Manifest.Versioned.SchemaVersion
	if version != 2 {
		return nil, "", fmt.Errorf("Unsupported manifest version: %d", version)
	}
	return manifest, descriptor.Digest.Hex(), nil
}

// GetManifestV2References returns a list of references by a V2 manifest
func GetManifestV2References(manifest distribution.Manifest, manifestDigest string) ([]string, error) {
	layers := []string{manifestDigest}

	switch manifest.(type) {
	case *schema2.DeserializedManifest:
		// Inc ref count for config and data layers.
		descriptors := manifest.References()
		for _, descriptor := range descriptors {
			if descriptor.Digest == "" {
				return nil, fmt.Errorf("Unsupported layer format in manifest")
			}

			layers = append(layers, descriptor.Digest.Hex())
		}
	default:
		return nil, fmt.Errorf("Unsupported manifest format")
	}
	return layers, nil
}

const (
	numbers = "0123456789"
	letters = "abcdefghijklmnopqrstuvwxyz"
)

func chooseRandom(choices string, n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = choices[rand.Intn(len(choices))]
	}
	return string(b)
}

// RandomHexString returns a random hexadecimal string of length n.
func RandomHexString(n int) string {
	choices := numbers + letters[:6]
	return chooseRandom(choices, n)
}

// RandomString returns a random alphanumeric string of length n.
func RandomString(n int) string {
	choices := letters + strings.ToUpper(letters) + numbers
	return chooseRandom(choices, n)
}
