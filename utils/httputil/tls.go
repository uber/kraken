package httputil

import (
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"io/ioutil"

	"github.com/uber/kraken/utils/log"
)

// ErrEmptyCommonName is returned when common name is not provided for key generation.
var ErrEmptyCommonName = errors.New("empty common name")

// TLSConfig defines TLS configuration.
type TLSConfig struct {
	Name   string   `yaml:"name"`
	CA     X509Pair `yaml:"ca"`
	Client X509Pair `yaml:"client"`
}

// X509Pair contains x509 cert configuration.
// Both Cert and Key should be already in pem format.
type X509Pair struct {
	Enabled    bool   `yaml:"enabled"`
	Cert       Secret `yaml:"cert"`
	Key        Secret `yaml:"key"`
	Passphrase Secret `yaml:"passphrase"`
}

// Secret contains secret path configuration.
type Secret struct {
	Path string `yaml:"path"`
}

// BuildClient builts tls.Config for http client.
func (c TLSConfig) BuildClient() (*tls.Config, error) {
	if !c.Client.Enabled {
		log.Warnf("Client TLS is disabled")
		return nil, nil
	}
	if c.Name == "" {
		return nil, ErrEmptyCommonName
	}

	caPool, err := createCertPool(c.CA.Cert.Path)
	if err != nil {
		return nil, fmt.Errorf("create cert pool: %s", err)
	}
	certPEM, err := parseCert(c.Client.Cert.Path)
	if err != nil {
		return nil, fmt.Errorf("parse client cert: %s", err)
	}
	keyPEM, err := parseKey(c.Client.Key.Path, c.Client.Passphrase.Path)
	if err != nil {
		return nil, fmt.Errorf("parse client key: %s", err)
	}
	cert, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		return nil, fmt.Errorf("load client x509 key pair: %s", err)
	}

	return &tls.Config{
		Certificates:             []tls.Certificate{cert},
		RootCAs:                  caPool,
		ServerName:               c.Name,
		PreferServerCipherSuites: true,
		InsecureSkipVerify:       false, // This is important to enforce verification of server.
	}, nil
}

func createCertPool(paths ...string) (*x509.CertPool, error) {
	pool := x509.NewCertPool()
	for _, p := range paths {
		pem, err := parseCert(p)
		if err != nil {
			return nil, fmt.Errorf("parse cert: %s", err)
		}
		if ok := pool.AppendCertsFromPEM(pem); !ok {
			return nil, fmt.Errorf("cannot append cert")
		}
	}
	return pool, nil
}

func parseCert(path string) ([]byte, error) {
	certBytes, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read file: %s", err)
	}
	return certBytes, nil
}

// parseKey reads key from file and decrypts if passphrase is provided.
func parseKey(path, passphrasePath string) ([]byte, error) {
	keyPEM, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read file: %s", err)
	}
	if passphrasePath != "" {
		passphrase, err := ioutil.ReadFile(passphrasePath)
		if err != nil {
			return nil, fmt.Errorf("read passphrase file: %s", err)
		}
		keyBytes, err := decryptPEMBlock(keyPEM, passphrase)
		if err != nil {
			return nil, fmt.Errorf("decrypt key: %s", err)
		}
		keyPEM, err = encodePEMKey(keyBytes)
		if err != nil {
			return nil, fmt.Errorf("encode key: %s", err)
		}
	}
	return keyPEM, nil
}

// decryptPEMBlock decrypts the block of data.
func decryptPEMBlock(data, secret []byte) ([]byte, error) {
	block, _ := pem.Decode(data)
	if block == nil || len(block.Bytes) < 1 {
		return nil, errors.New("empty block")
	}
	decoded, err := x509.DecryptPEMBlock(block, secret)
	if err != nil {
		return nil, fmt.Errorf("decrypt block: %s", err)
	}
	return decoded, nil
}

// encodePEMKey marshals the DER-encoded private key.
func encodePEMKey(data []byte) ([]byte, error) {
	buf := new(bytes.Buffer)
	err := pem.Encode(buf, &pem.Block{Type: "RSA PRIVATE KEY", Bytes: data})
	if err != nil {
		return nil, fmt.Errorf("encode key: %s", err)
	}
	return buf.Bytes(), nil
}
