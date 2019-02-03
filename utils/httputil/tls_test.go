package httputil

import (
	"bytes"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"io/ioutil"
	"math/big"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/pressly/chi"
	"github.com/stretchr/testify/require"

	"github.com/uber/kraken/utils/randutil"
	"github.com/uber/kraken/utils/testutil"
)

func genKeyPair(t *testing.T, caPEM, caKeyPEM, caSercret []byte) (certPEM, keyPEM, secretBytes []byte) {
	require := require.New(t)
	secret := randutil.Text(12)
	priv, err := rsa.GenerateKey(rand.Reader, 4096)
	require.NoError(err)
	pub := priv.Public()
	template := x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject: pkix.Name{
			Organization: []string{"kraken"},
			CommonName:   "kraken",
		},
		NotBefore: time.Now().Add(-5 * time.Minute),
		NotAfter:  time.Now().Add(time.Hour * 24 * 180),

		KeyUsage:              x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		BasicConstraintsValid: true,

		// Need for identifying root CA.
		IsCA: caPEM == nil,
	}

	parent := &template
	parentPriv := priv
	// If caPEM is provided, certificate generated should have ca cert as parent.
	if caPEM != nil {
		block, _ := pem.Decode(caPEM)
		require.NotNil(block)
		caCert, err := x509.ParseCertificate(block.Bytes)
		require.NoError(err)
		block, _ = pem.Decode(caKeyPEM)
		require.NotNil(block)
		decoded, err := x509.DecryptPEMBlock(block, caSercret)
		require.NoError(err)
		caKey, err := x509.ParsePKCS1PrivateKey(decoded)
		require.NoError(err)

		parent = caCert
		parentPriv = caKey
	}
	// Certificate should be signed with parent certificate, parent private key and child public key.
	// If the certificate is self-signed, parent is an empty template, and parent private key is the private key of the public key.
	derBytes, err := x509.CreateCertificate(rand.Reader, &template, parent, pub, parentPriv)
	require.NoError(err)

	// Encode cert and key to PEM format.
	cert := &bytes.Buffer{}
	require.NoError(pem.Encode(cert, &pem.Block{Type: "CERTIFICATE", Bytes: derBytes}))
	encrypted, err := x509.EncryptPEMBlock(rand.Reader, "RSA PRIVATE KEY", x509.MarshalPKCS1PrivateKey(priv), secret, x509.PEMCipherAES256)
	require.NoError(err)
	return cert.Bytes(), pem.EncodeToMemory(encrypted), secret
}

func genFile(t *testing.T, bytesPEM []byte) (string, func()) {
	require := require.New(t)
	var cleanup testutil.Cleanup
	defer cleanup.Recover()

	f, err := ioutil.TempFile(".", "")
	require.NoError(err)
	cleanup.Add(func() { os.Remove(f.Name()) })
	defer f.Close()
	_, err = f.Write(bytesPEM)
	require.NoError(err)

	return f.Name(), cleanup.Run
}

func genCerts(t *testing.T) (config *TLSConfig, serverCerts []X509Pair, cleanupfunc func()) {
	var cleanup testutil.Cleanup
	defer cleanup.Recover()

	// Create two CA certs.
	// Server cert, which is also the root CA.
	s1CertPEM, s1KeyPEM, s1SecretBytes := genKeyPair(t, nil, nil, nil)
	s1Secret, c := genFile(t, s1SecretBytes)
	cleanup.Add(c)
	s1Cert, c := genFile(t, s1CertPEM)
	cleanup.Add(c)
	s1Key, c := genFile(t, s1KeyPEM)
	cleanup.Add(c)
	s1 := X509Pair{
		Cert:       Secret{s1Cert},
		Key:        Secret{s1Key},
		Passphrase: Secret{s1Secret},
	}
	s2CertPEM, s2KeyPEM, s2SecretBytes := genKeyPair(t, nil, nil, nil)
	s2Secret, c := genFile(t, s2SecretBytes)
	cleanup.Add(c)
	s2Cert, c := genFile(t, s2CertPEM)
	cleanup.Add(c)
	s2Key, c := genFile(t, s2KeyPEM)
	cleanup.Add(c)
	s2 := X509Pair{
		Cert:       Secret{s2Cert},
		Key:        Secret{s2Key},
		Passphrase: Secret{s2Secret},
	}

	// Client cert, signed with root CA.
	cCertPEM, cKeyPEM, cSecretBytes := genKeyPair(t, s1CertPEM, s1KeyPEM, s1SecretBytes)
	cSecret, c := genFile(t, cSecretBytes)
	cleanup.Add(c)
	cCert, c := genFile(t, cCertPEM)
	cleanup.Add(c)
	cKey, c := genFile(t, cKeyPEM)
	cleanup.Add(c)

	config = &TLSConfig{}
	config.Name = "kraken"
	config.CAs = []Secret{{s1Cert}, {s2Cert}}
	config.Client.Cert.Path = cCert
	config.Client.Key.Path = cKey
	config.Client.Passphrase.Path = cSecret

	return config, []X509Pair{s1, s2}, cleanup.Run
}

func startTLSServer(t *testing.T, cert, key, passphrase string, clientCAs []Secret) (string, func()) {
	fmt.Println(key)
	require := require.New(t)
	var err error
	certPEM, err := parseCert(cert)
	require.NoError(err)
	keyPEM, err := parseKey(key, passphrase)
	require.NoError(err)
	c, err := tls.X509KeyPair(certPEM, keyPEM)
	require.NoError(err)
	caPool, err := createCertPool(clientCAs...)
	require.NoError(err)

	config := &tls.Config{
		Certificates: []tls.Certificate{c},
		ServerName:   "kraken",

		// A list if trusted CA to verify certificate from clients.
		// In this test, server is using the root CA as both cert and trusted client CA.
		ClientCAs: caPool,

		// Enforce tls on client.
		ClientAuth: tls.RequireAndVerifyClientCert,
		CipherSuites: []uint16{tls.TLS_RSA_WITH_AES_128_CBC_SHA,
			tls.TLS_RSA_WITH_AES_256_CBC_SHA,
			tls.TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA,
			tls.TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA,
			tls.TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA,
			tls.TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA,
			tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,
			tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256},
	}

	l, err := tls.Listen("tcp", ":0", config)
	require.NoError(err)
	r := chi.NewRouter()
	r.Get("/", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		fmt.Fprintln(w, "OK")
	})
	go http.Serve(l, r)
	return l.Addr().String(), func() { l.Close() }
}

func TestTLSClientDisabled(t *testing.T) {
	require := require.New(t)

	c, _, cleanup := genCerts(t)
	defer cleanup()

	c.Client.Disabled = true
	tls, err := c.BuildClient()
	require.NoError(err)
	require.Nil(tls)
}

func TestTLSClient(t *testing.T) {
	c, serverCerts, cleanup := genCerts(t)
	defer cleanup()
	tls, err := c.BuildClient()
	require.NoError(t, err)

	t.Run("success", func(t *testing.T) {
		require := require.New(t)
		addr1, stop := startTLSServer(t, serverCerts[0].Cert.Path, serverCerts[0].Key.Path, serverCerts[0].Passphrase.Path, c.CAs)
		defer stop()

		resp, err := Get("https://"+addr1+"/", SendTLS(tls))
		require.NoError(err)
		require.Equal(http.StatusOK, resp.StatusCode)

		addr2, stop := startTLSServer(t, serverCerts[1].Cert.Path, serverCerts[1].Key.Path, serverCerts[1].Passphrase.Path, c.CAs)
		defer stop()

		resp, err = Get("https://"+addr2+"/", SendTLS(tls))
		require.NoError(err)
		require.Equal(http.StatusOK, resp.StatusCode)
	})

	t.Run("authentication failed", func(t *testing.T) {
		require := require.New(t)
		addr, stop := startTLSServer(t, serverCerts[0].Cert.Path, serverCerts[0].Key.Path, serverCerts[0].Passphrase.Path, c.CAs)
		defer stop()

		// Swap client and server certs. This should make verification fail.
		badConfig := &TLSConfig{}
		badConfig.Name = "kraken"
		badConfig.Server = c.Client
		badConfig.Client = c.Server
		badtls, err := badConfig.BuildClient()
		require.NoError(err)

		_, err = Get("https://"+addr+"/", SendTLS(badtls))
		require.True(IsNetworkError(err))
	})

	t.Run("fallback on http server", func(t *testing.T) {
		require := require.New(t)
		r := chi.NewRouter()
		r.Get("/", func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			fmt.Fprintln(w, "OK")
		})
		addr, stop := testutil.StartServer(r)
		defer stop()

		resp, err := Get("https://"+addr+"/", SendTLS(tls))
		require.NoError(err)
		require.Equal(http.StatusOK, resp.StatusCode)
	})
}
