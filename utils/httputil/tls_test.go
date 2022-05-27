// Copyright (c) 2016-2019 Uber Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
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
	"math/big"
	"net/http"
	"testing"
	"time"

	"github.com/go-chi/chi"
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

func genCerts(t *testing.T) (config *TLSConfig, cleanupfunc func()) {
	var cleanup testutil.Cleanup
	defer cleanup.Recover()

	// Server cert, which is also the root CA.
	sCertPEM, sKeyPEM, sSecretBytes := genKeyPair(t, nil, nil, nil)
	sCert, c := testutil.TempFile(sCertPEM)
	cleanup.Add(c)

	// Client cert, signed with root CA.
	cCertPEM, cKeyPEM, cSecretBytes := genKeyPair(t, sCertPEM, sKeyPEM, sSecretBytes)
	cSecret, c := testutil.TempFile(cSecretBytes)
	cleanup.Add(c)
	cCert, c := testutil.TempFile(cCertPEM)
	cleanup.Add(c)
	cKey, c := testutil.TempFile(cKeyPEM)
	cleanup.Add(c)

	config = &TLSConfig{}
	config.Name = "kraken"
	config.CAs = []Secret{{sCert}, {sCert}}
	config.Client.Cert.Path = cCert
	config.Client.Key.Path = cKey
	config.Client.Passphrase.Path = cSecret

	return config, cleanup.Run
}

func startTLSServer(t *testing.T, clientCAs []Secret) (addr string, serverCA Secret, cleanupFunc func()) {
	var cleanup testutil.Cleanup
	defer cleanup.Recover()

	certPEM, keyPEM, passphrase := genKeyPair(t, nil, nil, nil)
	certPath, c := testutil.TempFile(certPEM)
	cleanup.Add(c)
	passphrasePath, c := testutil.TempFile(passphrase)
	cleanup.Add(c)
	keyPath, c := testutil.TempFile(keyPEM)
	cleanup.Add(c)

	require := require.New(t)
	var err error
	keyPEM, err = parseKey(keyPath, passphrasePath)
	require.NoError(err)
	x509cert, err := tls.X509KeyPair(certPEM, keyPEM)
	require.NoError(err)
	caPool, err := createCertPool(clientCAs)
	require.NoError(err)

	config := &tls.Config{
		Certificates: []tls.Certificate{x509cert},
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
	cleanup.Add(func() { l.Close() })
	return l.Addr().String(), Secret{certPath}, cleanup.Run
}

func TestTLSClientDisabled(t *testing.T) {
	require := require.New(t)
	c := TLSConfig{}
	c.Client.Disabled = true
	tls, err := c.BuildClient()
	require.NoError(err)
	require.Nil(tls)
}

func TestTLSClientSuccess(t *testing.T) {
	t.Skip("TODO https://github.com/uber/kraken/issues/230")

	require := require.New(t)
	c, cleanup := genCerts(t)
	defer cleanup()

	addr1, serverCA1, stop := startTLSServer(t, c.CAs)
	defer stop()
	addr2, serverCA2, stop := startTLSServer(t, c.CAs)
	defer stop()

	c.CAs = append(c.CAs, serverCA1, serverCA2)
	tls, err := c.BuildClient()
	require.NoError(err)

	resp, err := Get("https://"+addr1+"/", SendTLS(tls))
	require.NoError(err)
	require.Equal(http.StatusOK, resp.StatusCode)

	resp, err = Get("https://"+addr2+"/", SendTLS(tls))
	require.NoError(err)
	require.Equal(http.StatusOK, resp.StatusCode)
}

func TestTLSClientBadAuth(t *testing.T) {
	t.Skip("TODO https://github.com/uber/kraken/issues/230")

	require := require.New(t)
	c, cleanup := genCerts(t)
	defer cleanup()

	addr, _, stop := startTLSServer(t, c.CAs)
	defer stop()

	badConfig := &TLSConfig{}
	badtls, err := badConfig.BuildClient()
	require.NoError(err)

	_, err = Get("https://"+addr+"/", SendTLS(badtls), DisableHTTPFallback())
	require.True(IsNetworkError(err))
}

func TestTLSClientFallback(t *testing.T) {
	t.Skip("TODO https://github.com/uber/kraken/issues/230")

	require := require.New(t)
	c := &TLSConfig{}
	tls, err := c.BuildClient()
	require.NoError(err)

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
}

func TestTLSClientFallbackError(t *testing.T) {
	t.Skip("TODO https://github.com/uber/kraken/issues/230")

	require := require.New(t)

	c := &TLSConfig{}
	tls, err := c.BuildClient()
	require.NoError(err)

	_, err = Get("https://some-non-existent-addr/", SendTLS(tls))
	require.Error(err)
}
