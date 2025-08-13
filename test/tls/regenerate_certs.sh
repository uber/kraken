#!/bin/bash

set -ex

# Move to the script's directory
cd "$(dirname "$0")"

# Clean up existing files
rm -rf ca/* client/*
mkdir -p ca client

# Generate passphrase files (for Kraken to read)
echo -n "kraken" > ca/passphrase
echo -n "kraken" > client/passphrase

cd ca

# Create a CA key with strong parameters using passphrase file (traditional PEM encryption)
# Use tr to remove any newlines from passphrase file when OpenSSL reads it
openssl genrsa 4096 2>/dev/null | \
    openssl rsa -aes256 -passout 'pass:'"$(tr -d '\n' < passphrase)" -traditional -out server.key

# Create CA config file with proper extensions and SAN for server use
cat > ca.conf <<EOF
[req]
distinguished_name = req_distinguished_name
x509_extensions = v3_ca

[req_distinguished_name]

[v3_ca]
basicConstraints = critical,CA:TRUE
keyUsage = critical,keyCertSign,cRLSign,digitalSignature,keyEncipherment
extendedKeyUsage = serverAuth,clientAuth
subjectKeyIdentifier = hash
authorityKeyIdentifier = keyid:always,issuer:always
subjectAltName = @alt_names_ca

[alt_names_ca]
DNS.1 = localhost
DNS.2 = kraken
DNS.3 = *.localhost
DNS.4 = *.kraken
IP.1 = 127.0.0.1
IP.2 = ::1
EOF

# Create CA certificate (self-signed) with SHA256 and proper CA extensions
openssl req -new -x509 -sha256 -days 365 \
    -key server.key \
    -passin 'pass:'"$(tr -d '\n' < passphrase)" \
    -subj "/C=US/ST=CA/L=San Francisco/O=Uber/OU=cluster-mgmt/CN=Kraken Test CA" \
    -config ca.conf \
    -extensions v3_ca \
    -out server.crt

cd ../client

# Create client key in PKCS#1 format with encryption
openssl genrsa 4096 2>/dev/null | \
    openssl rsa -aes256 -passout 'pass:'"$(tr -d '\n' < passphrase)" -traditional -out client.key

# Create a decrypted version for curl commands
openssl rsa -in client.key -passin 'pass:'"$(tr -d '\n' < passphrase)" -out client_decrypted.key

# Create client config file with SAN extensions
cat > client.conf <<EOF
[req]
distinguished_name = req_distinguished_name
req_extensions = v3_req

[req_distinguished_name]

[v3_req]
basicConstraints = CA:FALSE
keyUsage = nonRepudiation, digitalSignature, keyEncipherment
subjectAltName = @alt_names

[v3_client]
basicConstraints = CA:FALSE
keyUsage = nonRepudiation, digitalSignature, keyEncipherment
extendedKeyUsage = clientAuth
subjectAltName = @alt_names

[alt_names]
DNS.1 = localhost
DNS.2 = kraken
DNS.3 = *.localhost
IP.1 = 127.0.0.1
IP.2 = ::1
EOF

# Create client CSR with SHA256 and SAN extensions
openssl req -new -sha256 \
    -key client_decrypted.key \
    -subj "/C=US/ST=CA/L=San Francisco/O=Uber/OU=kraken/CN=Kraken Test Client" \
    -config client.conf \
    -extensions v3_req \
    -out client.csr

# Generate client certificate signed by CA with SHA256 and SAN extensions
openssl x509 -req -sha256 -days 365 \
    -in client.csr \
    -CA ../ca/server.crt \
    -CAkey ../ca/server.key \
    -passin 'pass:'"$(tr -d '\n' < ../ca/passphrase)" \
    -CAcreateserial \
    -extensions v3_client \
    -extfile client.conf \
    -out client.crt

# Verify the client certificate
openssl verify -CAfile ../ca/server.crt client.crt

# Set permissions
chmod 644 ../ca/server.crt client.crt
chmod 600 ../ca/server.key client.key client_decrypted.key ../ca/passphrase passphrase

# Clean up CSR and config files
rm -f ../ca/server.csr ../ca/ca.conf client.csr client.conf

# Show key format and verify it's in traditional PEM format with DEK-Info
openssl rsa -in client.key -text -noout -passin 'pass:'"$(tr -d '\n' < passphrase)"
