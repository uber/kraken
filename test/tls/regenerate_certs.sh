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

# Create CA certificate (self-signed) with SHA256 and proper CA extensions
openssl req -new -x509 -sha256 -days 365 \
    -key server.key \
    -passin 'pass:'"$(tr -d '\n' < passphrase)" \
    -subj "/C=US/ST=CA/L=San Francisco/O=Uber/OU=cluster-mgmt/CN=kraken" \
    -extensions v3_ca \
    -out server.crt

cd ../client

# Create client key in PKCS#1 format with encryption
openssl genrsa 4096 2>/dev/null | \
    openssl rsa -aes256 -passout 'pass:'"$(tr -d '\n' < passphrase)" -traditional -out client.key

# Create a decrypted version for curl commands
openssl rsa -in client.key -passin 'pass:'"$(tr -d '\n' < passphrase)" -out client_decrypted.key

# Create client CSR with SHA256
openssl req -new -sha256 \
    -key client_decrypted.key \
    -subj "/C=US/ST=CA/L=San Francisco/O=Uber/OU=kraken/CN=kraken" \
    -out client.csr

# Generate client certificate signed by CA with SHA256
openssl x509 -req -sha256 -days 365 \
    -in client.csr \
    -CA ../ca/server.crt \
    -CAkey ../ca/server.key \
    -passin 'pass:'"$(tr -d '\n' < ../ca/passphrase)" \
    -CAcreateserial \
    -out client.crt

# Verify the client certificate
openssl verify -CAfile ../ca/server.crt client.crt

# Set permissions
chmod 644 ../ca/server.crt client.crt
chmod 600 ../ca/server.key client.key client_decrypted.key ../ca/passphrase passphrase

# Clean up CSR files
rm -f ../ca/server.csr client.csr

# Show key format and verify it's in traditional PEM format with DEK-Info
openssl rsa -in client.key -text -noout -passin 'pass:'"$(tr -d '\n' < passphrase)" 