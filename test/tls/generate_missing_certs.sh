#!/bin/bash
set -ex

cd test/tls

# Generate server certificate from existing CSR
cd ca
openssl genrsa -aes256 -passout file:passphrase -out server.key 4096
openssl x509 -req -days 365 -in server.csr -signkey server.key -passin file:passphrase -out server.crt

# Generate client certificate from existing CSR
cd ../client
openssl x509 -req -days 365 -in client.csr -CA ../ca/server.crt -CAkey ../ca/server.key -passin file:../ca/passphrase -CAcreateserial -out client.crt

# Decrypt client key for curl (using existing key)
openssl rsa -in client.key -passin file:passphrase -out client_decrypted.key 