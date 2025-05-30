#!/bin/bash
set -ex

# Get the script's directory
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# Clean up any existing files
rm -f "$SCRIPT_DIR"/ca/*.{key,crt,csr,srl} "$SCRIPT_DIR"/client/*.{key,crt,csr}

# Ensure directories exist
mkdir -p "$SCRIPT_DIR"/{ca,client}

# Generate CA/server certificates
cd "$SCRIPT_DIR"/ca
echo "password" > passphrase
openssl genrsa -aes256 -passout file:passphrase -out server.key 4096
openssl req -new -key server.key -passin file:passphrase -out server.csr -subj "/C=UC/ST=CA/L=San Francisco/O=Uber/OU=cluster-mgmt/CN=kraken"
openssl x509 -req -days 3650 -in server.csr -signkey server.key -passin file:passphrase -out server.crt

# Generate client certificates
cd "$SCRIPT_DIR"/client
echo "password" > passphrase
openssl genrsa -aes256 -passout file:passphrase -out client.key 4096
openssl req -new -key client.key -passin file:passphrase -out client.csr -subj "/C=US/ST=CA/L=San Francisco/O=Uber/OU=kraken/CN=kraken"
openssl x509 -req -days 3650 -in client.csr -CA ../ca/server.crt -CAkey ../ca/server.key -passin file:../ca/passphrase -CAcreateserial -out client.crt

# Decrypt client key for curl/testing
openssl rsa -in client.key -passin file:passphrase -out client_decrypted.key

# Set proper permissions
chmod 644 "$SCRIPT_DIR"/ca/{server.key,server.crt,server.csr,passphrase}
chmod 644 "$SCRIPT_DIR"/client/{client.key,client.crt,client.csr,client_decrypted.key,passphrase}

echo "Certificates have been regenerated successfully!"
echo "CA/Server certificates in ca/:"
ls -l "$SCRIPT_DIR"/ca/
echo -e "\nClient certificates in client/:"
ls -l "$SCRIPT_DIR"/client/

echo -e "\nCertificates are ready to be mounted into containers at /etc/kraken/tls/" 