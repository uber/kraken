#!/bin/bash
# shellcheck disable=SC1003

# This script will extract TLS certs stored in vault and write them out to
# the output path specific. Certs are simply stored as strings, so this
# this script will extract them from the secrets.json file, convert the \n into
# actual newlines, and then write out the appropriate files.

# If vault PKI is available, that is a better mechanism for storing certs

set -eu

if ! command -v jq > /dev/null; then
  echo "ERROR: You must install jq for this script to to work!"
  echo "       Go to https://stedolan.github.io/jq and follow the instructions for your distro"
  exit 1
fi

function usage() {
  echo "Usage:"
  echo "  build_certs_from_vault.sh secretsFile outputDir serverCertPath serverKeyPath clientCertPath clientKeyPath"
  echo ""
  echo "Required arguments:"
  echo "  secretsFile:   Path to the secrets.json"
  echo "  outputDir:     Directory where all .crt/.key files will be written"
  echo "  serviceFilter: Top-level filter, in jq format. This indicates the root path"
  echo "                 where all cert/key data resides. It should take the form"
  echo "                 'amazon/app/kraken/dev/tls'. Client and server crt/key"
	echo "                 data will be sub-paths under here in the form of"
	echo "                 <root path>/[server|client]"
	echo ""
}

function createFile() {
	local path=$1
	local key=$2
	local output=$3

	# Pull the secret and convert '\n' into newlines
	jq ".[\"$path\"].$key" "$secretsFile" | sed 's/"//g;s/\\n/\'$'\n''/g' > "$output"
	if grep "null" "$output" > /dev/null; then
		echo "WARN: No vault entry for $path:$key found!"
		rm "$output"
		return
	fi
	chmod 600 "$output"
}

if [[ $# -ne 3 ]]; then
  usage;
  exit 1
fi

secretsFile=$1
outputPath=$2

# Remove any trailing /
rootPath=$(echo "$3" | sed 's/\/$//')

serverPath="$rootPath/server"
clientPath="$rootPath/client"

mkdir -p "$outputPath"
createFile "$serverPath" "crt" "$outputPath/server.crt"
createFile "$serverPath" "key" "$outputPath/server.key"
createFile "$clientPath" "crt" "$outputPath/client.crt"
createFile "$clientPath" "key"  "$outputPath/client.key"
