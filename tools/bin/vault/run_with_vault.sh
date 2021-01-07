#!/usr/bin/env bash
# shellcheck disable=SC2086

set -eu

function usage() {
  echo "Usage:"
  echo "  run_with_vault.sh secretsFile outputDir serviceFilter krakenCmd [krakenArgs ...]"
  echo ""
  echo "Required arguments:"
  echo "  secretsFile:   Path to the secrets.json"
  echo "  outputDir:     Directory where secrets.yaml and .crt/.key files will be written."
  echo "                 Cert files will be under a subdirectory 'tls'"
  echo "  serviceFilter: Top-level filter, in jq format. This indicates the root path"
  echo "                 where all secrets data resides. It should take the form"
  echo "                 'amazon/app/kraken/dev/origin"
  echo "  krakenCmd:     Kraken command to execute"
  echo "  krakenArgs:    List of arguments to pass to the Kraken command"
  echo ""
}

if [[ $# -lt 4 ]]; then
  usage;
  exit 1
fi

secretsFile=$1
outputDir=$2
serviceFilter=$3
krakenCmd=$4
shift 4
krakenArgs=( "$@" )

mkdir -p "$outputDir"

# Find script location so we can run the helper scripts
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

# First, generate the Kraken secrets.yaml from the Vault secrets.json.
$DIR/build_secrets_from_vault.sh "$secretsFile" "$outputDir" "$serviceFilter"

# Next, generate the TLS certs from Vault
# Certs are common across services and stored under /tls the evironment root path,
# so slice out the service name from the filter and add tls
tlsFilter=$(dirname "$serviceFilter")/tls
$DIR/build_certs_from_vault.sh "$secretsFile" "$outputDir/tls" "$tlsFilter"

# Finally, run Kraken
$krakenCmd ${krakenArgs[*]}
