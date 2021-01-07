#!/usr/bin/env bash
# shellcheck disable=SC2207,SC2086

# This script will read in the secrets.json, and then builds the Kraken
# secrets.yaml file in the correct format. Note that this script assumes
# the secrets.json file takes the following form:
#
# {
#  "amazon/app/kraken/dev/origin/auth/s3/svc-kraken/s3/aws_access_key_id": {
#    "aws_access_key_id": "A_TOTALLY_REAL_KEY"
#  },
#  "amazon/app/kraken/dev/origin/auth/s3/svc-kraken/s3/aws_secret_access_key": {
#    "aws_secret_access_key": "SOME_OTHER_DEFINITELY_REAL_KEY"
#  }
# }

set -eu

if ! command -v yq > /dev/null; then
  echo "ERROR: You must install yq for this script to to work!"
  echo "       Go to https://github.com/mikefarah/yq and follow the instructions for your distro"
  exit 1
fi

if ! command -v jq > /dev/null; then
  echo "ERROR: You must install jq for this script to to work!"
  echo "       Go to https://stedolan.github.io/jq and follow the instructions for your distro"
  exit 1
fi

if command -v pip > /dev/null && pip show yq > /dev/null; then
  echo "ERROR: The Python version of yq installed by pip is not compatible with this script."
  echo "       Please uninstall the Python version and the go to https://github.com/mikefarah/yq"
  echo "       and follow the instructions for your distro"
  exit 1
fi

function usage() {
  echo "Usage:"
  echo "  build_secrets_from_vault.sh secretsFile serviceFilter"
  echo ""
  echo "Required arguments:"
  echo "  secretsFile:   Path to the secrets.json"
  echo "  outputDir:     Directory where secrets.yaml will be written"
  echo "  serviceFilter: Top-level filter, in jq format. This is used to remove the vault-specific"
  echo "                 keys from the final yaml, leaving only Kraken-specific keys"
  echo "                 Example: 'amazon/app/kraken/dev/origin'"
  echo ""
}

function indent() {
    local string="$1"
    local num_spaces="$2"
    printf "%${num_spaces}s%s\n" '' "$string"
}

function convertVaultToYAML() {
  local pathElements=($(echo "$1" | sed -e 's/\// /g'))
  local length=${#pathElements[@]}
  local depth=$(( 0 ))
  local value=""
  for elem in ${pathElements[*]}; do
      indent "${elem}:${value}" $(( depth++ )) 1
      if (( depth == length - 1 )); then
        value=" $2"
      fi
  done
}

if [[ $# -ne 3 ]]; then
  usage;
  exit 1
fi

secretsFile=$1
outputDir=$2
# Convert jq format to yq
serviceFilter=$( echo "$3" | sed -e "s/\//\./g" )

# Read in secrets.json and get each path. Note that for each path, the last
# element is also the key, e.g if path=aws/kraken/auth/sql/user then key=user
# We also filter out any paths with "/tls/" as they are part of the TLS certs
# and are handled separately.
set +e
paths=($(jq 'keys|.[]' "$secretsFile" | grep -v '/tls/'))
set -e
# For each path, get the value and convert to a simple yaml where each element

yamlFiles=()
count=$(( 0 ))
for path in ${paths[*]}; do
  path=${path//\"/}
  keys=($(jq ".[\"$path\"]|keys|.[]" "$secretsFile"))
  for key in ${keys[*]}; do
    value=$(jq ".[\"$path\"].$key" "$secretsFile" | sed 's/"//g')
    yamlFile="$outputDir/$(( count++ )).yaml"
    convertVaultToYAML "$path/$key" "$value" > "$yamlFile"
    yamlFiles+=("$yamlFile")
  done
done

# if no files, we're done
if [[ ${#yamlFiles[*]} == 0 ]]; then
  exit
fi

# Now merge them all into one
yq merge ${yamlFiles[*]} > "$outputDir/merged.yaml"

# Extract the correct sub-keys
yq read "$outputDir/merged.yaml" "$serviceFilter" > "$outputDir/secrets.yaml"

if grep "null" "$outputDir/secrets.yaml" > /dev/null; then
  rm "$outputDir/secrets.yaml"
fi

# Clean up
rm ${yamlFiles[*]} "$outputDir/merged.yaml"
