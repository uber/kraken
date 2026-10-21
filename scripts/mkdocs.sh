#!/bin/bash

set -e
cd $(dirname "${BASH_SOURCE[0]}")/..

# Setup virtualenv if not exists
if [ ! -d env ]; then
    virtualenv env
fi

# Active virtualenv
. env/bin/activate

# Install pip requirements for the deployment script
pip install -q -r requirements-docs.txt

# Generate docs/index.md file automatically from README.md
sed -e 's|(docs/|(|g' \
    -e 's|(examples/|(https://github.com/uber/kraken/blob/master/examples/|g' \
    README.md > docs/index.md
cp assets/architecture-dark.svg docs/assets/

# Run the mkdocs tool
echo "cmd is $@"
mkdocs "$@"

# Exit virtualenv
deactivate
