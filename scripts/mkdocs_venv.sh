#!/bin/bash

set -e
cd $(dirname "${BASH_SOURCE[0]}")/..

# Setup virtualenv if not exists
if [ ! -d venv ]; then
    python3 -m venv venv
fi

# Active virtualenv
. venv/bin/activate

# Install pip requirements for the deployment script
pip install -q -r requirements-docs.txt

# Run the mkdocs tool
echo "cmd is $@"
mkdocs "$@"

# Exit virtualenv
deactivate
