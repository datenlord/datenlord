#!/bin/bash

source scripts/config.sh

# Check if kind is installed
if command -v kind &> /dev/null; then
    # Extract the version number from kind version's output
    CURRENT_KIND_VERSION=$(kind --version | awk '{print $3}')

    # Compare the extracted version with the expected version
    if [ "$CURRENT_KIND_VERSION" != "$KIND_VERSION" ]; then
        echo "kind version mismatch, installing $KIND_VERSION"
        sudo rm -f `which kind`
    else
        echo "kind already installed with correct version"
        exit 0
    fi
else
    echo "kind is not installed"
fi

# Install or update kind
echo "Installing kind"
curl -Lo ./kind https://kind.sigs.k8s.io/dl/v$KIND_VERSION/kind-linux-amd64
chmod +x ./kind
sudo mv ./kind /usr/local/bin/kind