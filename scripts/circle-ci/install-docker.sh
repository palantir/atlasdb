#!/bin/bash

set -x

MIN_VERSION_WANTED=1.10.0
CURRENT_VERSION=$(docker version --format '{{.Client.Version}}')

if [ "$MIN_VERSION_WANTED" != "$(echo -e "$CURRENT_VERSION\n$MIN_VERSION_WANTED" | sort -V | head -n 1)" ]; then
    curl -sSL https://s3.amazonaws.com/circle-downloads/install-circleci-docker.sh | bash -s -- "$MIN_VERSION_WANTED"
fi
