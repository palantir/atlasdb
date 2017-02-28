#!/bin/bash

set -x

MIN_VERSION_WANTED=1.12.3
CURRENT_VERSION=$(docker version --format '{{.Client.Version}}')

if [ "$MIN_VERSION_WANTED" != "$(echo -e "$CURRENT_VERSION\n$MIN_VERSION_WANTED" | sort -V | head -n 1)" ]; then
#    curl -sSL https://s3.amazonaws.com/circle-downloads/install-circleci-docker.sh | bash -s -- "$MIN_VERSION_WANTED"
    curl -fsSLO "https://get.docker.com/builds/Linux/x86_64/docker-1.13.1.tgz" && sudo tar --strip-components=1 -xvzf docker-1.13.1.tgz -C /usr/bin
    rm docker-1.13.1.tgz
fi
