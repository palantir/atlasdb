#!/bin/bash

set -x

CURRENT_VERSION=$(docker version --format '{{.Client.Version}}')

if [[ $INTERNAL_BUILD == true ]]; then
    MIN_VERSION_WANTED=1.13.1
else
    MIN_VERSION_WANTED=1.10.0
fi

if [ "$MIN_VERSION_WANTED" != "$(echo -e "$CURRENT_VERSION\n$MIN_VERSION_WANTED" | sort -V | head -n 1)" ]; then
    if [[ $INTERNAL_BUILD == true ]]; then
        curl -fsSLO "https://get.docker.com/builds/Linux/x86_64/docker-$MIN_VERSION_WANTED.tgz" \
                && sudo tar --strip-components=1 -xvzf "docker-$MIN_VERSION_WANTED.tgz" -C /usr/bin \
                && rm "docker-$MIN_VERSION_WANTED.tgz"
    else
        curl -sSL https://s3.amazonaws.com/circle-downloads/install-circleci-docker.sh | bash -s -- "$MIN_VERSION_WANTED"
    fi
fi
