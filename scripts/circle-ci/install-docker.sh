#!/bin/bash

set -x

MIN_VERSION_WANTED=1.13.1
CURRENT_VERSION=$(docker version --format '{{.Client.Version}}')

if [ "$MIN_VERSION_WANTED" != "$(echo -e "$CURRENT_VERSION\n$MIN_VERSION_WANTED" | sort -V | head -n 1)" ]; then
    curl -fsSLO "https://get.docker.com/builds/Linux/x86_64/docker-$MIN_VERSION_WANTED.tgz" \
            && sudo tar --strip-components=1 -xvzf "docker-$MIN_VERSION_WANTED.tgz" -C /usr/bin \
            && rm "docker-$MIN_VERSION_WANTED.tgz"
fi
