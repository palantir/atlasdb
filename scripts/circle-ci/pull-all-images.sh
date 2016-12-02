#!/usr/bin/env sh

pull_images() {
    docker-compose -f scripts/circle-ci/common-containers.yml pull
}

while ! pull_images; do
    # Don't take up all cpu cycles if something fails
    sleep 1
done
