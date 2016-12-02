#!/usr/bin/env sh
set -e

cd "${BASH_SOURCE%/*}" || exit

# wait for docker to switch on
until docker ps; do sleep 1; done

docker-compose -f common-containers.yml pull --ignore-pull-failures
