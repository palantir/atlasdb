#!/bin/bash

set -e
cd $(dirname $0)/..

./gradlew docker --parallel --console=plain

cd docker-containers 

function on_exit() {
    docker-compose stop -t 1
}

docker-compose rm -f
docker-compose build
docker-compose up -t 0 --no-build -d 
trap "on_exit" EXIT
docker-compose logs &
docker-compose run ete ./gradlew atlasdb-cassandra-tests:test --console=plain --info --continue 
