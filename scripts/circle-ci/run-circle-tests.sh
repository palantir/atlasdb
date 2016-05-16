#!/usr/bin/env bash

set -x

function checkDocsBuild {
  cd docs/
  make html
}

CASSANDRA=':atlasdb-cassandra-tests:check'
SHARED=':atlasdb-tests-shared:check'

case $CIRCLE_NODE_INDEX in
    0) ./gradlew --continue check -x $CASSANDRA -x $SHARED ;;
    1) ./gradlew --continue --parallel $CASSANDRA --console=plain ;;
    2) ./gradlew --continue --parallel $SHARED && checkDocsBuild ;;
esac
