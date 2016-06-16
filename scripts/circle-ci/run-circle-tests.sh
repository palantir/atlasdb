#!/usr/bin/env bash

set -x

function checkDocsBuild {
  cd docs/
  make html
}

CASSANDRA=':atlasdb-cassandra-tests:check'
SHARED=':atlasdb-tests-shared:check'
ETE=':atlasdb-ete-tests:check'

case $CIRCLE_NODE_INDEX in
    0) ./gradlew --profile --continue check -x $CASSANDRA -x $SHARED -x $ETE ;;
    1) ./gradlew --continue --parallel $CASSANDRA ;;
    2) ./gradlew --continue --parallel $SHARED $ETE && checkDocsBuild ;;
esac
