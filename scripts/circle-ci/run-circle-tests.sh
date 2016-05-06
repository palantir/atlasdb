#!/usr/bin/env bash

set -x

CASSANDRA=':atlasdb-cassandra-tests:check'
SHARED=':atlasdb-tests-shared:check'

case $CIRCLE_NODE_INDEX in
    0) ./gradlew --continue check -x $CASSANDRA -x $SHARED ;;
    1) ./gradlew docker $CASSANDRA --parallel --console=plain --continue ;;
    2) ./gradlew --parallel $SHARED && cd docs/; make html ;;
esac
