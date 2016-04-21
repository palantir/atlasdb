#!/usr/bin/env bash

CASSANDRA=':atlasdb-cassandra-tests:check'
SHARED=':atlasdb-tests-shared:check'

case $CIRCLE_NODE_INDEX in
    0) ./gradlew --parallel check -x $CASSANDRA -x $SHARED ;;
    1) ./scripts/ete.sh;;
    2) ./gradlew --parallel $SHARED ;;
esac
