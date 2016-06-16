#!/usr/bin/env bash

set -x

function checkDocsBuild {
  cd docs/
  make html
}

# Container 0 - runs tasks not found in the below containers

# Container 1
CASSANDRA=':atlasdb-cassandra-tests:check'

# Container 2
SHARED=':atlasdb-tests-shared:check'
ETE=':atlasdb-ete-tests:check'

#Container 3
TIMELOCK_ETE=':atlasdb-timelock-ete:check'
DROPWIZARD=':atlasdb-dropwizard-tests:check'
LOCK=':lock-impl:check'

EXCLUDE=($CASSANDRA $SHARED $ETE $TIMELOCK_ETE $DROPWIZARD $LOCK)

for task in "${EXCLUDE[@]}"
do
    EXCLUDE_ARGS="$EXCLUDE_ARGS -x $task"
done

case $CIRCLE_NODE_INDEX in
    0) ./gradlew --profile --continue check $EXCLUDE_ARGS ;;
    1) ./gradlew --continue --parallel $CASSANDRA ;;
    2) ./gradlew --continue --parallel $SHARED $ETE ;;
    3) ./gradlew --continue --parallel $TIMELOCK_ETE $DROPWIZARD $LOCK && checkDocsBuild ;;
esac
