#!/usr/bin/env bash

set -x

function checkDocsBuild {
  cd docs/
  make html
}

# Container 1
CONTAINER_1=(':atlasdb-cassandra-tests:check')

# Container 2
CONTAINER_2=(':atlasdb-tests-shared:check' ':atlasdb-ete-tests:check')

#Container 3
CONTAINER_3=(':atlasdb-timelock-ete:check' ':atlasdb-dropwizard-tests:check' ':lock-impl:check' ':atlasdb-dbkvs-tests:check')

# Container 0 - runs tasks not found in the below containers
CONTAINER_0_EXCLUDE=("${CONTAINER_1[@]}" "${CONTAINER_2[@]}" "${CONTAINER_3[@]}")

for task in "${CONTAINER_0_EXCLUDE[@]}"
do
    CONTAINER_0_EXCLUDE_ARGS="$CONTAINER_0_EXCLUDE_ARGS -x $task"
done

case $CIRCLE_NODE_INDEX in
    0) ./gradlew --profile --continue check $CONTAINER_0_EXCLUDE_ARGS ;;
    1) ./gradlew --continue --parallel ${CONTAINER_1[@]} ;;
    2) ./gradlew --continue --parallel ${CONTAINER_2[@]} ;;
    3) ./gradlew --profile --continue ${CONTAINER_3[@]} && checkDocsBuild ;;
esac
