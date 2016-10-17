#!/usr/bin/env bash

set -x

function checkDocsBuild {
  cd docs/
  make html
}

CONTAINER_1=(':atlasdb-cassandra-integration-tests:check')

CONTAINER_2=(':atlasdb-ete-tests:check')

CONTAINER_3=(':atlasdb-timelock-ete:check' ':lock-impl:check' ':atlasdb-dbkvs-tests:check' ':atlasdb-tests-shared:check' ':atlasdb-ete-test-utils:check' ':atlasdb-cassandra:check' ':atlasdb-api:check')

CONTAINER_4=(':atlasdb-dbkvs:check' ':atlasdb-cassandra-multinode-tests:check' ':atlasdb-impl-shared:check' ':atlasdb-dropwizard-bundle:check')

CONTAINER_5=(':atlasdb-ete-tests:longTest')

# Container 0 - runs tasks not found in the below containers
CONTAINER_0_EXCLUDE=("${CONTAINER_1[@]}" "${CONTAINER_2[@]}" "${CONTAINER_3[@]}" "${CONTAINER_4[@]}" "${CONTAINER_5[@]}")

for task in "${CONTAINER_0_EXCLUDE[@]}"
do
    CONTAINER_0_EXCLUDE_ARGS="$CONTAINER_0_EXCLUDE_ARGS -x $task"
done

case $CIRCLE_NODE_INDEX in
    0) ./gradlew --profile --continue -x findbugsMain -x findbugsTest -x checkstyleMain -x checkstyleTest check $CONTAINER_0_EXCLUDE_ARGS ;;
    1) ./gradlew --profile --continue -x findbugsMain -x findbugsTest -x checkstyleMain -x checkstyleTest ${CONTAINER_1[@]} ;;
    2) ./gradlew --profile --continue -x findbugsMain -x findbugsTest -x checkstyleMain -x checkstyleTest ${CONTAINER_2[@]} -x :atlasdb-ete-tests:longTest ;;
    3) ./gradlew --profile --continue -x findbugsMain -x findbugsTest -x checkstyleMain -x checkstyleTest ${CONTAINER_3[@]} ;;
    4) ./gradlew --profile --continue -x findbugsMain -x findbugsTest -x checkstyleMain -x checkstyleTest ${CONTAINER_4[@]} ;;
    5) ./gradlew --profile --continue -x findbugsMain -x findbugsTest -x checkstyleMain -x checkstyleTest ${CONTAINER_5[@]} ;;
    6) ./gradlew --profile --continue findbugsMain findbugsTest checkstyleMain checkstyleTest && checkDocsBuild ;;
esac
