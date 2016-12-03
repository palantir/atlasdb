#!/usr/bin/env bash

set -x

TEST_CONTAINER_ARGS="--profile --continue -x compileJava -x compileTestJava -x findbugsMain -x findbugsTest -x checkstyleMain -x checkstyleTest"

function checkDocsBuild {
  cd docs/
  make html
}

CONTAINER_1=(':atlasdb-cassandra-integration-tests:check')

CONTAINER_2=(':atlasdb-ete-tests:check')

CONTAINER_3=(':atlasdb-ete-test-utils:check' ':atlasdb-cassandra:check' ':atlasdb-api:check' ':atlasdb-jepsen-tests:check' ':atlasdb-cassandra-integration-tests:longTest')

CONTAINER_4=(':atlasdb-dbkvs:check' ':atlasdb-cassandra-multinode-tests:check' ':atlasdb-impl-shared:check' ':atlasdb-dropwizard-bundle:check')

CONTAINER_5=(':atlasdb-ete-tests:longTest' ':lock-impl:check' ':atlasdb-dbkvs-tests:check' ':atlasdb-tests-shared:check')

# Container 0 - runs tasks not found in the below containers
CONTAINER_0_EXCLUDE=("${CONTAINER_1[@]}" "${CONTAINER_2[@]}" "${CONTAINER_3[@]}" "${CONTAINER_4[@]}" "${CONTAINER_5[@]}")

for task in "${CONTAINER_0_EXCLUDE[@]}"
do
    CONTAINER_0_EXCLUDE_ARGS="$CONTAINER_0_EXCLUDE_ARGS -x $task"
done

# Short circuit the build if it's docs only
if ./scripts/circle-ci/check-only-docs-changes.sh; then
    if [ $CIRCLE_NODE_INDEX -eq 0 ]; then
        checkDocsBuild
        exit $?
    fi
    exit 0
fi

case $CIRCLE_NODE_INDEX in
    0) ./gradlew $TEST_CONTAINER_ARGS check $CONTAINER_0_EXCLUDE_ARGS ;;
    1) ./gradlew $TEST_CONTAINER_ARGS ${CONTAINER_1[@]} -x :atlasdb-cassandra-integration-tests:longTest ;;
    2) ./gradlew $TEST_CONTAINER_ARGS ${CONTAINER_2[@]} -x :atlasdb-ete-tests:longTest ;;
    3) ./gradlew $TEST_CONTAINER_ARGS ${CONTAINER_3[@]} -x :atlasdb-jepsen-tests:jepsenTest ;;
    4) ./gradlew $TEST_CONTAINER_ARGS ${CONTAINER_4[@]} ;;
    5) ./gradlew $TEST_CONTAINER_ARGS ${CONTAINER_5[@]} ;;
    6) ./gradlew --profile --continue -x compileJava -x compileTestJava findbugsMain findbugsTest checkstyleMain checkstyleTest && checkDocsBuild ;;
esac
