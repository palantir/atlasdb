#!/usr/bin/env bash

set -x

BASE_GRADLE_ARGS="--profile"


CONTAINER_1=(':atlasdb-cassandra-integration-tests:testSubset1')

CONTAINER_2=(':atlasdb-ete-tests:check')

CONTAINER_3=(':timelock-server:integTest')

CONTAINER_4=(':atlasdb-cassandra-multinode-tests:check' ':atlasdb-tests-shared:check' ':atlasdb-perf:check' ':atlasdb-ete-tests:dbkvsTest')

CONTAINER_5=(':lock-impl:check' ':atlasdb-dbkvs-tests:postgresTest' ':atlasdb-ete-test-utils:check' ':atlasdb-ete-tests:longTest')

CONTAINER_6=(':timelock-server:suiteTest')

CONTAINER_7=(':timelock-server:stressTest')

CONTAINER_8=(':atlasdb-cassandra-integration-tests:testSubset2')

CONTAINER_9=(':atlasdb-ete-tests:oracleTest')

CONTAINER_10=('atlasdb-dbkvs-tests:oracleTest')

CONTAINER_11=(':atlasdb-impl-shared:check' )

CONTAINER_12=(':atlasdb-dbkvs:check' ':atlasdb-cassandra:check' )

CONTAINER_13=(':atlasdb-ete-tests:timeLockMigrationTest')

# Excluded as it is split into two subsets
EXCLUDED=(':atlasdb-cassandra-integration-tests:check')

# Container 0 - runs tasks not found in the below containers
CONTAINER_0_EXCLUDE=("${CONTAINER_1[@]}" "${CONTAINER_2[@]}" "${CONTAINER_3[@]}" "${CONTAINER_4[@]}" "${CONTAINER_5[@]}" "${CONTAINER_6[@]}" "${CONTAINER_7[@]}" "${CONTAINER_8[@]}" "${CONTAINER_9[@]}" "${CONTAINER_10[@]}" "${CONTAINER_11[@]}" "${CONTAINER_12[@]}" "${CONTAINER_13[@]}" "${EXCLUDED[@]}")

for task in "${CONTAINER_0_EXCLUDE[@]}"
do
    CONTAINER_0_EXCLUDE_ARGS="$CONTAINER_0_EXCLUDE_ARGS -x $task"
done

test_suite_index=$1

# Short circuit the build if it's docs only
if ./scripts/circle-ci/check-only-docs-changes.sh; then
    if [ "$test_suite_index" -eq 0 ]; then
        ./scripts/circle-ci/check-docs-build.sh
        exit $?
    fi
    exit 0
fi

if [ "$test_suite_index" -eq 9 ] || [ "$test_suite_index" -eq 10 ]; then
    printenv DOCKERHUB_PASSWORD | docker login --username "$DOCKERHUB_USERNAME" --password-stdin
    # The oracle container is very large and takes a long time to pull.
    # If this image is not pulled here, the circle build usually times out.
    docker-compose -f atlasdb-dbkvs-tests/src/test/resources/docker-compose.oracle.yml pull oracle
fi

JAVA_GC_LOGGING_OPTIONS="${JAVA_GC_LOGGING_OPTIONS} -XX:+HeapDumpOnOutOfMemoryError"
JAVA_GC_LOGGING_OPTIONS="${JAVA_GC_LOGGING_OPTIONS} -Xlog:class+unload=off"
JAVA_GC_LOGGING_OPTIONS="${JAVA_GC_LOGGING_OPTIONS} -Xlog:gc:build-%t-%p.gc.log"

if [ "$test_suite_index" -eq "3" ]; then
    export _JAVA_OPTIONS="-Xms8g -Xmx8g -XX:ActiveProcessorCount=8 ${JAVA_GC_LOGGING_OPTIONS}"
    BASE_GRADLE_ARGS+=" --scan --parallel"
else
    export _JAVA_OPTIONS="-Xms4g -Xmx4g ${JAVA_GC_LOGGING_OPTIONS}"
    BASE_GRADLE_ARGS+=" --scan --parallel"
fi
export CASSANDRA_MAX_HEAP_SIZE=512m
export CASSANDRA_HEAP_NEWSIZE=64m

case "$test_suite_index" in
    0) ./gradlew $BASE_GRADLE_ARGS check $CONTAINER_0_EXCLUDE_ARGS -x :atlasdb-jepsen-tests:check;;
    1) ./gradlew $BASE_GRADLE_ARGS ${CONTAINER_1[@]} ;;
    2) ./gradlew $BASE_GRADLE_ARGS ${CONTAINER_2[@]} -x :atlasdb-ete-tests:longTest -x atlasdb-ete-tests:dbkvsTest -x :atlasdb-ete-tests:timeLockMigrationTest ;;
    3) ./gradlew $BASE_GRADLE_ARGS ${CONTAINER_3[@]} ;;
    4) ./gradlew $BASE_GRADLE_ARGS ${CONTAINER_4[@]} ;;
    5) ./gradlew $BASE_GRADLE_ARGS ${CONTAINER_5[@]} ;;
    6) ./gradlew $BASE_GRADLE_ARGS ${CONTAINER_6[@]} ;;
    7) ./gradlew $BASE_GRADLE_ARGS ${CONTAINER_7[@]} ;;
    8) ./gradlew $BASE_GRADLE_ARGS ${CONTAINER_8[@]} ;;
    9) ./gradlew $BASE_GRADLE_ARGS ${CONTAINER_9[@]} ;;
    10) ./gradlew $BASE_GRADLE_ARGS ${CONTAINER_10[@]} ;;
    11) ./gradlew $BASE_GRADLE_ARGS ${CONTAINER_11[@]} ;;
    12) ./gradlew $BASE_GRADLE_ARGS ${CONTAINER_12[@]} ;;
    13) ./gradlew $BASE_GRADLE_ARGS ${CONTAINER_13[@]} ;;
esac
