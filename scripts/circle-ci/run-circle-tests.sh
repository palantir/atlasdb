#!/usr/bin/env bash

set -x

BASE_GRADLE_ARGS="--profile --continue"

function checkDocsBuild {
    sudo apt-get update
    sudo apt-get install python-pip
    sudo -H pip install --upgrade sphinx sphinx_rtd_theme requests recommonmark
    cd docs/
    make html
}

CONTAINER_1=(':atlasdb-cassandra-integration-tests:check')

CONTAINER_2=(':atlasdb-ete-tests:check')

CONTAINER_3=(':atlasdb-dbkvs:check' ':atlasdb-cassandra:check' ':timelock-server:integTest')

CONTAINER_4=(':atlasdb-cassandra-multinode-tests:check' ':atlasdb-impl-shared:check' ':atlasdb-tests-shared:check' ':atlasdb-perf:check' ':atlasdb-ete-tests:dbkvsTest')

CONTAINER_5=(':lock-impl:check' ':atlasdb-dbkvs-tests:check' ':atlasdb-ete-test-utils:check' ':atlasdb-ete-tests:longTest')

CONTAINER_6=(':timelock-server:suiteTest')

CONTAINER_7=('compileJava' 'compileTestJava' ':atlasdb-refactorings:test')

# Container 0 - runs tasks not found in the below containers
CONTAINER_0_EXCLUDE=("${CONTAINER_1[@]}" "${CONTAINER_2[@]}" "${CONTAINER_3[@]}" "${CONTAINER_4[@]}" "${CONTAINER_5[@]}" "${CONTAINER_6[@]}")

for task in "${CONTAINER_0_EXCLUDE[@]}"
do
    CONTAINER_0_EXCLUDE_ARGS="$CONTAINER_0_EXCLUDE_ARGS -x $task"
done

# refaster runs with errorprone which patches the bootClasspath, need to ensure we run only on the errorprone container
CONTAINER_0_EXCLUDE_ARGS="$CONTAINER_0_EXCLUDE_ARGS -x :atlasdb-refactorings:test"

# Short circuit the build if it's docs only
if ./scripts/circle-ci/check-only-docs-changes.sh; then
    if [ $CIRCLE_NODE_INDEX -eq 0 ]; then
        checkDocsBuild
        exit $?
    fi
    exit 0
fi

JAVA_GC_LOGGING_OPTIONS="${JAVA_GC_LOGGING_OPTIONS} -XX:+HeapDumpOnOutOfMemoryError"
JAVA_GC_LOGGING_OPTIONS="${JAVA_GC_LOGGING_OPTIONS} -verbose:gc"
JAVA_GC_LOGGING_OPTIONS="${JAVA_GC_LOGGING_OPTIONS} -XX:+PrintGC"
JAVA_GC_LOGGING_OPTIONS="${JAVA_GC_LOGGING_OPTIONS} -XX:+PrintGCDateStamps"
JAVA_GC_LOGGING_OPTIONS="${JAVA_GC_LOGGING_OPTIONS} -XX:+PrintGCDetails"
JAVA_GC_LOGGING_OPTIONS="${JAVA_GC_LOGGING_OPTIONS} -XX:-TraceClassUnloading"
JAVA_GC_LOGGING_OPTIONS="${JAVA_GC_LOGGING_OPTIONS} -Xloggc:build-%t-%p.gc.log"

# External builds have a 16gb limit.
if [ "$CIRCLE_NODE_INDEX" -eq "7" ]; then
    export _JAVA_OPTIONS="-Xms2g -Xmx4g -XX:ActiveProcessorCount=8 ${JAVA_GC_LOGGING_OPTIONS}"
else
    BASE_GRADLE_ARGS+=" --parallel"
    export _JAVA_OPTIONS="-Xmx4g ${JAVA_GC_LOGGING_OPTIONS}"
    BASE_GRADLE_ARGS+=" --parallel"
fi
export CASSANDRA_MAX_HEAP_SIZE=512m
export CASSANDRA_HEAP_NEWSIZE=64m

case $CIRCLE_NODE_INDEX in
    0) ./gradlew $BASE_GRADLE_ARGS check $CONTAINER_0_EXCLUDE_ARGS -x :atlasdb-jepsen-tests:check;;
    1) ./gradlew $BASE_GRADLE_ARGS ${CONTAINER_1[@]} ;;
    2) ./gradlew $BASE_GRADLE_ARGS ${CONTAINER_2[@]} -x :atlasdb-ete-tests:longTest -x atlasdb-ete-tests:dbkvsTest ;;
    3) ./gradlew $BASE_GRADLE_ARGS ${CONTAINER_3[@]} ;;
    4) ./gradlew $BASE_GRADLE_ARGS ${CONTAINER_4[@]} ;;
    5) ./gradlew $BASE_GRADLE_ARGS ${CONTAINER_5[@]} ;;
    6) ./gradlew $BASE_GRADLE_ARGS ${CONTAINER_6[@]} ;;
    7) ./gradlew $BASE_GRADLE_ARGS ${CONTAINER_7[@]} --stacktrace -PenableErrorProne=true && checkDocsBuild ;;
esac
