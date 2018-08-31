#!/usr/bin/env bash

set -x

BASE_GRADLE_ARGS="--profile --continue"

function checkDocsBuild {
    sudo apt-get install python-pip
    sudo -H pip install --upgrade sphinx sphinx_rtd_theme requests recommonmark
    cd docs/
    make html
}

CONTAINER_1=(':atlasdb-cassandra-integration-tests:check' ':atlasdb-cassandra:check')

CONTAINER_2=(':atlasdb-ete-tests:check' ':atlasdb-ete-test-utils:check')

CONTAINER_3=(':atlasdb-dbkvs:check'  ':atlasdb-perf:postgresBenchmarkTest')

CONTAINER_4=(':atlasdb-cassandra-multinode-tests:check' ':atlasdb-impl-shared:check' ':atlasdb-cassandra-integration-tests:longTest' ':atlasdb-tests-shared:check' ':atlasdb-perf:check')

CONTAINER_5=(':atlasdb-cassandra-integration-tests:memorySensitiveTest' ':atlasdb-ete-tests:longTest' ':lock-impl:check' ':atlasdb-dbkvs-tests:check')

CONTAINER_6=(':atlasdb-ete-tests:startupIndependenceTest')

CONTAINER_7=('compileJava' 'compileTestJava')

# Container 0 - runs tasks not found in the below containers
CONTAINER_0_EXCLUDE=("${CONTAINER_1[@]}" "${CONTAINER_2[@]}" "${CONTAINER_3[@]}" "${CONTAINER_4[@]}" "${CONTAINER_5[@]}" "${CONTAINER_6[@]}")

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

JAVA_GC_LOGGING_OPTIONS="${JAVA_GC_LOGGING_OPTIONS} -XX:+HeapDumpOnOutOfMemoryError"
JAVA_GC_LOGGING_OPTIONS="${JAVA_GC_LOGGING_OPTIONS} -verbose:gc"
JAVA_GC_LOGGING_OPTIONS="${JAVA_GC_LOGGING_OPTIONS} -XX:+PrintGC"
JAVA_GC_LOGGING_OPTIONS="${JAVA_GC_LOGGING_OPTIONS} -XX:+PrintGCDateStamps"
JAVA_GC_LOGGING_OPTIONS="${JAVA_GC_LOGGING_OPTIONS} -XX:+PrintGCDetails"
JAVA_GC_LOGGING_OPTIONS="${JAVA_GC_LOGGING_OPTIONS} -XX:-TraceClassUnloading"
JAVA_GC_LOGGING_OPTIONS="${JAVA_GC_LOGGING_OPTIONS} -Xloggc:build-%t-%p.gc.log"

# External builds have a 4GB limit so we have to tune everything so it fits in memory (only just!)
if [[ $INTERNAL_BUILD == true ]]; then
    if [ "$CIRCLE_NODE_INDEX" -eq "7" ]; then
        export _JAVA_OPTIONS="-Xms2g -Xmx4g ${JAVA_GC_LOGGING_OPTIONS}"
    else
        BASE_GRADLE_ARGS+=" --parallel"
        export _JAVA_OPTIONS="-Xmx1024m ${JAVA_GC_LOGGING_OPTIONS}"
        BASE_GRADLE_ARGS+=" --parallel"
    fi
    export CASSANDRA_MAX_HEAP_SIZE=512m
    export CASSANDRA_HEAP_NEWSIZE=64m
else
    ./gradlew $BASE_GRADLE_ARGS --parallel compileJava compileTestJava
    export GRADLE_OPTS="-Xss1024K -XX:+CMSClassUnloadingEnabled -XX:InitialCodeCacheSize=32M -XX:CodeCacheExpansionSize=1M -XX:CodeCacheMinimumFreeSpace=1M -XX:ReservedCodeCacheSize=150M -XX:MinMetaspaceExpansion=1M -XX:MaxMetaspaceExpansion=8M -XX:MaxMetaspaceSize=128M -XX:MaxDirectMemorySize=96M -XX:CompressedClassSpaceSize=32M"
    export _JAVA_OPTIONS="${_JAVA_OPTIONS} ${JAVA_GC_LOGGING_OPTIONS}"
    export CASSANDRA_MAX_HEAP_SIZE=160m
    export CASSANDRA_HEAP_NEWSIZE=24m
fi

ETE_EXCLUDES=('-x :atlasdb-ete-tests:longTest')

if [[ $INTERNAL_BUILD != true ]]; then
    # Timelock and Startup ordering require Docker 1.12; currently unavailable on external Circle. Might not be needed
    # if we move to CircleCI 2.0.
    ETE_EXCLUDES+=('-x :atlasdb-ete-tests:timeLockTest')
    ETE_EXCLUDES+=('-x :atlasdb-ete-tests:startupIndependenceTest')

    # Sweep tests include a test that we do not OOM when writing a lot of data.
    # Running this test is not feasible on external Circle owing to small size of the containers, but it gives good
    # signal so it's worth keeping it around on internal runs.
    ETE_EXCLUDES+=('-x :atlasdb-cassandra-integration-tests:memorySensitiveTest')
fi

case $CIRCLE_NODE_INDEX in
    0) ./gradlew $BASE_GRADLE_ARGS check $CONTAINER_0_EXCLUDE_ARGS -x :atlasdb-jepsen-tests:jepsenTest ;;
    1) ./gradlew $BASE_GRADLE_ARGS ${CONTAINER_1[@]} ${ETE_EXCLUDES[@]} -x :atlasdb-cassandra-integration-tests:longTest ;;
    2) ./gradlew $BASE_GRADLE_ARGS ${CONTAINER_2[@]} ${ETE_EXCLUDES[@]} -x :atlasdb-ete-tests:startupIndependenceTest;;
    3) ./gradlew $BASE_GRADLE_ARGS ${CONTAINER_3[@]} ;;
    4) ./gradlew $BASE_GRADLE_ARGS ${CONTAINER_4[@]} ;;
    5) ./gradlew $BASE_GRADLE_ARGS ${CONTAINER_5[@]} -x :atlasdb-perf:postgresBenchmarkTest;;
    6) ./gradlew $BASE_GRADLE_ARGS ${CONTAINER_6[@]} ${ETE_EXCLUDES[@]} && checkDocsBuild ;;
    7) ./gradlew $BASE_GRADLE_ARGS ${CONTAINER_7[@]} -PenableErrorProne=true ;;
esac
