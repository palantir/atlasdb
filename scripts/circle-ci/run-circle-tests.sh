#!/usr/bin/env bash

set -x

BASE_GRADLE_ARGS="--profile --continue"

function checkDocsBuild {
    pyenv install 3.5.4
    pyenv global 3.5.4
    # these versions are quite old, but the docs all stop building on newer versions. Since the docs publish has now been failing for like 6 months, I'm trying to maintain the status quo.
    pip3 install sphinx==1.8.5 sphinx_rtd_theme==0.5.0 requests==2.24.0 recommonmark==0.6.0 pygments==2.5.2 markupsafe==1.1.1 jinja2==2.11.2 alabaster==0.7.12 babel==2.8.0 certifi==2020.6.20 sphinxcontrib-websupport==1.1.2 setuptools==44.1.1 typing==3.7.4.3 urllib3==1.25.10 snowballstemmer==2.0.0 six==1.15.0
    cd docs/
    make html
}

CONTAINER_1=(':atlasdb-cassandra-integration-tests:check')

CONTAINER_2=(':atlasdb-ete-tests:check')

CONTAINER_3=(':atlasdb-dbkvs:check' ':atlasdb-cassandra:check' ':timelock-server:integTest')

CONTAINER_4=(':atlasdb-cassandra-multinode-tests:check' ':atlasdb-impl-shared:check' ':atlasdb-tests-shared:check' ':atlasdb-perf:check' ':atlasdb-ete-tests:dbkvsTest')

CONTAINER_5=(':lock-impl:check' ':atlasdb-dbkvs-tests:postgresTest' ':atlasdb-ete-test-utils:check' ':atlasdb-ete-tests:longTest')

CONTAINER_6=(':timelock-server:suiteTest')

CONTAINER_7=(':timelock-server:stressTest')

CONTAINER_8=(':atlasdb-ete-tests:timeLockMigrationTest')

CONTAINER_9=(':atlasdb-ete-tests:oracleTest')

CONTAINER_10=('atlasdb-dbkvs-tests:oracleTest')

CONTAINER_11=('compileJava' 'compileTestJava')

# Container 0 - runs tasks not found in the below containers
CONTAINER_0_EXCLUDE=("${CONTAINER_1[@]}" "${CONTAINER_2[@]}" "${CONTAINER_3[@]}" "${CONTAINER_4[@]}" "${CONTAINER_5[@]}" "${CONTAINER_6[@]}" "${CONTAINER_7[@]}" "${CONTAINER_8[@]}" "${CONTAINER_9[@]}" "${CONTAINER_10[@]}")

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

if [ $CIRCLE_NODE_INDEX -eq 9 ] || [ $CIRCLE_NODE_INDEX -eq 10 ]; then
    printenv DOCKERHUB_PASSWORD | docker login --username "$DOCKERHUB_USERNAME" --password-stdin
    # The oracle container is very large and takes a long time to pull.
    # If this image is not pulled here, the circle build usually times out.
    docker-compose -f atlasdb-dbkvs-tests/src/test/resources/docker-compose.oracle.yml pull oracle
fi

JAVA_GC_LOGGING_OPTIONS="${JAVA_GC_LOGGING_OPTIONS} -XX:+HeapDumpOnOutOfMemoryError"
JAVA_GC_LOGGING_OPTIONS="${JAVA_GC_LOGGING_OPTIONS} -verbose:gc"
JAVA_GC_LOGGING_OPTIONS="${JAVA_GC_LOGGING_OPTIONS} -XX:+PrintGC"
JAVA_GC_LOGGING_OPTIONS="${JAVA_GC_LOGGING_OPTIONS} -XX:+PrintGCDateStamps"
JAVA_GC_LOGGING_OPTIONS="${JAVA_GC_LOGGING_OPTIONS} -XX:+PrintGCDetails"
JAVA_GC_LOGGING_OPTIONS="${JAVA_GC_LOGGING_OPTIONS} -XX:-TraceClassUnloading"
JAVA_GC_LOGGING_OPTIONS="${JAVA_GC_LOGGING_OPTIONS} -Xloggc:build-%t-%p.gc.log"

# External builds have a 16gb limit.
if [ "$CIRCLE_NODE_INDEX" -eq "11" ]; then
    export _JAVA_OPTIONS="-Xms2g -Xmx4g -XX:ActiveProcessorCount=8 ${JAVA_GC_LOGGING_OPTIONS}"
else
    export _JAVA_OPTIONS="-Xmx4g ${JAVA_GC_LOGGING_OPTIONS}"
    BASE_GRADLE_ARGS+=" --parallel"
fi
export CASSANDRA_MAX_HEAP_SIZE=512m
export CASSANDRA_HEAP_NEWSIZE=64m

case $CIRCLE_NODE_INDEX in
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
    11) ./gradlew $BASE_GRADLE_ARGS ${CONTAINER_11[@]} --stacktrace -PenableErrorProne=true && checkDocsBuild ;;
esac
