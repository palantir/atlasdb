#!/usr/bin/env bash

set -x

BASE_GRADLE_ARGS="--profile --continue"
# TEST_CONTAINER_ARGS="-x findbugsMain -x findbugsTest -x checkstyleMain -x checkstyleTest"

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

if [[ $INTERNAL_BUILD != true ]]; then
    ./gradlew $BASE_GRADLE_ARGS --parallel compileJava compileTestJava
    BASE_GRADLE_ARGS+=" -x compileJava -x compileTestJava"
    GRADLE_OPTS="-Xss1024K -XX:+CMSClassUnloadingEnabled -XX:InitialCodeCacheSize=32M -XX:CodeCacheExpansionSize=1M -XX:CodeCacheMinimumFreeSpace=1M -XX:ReservedCodeCacheSize=150M -XX:MinMetaspaceExpansion=1M -XX:MaxMetaspaceExpansion=8M -XX:MaxMetaspaceSize=128M -XX:MaxDirectMemorySize=96M -XX:CompressedClassSpaceSize=32M"
else
    BASE_GRADLE_ARGS+=" --parallel"
    _JAVA_OPTIONS="-Xmx1024m"
fi

case $CIRCLE_NODE_INDEX in
    0) ./gradlew $BASE_GRADLE_ARGS $TEST_CONTAINER_ARGS check $CONTAINER_0_EXCLUDE_ARGS ;;
    1) ./gradlew $BASE_GRADLE_ARGS $TEST_CONTAINER_ARGS ${CONTAINER_1[@]} -x :atlasdb-cassandra-integration-tests:longTest ;;
    2) ./gradlew $BASE_GRADLE_ARGS $TEST_CONTAINER_ARGS ${CONTAINER_2[@]} -x :atlasdb-ete-tests:longTest ;;
    3) ./gradlew $BASE_GRADLE_ARGS $TEST_CONTAINER_ARGS ${CONTAINER_3[@]} -x :atlasdb-jepsen-tests:jepsenTest ;;
    4) ./gradlew $BASE_GRADLE_ARGS $TEST_CONTAINER_ARGS ${CONTAINER_4[@]} ;;
    5) ./gradlew $BASE_GRADLE_ARGS $TEST_CONTAINER_ARGS ${CONTAINER_5[@]} ;;
    #6) ./gradlew $BASE_GRADLE_ARGS findbugsMain findbugsTest checkstyleMain checkstyleTest && checkDocsBuild ;;
esac
