#!/usr/bin/env bash

set -x

BASE_GRADLE_ARGS="--profile --continue"

function checkDocsBuild {
     pyenv install 3.5.4
     pyenv global 3.5.4
     # these versions are quite old, but the docs all stop building on newer versions. Since the docs publish has now been failing for like 6 months, I'm trying to maintain the status quo.
     pip3 install sphinx==3.5.4 sphinx_rtd_theme==0.5.0 requests==2.24.0 recommonmark==0.6.0 pygments==2.5.2 markupsafe==1.1.1 jinja2==2.11.2 alabaster==0.7.12 babel==2.8.0 certifi==2020.6.20 sphinxcontrib-websupport==1.1.2 setuptools==44.1.1 typing==3.7.4.3 urllib3==1.25.10 snowballstemmer==2.0.0 six==1.15.0
     cd docs/
     make html
}

CONTAINER_1=(":atlasdb-ete-tests:test --tests *shouldSweepStreamIndices")

CONTAINER_11=('compileJava' 'compileTestJava')

JAVA_GC_LOGGING_OPTIONS="${JAVA_GC_LOGGING_OPTIONS} -XX:+HeapDumpOnOutOfMemoryError"
JAVA_GC_LOGGING_OPTIONS="${JAVA_GC_LOGGING_OPTIONS} -Xlog:class+unload=off"
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
    1) ./gradlew $BASE_GRADLE_ARGS ${CONTAINER_1[@]} ;;
    2) ./gradlew $BASE_GRADLE_ARGS ${CONTAINER_11[@]} ;;
    3) ./gradlew $BASE_GRADLE_ARGS ${CONTAINER_11[@]} ;;
    4) ./gradlew $BASE_GRADLE_ARGS ${CONTAINER_11[@]} ;;
    5) ./gradlew $BASE_GRADLE_ARGS ${CONTAINER_11[@]} ;;
    6) ./gradlew $BASE_GRADLE_ARGS ${CONTAINER_11[@]} ;;
    7) ./gradlew $BASE_GRADLE_ARGS ${CONTAINER_11[@]} ;;
    8) ./gradlew $BASE_GRADLE_ARGS ${CONTAINER_11[@]} ;;
    9) ./gradlew $BASE_GRADLE_ARGS ${CONTAINER_11[@]} ;;
    10) ./gradlew $BASE_GRADLE_ARGS ${CONTAINER_11[@]} ;;
    11) ./gradlew $BASE_GRADLE_ARGS ${CONTAINER_11[@]} --stacktrace -PenableErrorProne=true && checkDocsBuild ;;
esac
