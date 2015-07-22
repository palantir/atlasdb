#!/bin/bash

SUB_DIR="${SUB_DIR:-src}"

for SOURCE_DIR in "$@"; do
    SOURCE_NAME=$(basename $SOURCE_DIR)
    if [ "$SOURCE_NAME" = $(echo $SOURCE_NAME | tr '[:upper:]' '[:lower:]') ]; then
	TARGET_NAME=${SOURCE_NAME}-protobufs
    else
	TARGET_NAME=${SOURCE_NAME}Protobufs
    fi
    mkdir -p $SOURCE_DIR/../$TARGET_NAME
    TARGET_DIR=$(cd $SOURCE_DIR/../$TARGET_NAME; pwd -P)

    [ -n "$PROTOC" ] || PROTOC=protoc
    CORRECT_VERSION="libprotoc 2.6.0"
    CORRECT_VERSION_2="libprotoc 2.6.1"
    ACTUAL_VERSION="$($PROTOC --version)"
    if [ "$CORRECT_VERSION" != "$ACTUAL_VERSION" ] && [ "$CORRECT_VERSION_2" != "$ACTUAL_VERSION" ]; then
        printf "Expecting $CORRECT_VERSION but found $ACTUAL_VERSION.\n"
        printf "Please point \$PROTOC at the right executable. See README.txt.\n"
        exit 1
    fi
    printf "\nCompiling $SOURCE_DIR/**/*.proto => $TARGET_DIR\n"
    printf "Using $PROTOC as protoc\n"

    rm -rf $TARGET_DIR
    mkdir -p $TARGET_DIR/src
    TEMPLATES=$(dirname $(echo "$0"))/*__*
    for n in $TEMPLATES; do
        sed \
            -e "s/@@TARGET_NAME@@/${TARGET_NAME}/g" \
            $n > $TARGET_DIR/$(basename $n | sed -e 's/__//g')
    done
    #find $SOURCE_DIR -name \*.proto -exec $PROTOC --java_out=$TARGET_DIR/src '{}' +
    find $SOURCE_DIR -name \*.proto | grep -E -v "/e?c?build/" | xargs -n1 $PROTOC --java_out=$TARGET_DIR/src -I $SOURCE_DIR/$SUB_DIR

    cat <<EOF
In ${SOURCE_DIR}/.classpath, make sure you have something like:
 <classpathentry combineaccessrules="false" exported="true" kind="src" path="/${TARGET_NAME}"/>
In ${SOURCE_DIR}/ivy.xml, make sure you have something like:
 <dependency rev="\${version.product}" org="palantir" name="${TARGET_NAME}"/>
Make sure to
   git add -f $TARGET_DIR/default_compile_settings.prefs
EOF
done
