#!/bin/bash
set -e

source scripts/benchmarks/servers.txt

./scripts/benchmarks/render-configs.sh

SCRIPTS_FOLDER="scripts/benchmarks"
SERVER_SIDE_SCRIPT="run-on-il.sh"
SERVER="$CLIENT"
YML_FILE="benchmark-server.yml"
REMOTE_DIR="/tmp/timelock-benchmark"
CASSANDRA_KEYSTORE="keystore.jks"
CASSANDRA_TRUSTSTORE="truststore.jks"

rm -rf timelock-server-benchmark-client/build/distributions/
./gradlew --parallel timelock-server-benchmark-client:distTar

SLS_FILE="$(find ./timelock-server-benchmark-client/build/distributions/ -name '*sls.tgz' | tail -1)"

echo "copying sls file to $SERVER"

ssh $SERVER "rm -rf $REMOTE_DIR && mkdir $REMOTE_DIR"
scp $SLS_FILE "$SERVER:$REMOTE_DIR"

echo "copying keystore and truststore"

scp "$SCRIPTS_FOLDER/$CASSANDRA_KEYSTORE" "$SERVER:$REMOTE_DIR"
scp "$SCRIPTS_FOLDER/$CASSANDRA_TRUSTSTORE" "$SERVER:$REMOTE_DIR"

echo "running remote script"

scp "$SCRIPTS_FOLDER/$SERVER_SIDE_SCRIPT" "$SERVER:$REMOTE_DIR"
ssh $SERVER "$REMOTE_DIR/$SERVER_SIDE_SCRIPT $YML_FILE"
