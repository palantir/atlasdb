#!/bin/bash
set -e

source scripts/benchmarks/servers.txt

./scripts/benchmarks/render-configs.sh

SERVER_SIDE_SCRIPT="run-on-il.sh"
SERVER="$CLIENT"
YML_FILE="benchmark-server.yml"
REMOTE_DIR="/opt/palantir/timelock"

rm -rf timelock-server-benchmark-client/build/distributions/
./gradlew --parallel timelock-server-benchmark-client:distTar

SLS_FILE="$(find ./timelock-server-benchmark-client/build/distributions/ -name '*sls.tgz' | tail -1)"

echo "copying sls file to $SERVER"

ssh $SERVER "rm -rf $REMOTE_DIR/*"
scp $SLS_FILE "$SERVER:$REMOTE_DIR"

echo "running remote script"

scp scripts/benchmarks/$SERVER_SIDE_SCRIPT "$SERVER:$REMOTE_DIR"
ssh $SERVER "$REMOTE_DIR/$SERVER_SIDE_SCRIPT $YML_FILE"
