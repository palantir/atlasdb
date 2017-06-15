#!/bin/bash
set -e

source scripts/benchmarks/servers.txt

./scripts/benchmarks/render-configs.sh

SERVER_SIDE_SCRIPT="run-on-il.sh"
declare -a SERVERS=("$SERVER1" "$SERVER2" "$SERVER3")

rm -rf timelock-server-benchmark-cluster/build/distributions/
./gradlew --parallel timelock-server-benchmark-cluster:distTar

SLS_FILE="$(find ./timelock-server-benchmark-cluster/build/distributions/ -name '*sls.tgz' | tail -1)"

echo "sls file: $SLS_FILE ..."

SERVER_INDEX=0
for SERVER in "${SERVERS[@]}"
do
{
  echo "server: $SERVER"

  echo "copying sls file"
  REMOTE_DIR="/opt/palantir/timelock"
  ssh $SERVER "rm -rf $REMOTE_DIR/*"
  scp $SLS_FILE "$SERVER:$REMOTE_DIR"

  echo "running remote script"
  scp scripts/benchmarks/$SERVER_SIDE_SCRIPT "$SERVER:$REMOTE_DIR"

  YML_FILE="timelock-remote$SERVER_INDEX.yml"
  ssh $SERVER "$REMOTE_DIR/$SERVER_SIDE_SCRIPT $YML_FILE"

  SERVER_INDEX=$((SERVER_INDEX+1))
} &
done

wait

echo "DONE - ALL SERVERS STARTED"
