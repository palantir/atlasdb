#!/bin/bash
set -e

SERVER_SIDE_SCRIPT="run-on-il.sh"
declare -a servers=("il-pg-alpha-1536393.usw1.palantir.global" "il-pg-alpha-1086749.usw1.palantir.global" "il-pg-alpha-1086750.usw1.palantir.global")

rm -rf timelock-server-benchmark-cluster/build/distributions/
./gradlew --parallel timelock-server-benchmark-cluster:distTar

SLS_FILE="$(find ./timelock-server-benchmark-cluster/build/distributions/ -name 'timelock*sls.tgz' | tail -1)"

echo "sls file: $SLS_FILE ..."

SERVER_INDEX=0
for server in "${servers[@]}"
do
  echo "server: $server"

  echo "copying sls file"
  REMOTE_DIR="/opt/palantir/timelock"
  ssh $server "rm -rf $REMOTE_DIR/*"
  scp $SLS_FILE "$server:$REMOTE_DIR"

  echo "running remote script"
  scp $SERVER_SIDE_SCRIPT "$server:$REMOTE_DIR"

  YML_FILE="timelock-remote$SERVER_INDEX.yml"
  ssh $server "$REMOTE_DIR/$SERVER_SIDE_SCRIPT $YML_FILE"

  SERVER_INDEX=$((SERVER_INDEX+1))
done








