#!/bin/bash
set -e

source scripts/benchmarks/servers.txt

./scripts/benchmarks/render-configs.sh

SCRIPTS_FOLDER="scripts/benchmarks"
SERVER_SIDE_SCRIPT="run-on-il.sh"
REMOTE_DIR="/tmp/timelock-benchmark"
CASSANDRA_KEYSTORE="keystore.jks"
CASSANDRA_TRUSTSTORE="truststore.jks"

declare -a SERVERS=("$SERVER1" "$SERVER2" "$SERVER3")

rm -rf timelock-server-benchmark-cluster/build/distributions/
./gradlew --parallel timelock-server-benchmark-cluster:distTar

SLS_FILE="$(find ./timelock-server-benchmark-cluster/build/distributions -name '*sls.tgz' | tail -1)"

echo "sls file: $SLS_FILE ..."

SERVER_INDEX=0
for SERVER in "${SERVERS[@]}"
do
{
  echo "server: $SERVER"
  echo "copying sls file"

  ssh $SERVER "rm -rf $REMOTE_DIR && mkdir $REMOTE_DIR"
  scp $SLS_FILE "$SERVER:$REMOTE_DIR"

  echo "copying keystore and truststore"

  scp "$SCRIPTS_FOLDER/$CASSANDRA_KEYSTORE" "$SERVER:$REMOTE_DIR"
  scp "$SCRIPTS_FOLDER/$CASSANDRA_TRUSTSTORE" "$SERVER:$REMOTE_DIR"

  echo "running remote script"
  scp "$SCRIPTS_FOLDER/$SERVER_SIDE_SCRIPT" "$SERVER:$REMOTE_DIR"

  YML_FILE="timelock-remote$SERVER_INDEX.yml"
  ssh $SERVER "$REMOTE_DIR/$SERVER_SIDE_SCRIPT $YML_FILE"

  SERVER_INDEX=$((SERVER_INDEX+1))
} &
done

wait

echo "DONE - ALL SERVERS STARTED"
