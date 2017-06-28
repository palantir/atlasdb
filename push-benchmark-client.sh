#!/bin/bash
set -e

source scripts/benchmarks/servers.txt

./scripts/benchmarks/render-configs.sh

SERVER_SIDE_SCRIPT="run-on-il.sh"
server="$CLIENT"

rm -rf timelock-server-benchmark-client/build/distributions/
./gradlew --parallel timelock-server-benchmark-client:distTar

SLS_FILE="$(find ./timelock-server-benchmark-client/build/distributions/ -name 'timelock*sls.tgz' | tail -1)"

echo "sls file: $SLS_FILE ..."

echo "copying sls file"
REMOTE_DIR="/opt/palantir/timelock"
ssh -t -t $server "su -c 'rm -rf $REMOTE_DIR/*' - palantir"
scp $SLS_FILE "$server:~"
ssh -t -t $server "su -c 'cp /home/jkong/timelock-benchmarks-server-0.44.0-31-g460eee0.dirty.sls.tgz $REMOTE_DIR' - palantir"

#ssh $server "rm -rf $REMOTE_DIR/*"
#scp $SLS_FILE "$server:$REMOTE_DIR"

echo "running remote script"
scp scripts/benchmarks/$SERVER_SIDE_SCRIPT "$server:~"
ssh -t -t $server "su -c 'cp /home/jkong/$SERVER_SIDE_SCRIPT $REMOTE_DIR' - palantir"

#scp scripts/benchmarks/$SERVER_SIDE_SCRIPT "$server:$REMOTE_DIR"

YML_FILE="benchmark-server.yml"
ssh -t -t $server "su -c '$REMOTE_DIR/$SERVER_SIDE_SCRIPT $YML_FILE' - palantir"
