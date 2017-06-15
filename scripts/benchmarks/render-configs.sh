#!/bin/bash

source scripts/benchmarks/servers.txt

# timelock-server-benchmark-cluster
TEMPLATE_DIR="./timelock-server-benchmark-cluster/var/conf/template"
RENDERED_DIR="./timelock-server-benchmark-cluster/var/conf"

declare -a configs=("timelock-remote0.yml" "timelock-remote1.yml" "timelock-remote2.yml")

for config in "${configs[@]}"
do
  cat "$TEMPLATE_DIR/$config" | sed "s/SERVER1/$SERVER1/g" | sed "s/SERVER2/$SERVER2/g" | sed "s/SERVER3/$SERVER3/g" > "$RENDERED_DIR/$config"
done

# timelock-server-benchmark-client
TEMPLATE_DIR="./timelock-server-benchmark-client/var/conf/template"
RENDERED_DIR="./timelock-server-benchmark-client/var/conf"

config="benchmark-server.yml"

cat "$TEMPLATE_DIR/$config" | sed "s/SERVER1/$SERVER1/g" | sed "s/SERVER2/$SERVER2/g" | sed "s/SERVER3/$SERVER3/g" | sed "s/CASSANDRA1/$CASSANDRA1/g" | sed "s/CASSANDRA2/$CASSANDRA2/g" | sed "s/CASSANDRA3/$CASSANDRA3/g" > "$RENDERED_DIR/$config"

