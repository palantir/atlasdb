#!/bin/bash
set -e

YML_FILE=$1

CASSANDRA_KEYSTORE="keystore.jks"
CASSANDRA_TRUSTSTORE="truststore.jks"

cd "$(dirname "$0")"

kill -9 $(jps | grep 'Timelock' | grep -v 'grep' | awk '{print $1}') 2>/dev/null || { echo "unable to kill existing timelock - this is ok iff it wasn't running" ; : ; }

TARBALL="$(ls . | grep 'timelock' | tail -1)"

tar -xzf $TARBALL

echo "extracted $TARBALL"

DIR="$(ls -d */ | grep 'timelock' | tail -1)"
cd $DIR

rm var/security/*
cp ../$CASSANDRA_TRUSTSTORE var/security/$CASSANDRA_TRUSTSTORE
cp ../$CASSANDRA_KEYSTORE var/security/$CASSANDRA_KEYSTORE

sed -i -e "s/timelock.yml/$YML_FILE/g" service/bin/launcher-static.yml

echo "starting server"

./service/bin/init.sh start

echo "DONE"
