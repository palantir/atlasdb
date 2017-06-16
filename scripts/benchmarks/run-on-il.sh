#!/bin/bash
set -e

YML_FILE=$1

cd "$(dirname "$0")"

kill -9 $(jps | grep 'ServerLauncher' | grep -v 'grep' | awk '{print $1}') || { echo "unable to kill existing timelock - this is ok if it wasn't running" ; : ; }

TARBALL="$(ls . | grep 'timelock' | tail -1)"

tar -xzf $TARBALL

echo "extracted $TARBALL"

DIR="$(ls -d */ | grep 'timelock' | tail -1)"
cd $DIR

sed -i -e "s/timelock.yml/$YML_FILE/g" service/bin/launcher-static.yml

echo "starting server"

./service/bin/init.sh start

echo "DONE"








