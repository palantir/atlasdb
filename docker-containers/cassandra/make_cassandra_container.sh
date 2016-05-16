#!/bin/bash
DIR=$(dirname $0)
docker build -t atlas-cassandra:2.2-v0.1 $DIR
