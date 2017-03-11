#! /bin/sh

set -ev

for host in n1 n2 n3 n4 n5; do
	ssh-keyscan -H $host >> ~/.ssh/known_hosts
done
