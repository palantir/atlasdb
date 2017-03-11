#! /bin/sh

set -ev

for host in n1 n2 n3 n4 n5; do
	ssh-keyscan -t rsa $host >> ~/.ssh/known_hosts
done
