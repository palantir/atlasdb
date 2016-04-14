FROM frolvlad/alpine-oraclejdk8:slim

RUN apk update && apk add libstdc++ bash

EXPOSE 3828

ADD build/distributions/atlasdb-timelock-snapshot.tgz /atlasdb

ADD docker/conf/ /atlasdb/atlasdb-timelock-snapshot/var/conf

WORKDIR /atlasdb/atlasdb-timelock-snapshot/service/bin/
CMD bash -c './init.sh console'
