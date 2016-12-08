FROM frolvlad/alpine-oraclejdk8:slim

RUN apk update && apk add libstdc++ bash

EXPOSE 3828

ADD build/docker/atlasdb-timelock-server-snapshot.tgz /atlasdb

# Remove possible version prefix
RUN for file in atlasdb/*; do mv "$file" "atlasdb/atlasdb-timelock-server-snapshot"; done

ADD docker/conf/ /atlasdb/atlasdb-timelock-server-snapshot/var/conf

WORKDIR /atlasdb/atlasdb-timelock-server-snapshot/service/bin/
CMD bash -c './init.sh console'
