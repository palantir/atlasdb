FROM frolvlad/alpine-oraclejdk8:slim

RUN apk update && apk add libstdc++ bash

EXPOSE 3828

ADD build/docker/atlasdb-todos-snapshot.tgz /atlasdb-todos

# Remove possible version prefix
RUN for file in atlasdb-todos/*; do mv "$file" "atlasdb-todos/atlasdb-todos-snapshot"; done

ADD docker/conf/ /atlasdb-todos/atlasdb-todos-snapshot/var/conf

WORKDIR /atlasdb-todos/atlasdb-todos-snapshot/service/bin/

CMD bash -c './init.sh console'
