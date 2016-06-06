FROM frolvlad/alpine-oraclejdk8:slim

RUN apk update && apk add libstdc++ bash wget

ENV DOCKERIZE_VERSION v0.2.0

RUN wget --no-check-certificate http://github.com/jwilder/dockerize/releases/download/$DOCKERIZE_VERSION/dockerize-linux-amd64-$DOCKERIZE_VERSION.tar.gz \
    && tar -C /usr/local/bin -xzvf dockerize-linux-amd64-$DOCKERIZE_VERSION.tar.gz

EXPOSE 3828

ADD build/docker/atlasdb-todos-snapshot.tgz /atlasdb-todos

# Remove possible version prefix
RUN for file in atlasdb-todos/*; do mv "$file" "atlasdb-todos/atlasdb-todos-snapshot"; done

ADD docker/conf/ /atlasdb-todos/atlasdb-todos-snapshot/var/conf

WORKDIR /atlasdb-todos/atlasdb-todos-snapshot/service/bin/

CMD echo "hello" #bash -c './init.sh console'
