FROM frolvlad/alpine-oraclejdk8:slim

RUN apk update && apk add libstdc++ bash wget

ENV DOCKERIZE_VERSION v0.2.0

ENV JAVA_OPTS "-Xmx512M"

RUN wget --no-check-certificate http://github.com/jwilder/dockerize/releases/download/$DOCKERIZE_VERSION/dockerize-linux-amd64-$DOCKERIZE_VERSION.tar.gz \
    && tar -C /usr/local/bin -xzvf dockerize-linux-amd64-$DOCKERIZE_VERSION.tar.gz

EXPOSE 3828

ADD build/docker/atlasdb-ete-snapshot.tgz /atlasdb-ete

# Remove possible version prefix
RUN for file in atlasdb-ete/*; do mv "$file" "atlasdb-ete/atlasdb-ete-snapshot"; done

ADD docker/conf/ /atlasdb-ete/atlasdb-ete-snapshot/var/conf

WORKDIR /atlasdb-ete/atlasdb-ete-snapshot

CMD service/bin/init.sh console
