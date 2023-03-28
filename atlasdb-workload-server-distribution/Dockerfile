FROM adoptopenjdk/openjdk11:jre-11.0.9_11.1-alpine
MAINTAINER AtlasDB Team

ENV DOCKERIZE_VERSION v0.2.0

# Bash is useful for monitoring; curl is used by our healthchecks.
RUN apk update && apk add bash curl procps wget  \
    && wget --no-check-certificate http://github.com/jwilder/dockerize/releases/download/$DOCKERIZE_VERSION/dockerize-linux-amd64-$DOCKERIZE_VERSION.tar.gz \
    && tar -C /usr/local/bin -xzvf dockerize-linux-amd64-$DOCKERIZE_VERSION.tar.gz

# Prefer ADD to COPY because it does the tar handling
ADD atlasdb-workload-server-*.tgz /

RUN mv /atlasdb-workload-server-* /atlasdb-workload-server

WORKDIR /atlasdb-workload-server

CMD ["service/bin/init.sh", "console"]