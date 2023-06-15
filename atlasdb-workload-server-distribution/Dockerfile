FROM adoptopenjdk/openjdk11:jdk-11.0.11_9-debian
MAINTAINER AtlasDB Team

ENV DOCKERIZE_VERSION v0.2.0

# Bash is useful for monitoring; curl is used by our healthchecks.
RUN apt-get update && apt-get -y install bash curl procps wget \
    && wget --no-check-certificate http://github.com/jwilder/dockerize/releases/download/$DOCKERIZE_VERSION/dockerize-linux-amd64-$DOCKERIZE_VERSION.tar.gz \
    && tar -C /usr/local/bin -xzvf dockerize-linux-amd64-$DOCKERIZE_VERSION.tar.gz

# Prefer ADD to COPY because it does the tar handling
ADD atlasdb-workload-server-*.tgz /

RUN mv /atlasdb-workload-server-* /atlasdb-workload-server

WORKDIR /atlasdb-workload-server

# Needed for Antithesis
RUN curl -o /usr/bin/yq -kLf https://github.com/mikefarah/yq/releases/download/v4.33.2/yq_linux_amd64 && \
    chmod +x /usr/bin/yq
RUN yq '.classpath += "service/lib/instrumentation.jar"' -i service/bin/launcher-check.yml \
    && yq '.classpath += "service/lib/jna.jar"' -i service/bin/launcher-check.yml \
    && yq '.classpath += "service/lib/instrumentation.jar"' -i service/bin/launcher-static.yml \
    && yq '.classpath += "service/lib/jna.jar"' -i service/bin/launcher-static.yml

CMD ["service/bin/init.sh", "console"]
