FROM adoptopenjdk/openjdk11:jdk-11.0.11_9-debian
MAINTAINER AtlasDB Team

# Bash is useful for monitoring; curl is used by our healthchecks.
RUN apt-get update && apt-get -y install bash curl

# Prefer ADD to COPY because it does the tar handling
ADD timelock-server-*.tgz /

RUN mv /timelock-server-* /timelock-server

WORKDIR /timelock-server

RUN mkdir -p var/data/paxos

# Needed for Antithesis
RUN curl -o /usr/bin/yq -kLf https://github.com/mikefarah/yq/releases/download/v4.33.2/yq_linux_amd64 && \
    chmod +x /usr/bin/yq
RUN yq '.classpath += "service/lib/instrumentation.jar"' -i service/bin/launcher-check.yml \
    && yq '.classpath += "service/lib/jna.jar"' -i service/bin/launcher-check.yml \
    && yq '.classpath += "service/lib/instrumentation.jar"' -i service/bin/launcher-static.yml \
    && yq '.classpath += "service/lib/jna.jar"' -i service/bin/launcher-static.yml

EXPOSE 8421 \
       8422

# Putting exit code as '1' on failure to adhere to docker specification
HEALTHCHECK --interval=5s \
            --timeout=5s \
            --retries=5 \
            CMD curl -f http://localhost:8422/healthcheck || exit 1

CMD ["service/bin/init.sh", "console"]
