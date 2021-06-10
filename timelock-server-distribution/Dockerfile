FROM adoptopenjdk/openjdk11:jre-11.0.9_11.1-alpine
MAINTAINER AtlasDB Team

# Bash is useful for monitoring; curl is used by our healthchecks.
RUN apk update && apk add bash curl

# Prefer ADD to COPY because it does the tar handling
ADD timelock-server-*.tgz /

RUN mv /timelock-server-* /timelock-server

WORKDIR /timelock-server

RUN mkdir -p var/data/paxos

EXPOSE 8421 \
       8422

# Putting exit code as '1' on failure to adhere to docker specification
HEALTHCHECK --interval=5s \
            --timeout=5s \
            --retries=5 \
            CMD curl -f http://localhost:8422/healthcheck || exit 1

CMD ["service/bin/init.sh", "console"]
