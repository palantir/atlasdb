FROM adoptopenjdk/openjdk11:jre-11.0.9_11.1-alpine
MAINTAINER AtlasDB Team

# Bash and curl are convenient for monitoring and debugging
RUN apk update && apk add bash curl

# Prefer ADD to COPY because it does the tar handling
ADD atlasdb-workload-server-*.tgz /

RUN mv /atlasdb-workload-server-* /atlasdb-workload-server

WORKDIR /atlasdb-workload-server

CMD ["service/bin/init.sh", "console"]
