FROM frolvlad/alpine-oraclejdk8:slim
MAINTAINER AtlasDB Team

RUN apk update && apk add libstdc++ bash wget

# Prefer ADD to COPY because it does the tar handling
ADD build/docker/timelock-server-snapshot.tgz /timelock-server

# Remove possible version prefix
RUN for file in timelock-server/*; do mv "$file" "timelock-server/timelock-server-snapshot"; done

WORKDIR /timelock-server/timelock-server-snapshot

EXPOSE 8421
EXPOSE 8422

CMD service/bin/init.sh console
