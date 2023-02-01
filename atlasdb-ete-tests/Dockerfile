FROM cimg/openjdk:11.0

ENV DOCKERIZE_VERSION v0.2.0

RUN sudo apt-get update \
    && sudo apt-get install -y procps wget \
    && wget "https://github.com/jwilder/dockerize/releases/download/$DOCKERIZE_VERSION/dockerize-linux-amd64-$DOCKERIZE_VERSION.tar.gz" \
    && sudo tar -C /usr/local/bin -xzvf "dockerize-linux-amd64-$DOCKERIZE_VERSION.tar.gz"

EXPOSE 3828

ADD build/docker/atlasdb-ete-snapshot.tgz /atlasdb-ete

# Remove possible version prefix
RUN for file in atlasdb-ete/*; do sudo mv "$file" "atlasdb-ete/atlasdb-ete-snapshot"; done

ADD docker/ /atlasdb-ete/atlasdb-ete-snapshot/var/

WORKDIR /atlasdb-ete/atlasdb-ete-snapshot

CMD service/bin/init.sh console
