FROM anapsix/alpine-java

# Make wget work (from https://github.com/Yelp/dumb-init/issues/73)
RUN apk update && \
    apk add ca-certificates && \
    update-ca-certificates && \
    apk add openssl openssh

# Install leiningen
RUN wget https://raw.githubusercontent.com/technomancy/leiningen/2.7.1/bin/lein && \
    mv /lein /usr/bin && \
    chmod +x /usr/bin/lein

# Install Jepsen
RUN wget https://github.com/jepsen-io/jepsen/archive/0.1.4.zip && \
    unzip -d / 0.1.4.zip && mv /jepsen-0.1.4 /jepsen && \
    cd /jepsen/jepsen && \
    lein install
