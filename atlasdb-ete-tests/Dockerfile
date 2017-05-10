FROM centos:centos7

MAINTAINER hsaraogi@palantir.com <Himangi Saraogi>

# Default to UTF-8 file.encoding
ENV LC_ALL en_US.UTF-8

ENV JAVA_HOME /usr/java/jdk1.8.0_102/jre
ENV JAVA_VERSION 8u102
ENV RPM_VERSION 8u102-b14

# - install java8 jdk
RUN yum install -y wget && \
    wget \
      --no-cookies \
      --no-check-certificate \
      --header "Cookie: gpw_e24=http%3A%2F%2Fwww.oracle.com%2F; oraclelicense=accept-securebackup-cookie" \
      "http://download.oracle.com/otn-pub/java/jdk/${RPM_VERSION}/jdk-${JAVA_VERSION}-linux-x64.rpm" && \
    yum localinstall -y "jdk-${JAVA_VERSION}-linux-x64.rpm" && \
    rm -f "jdk-${JAVA_VERSION}-linux-x64.rpm"

ENV DOCKERIZE_VERSION v0.2.0

RUN wget --no-check-certificate http://github.com/jwilder/dockerize/releases/download/$DOCKERIZE_VERSION/dockerize-linux-amd64-$DOCKERIZE_VERSION.tar.gz \
    && tar -C /usr/local/bin -xzvf dockerize-linux-amd64-$DOCKERIZE_VERSION.tar.gz

EXPOSE 3828

ADD build/docker/atlasdb-ete-snapshot.tgz /atlasdb-ete

# Remove possible version prefix
RUN for file in atlasdb-ete/*; do mv "$file" "atlasdb-ete/atlasdb-ete-snapshot"; done

ADD docker/conf/ /atlasdb-ete/atlasdb-ete-snapshot/var/conf

WORKDIR /atlasdb-ete/atlasdb-ete-snapshot

CMD service/bin/init.sh console
