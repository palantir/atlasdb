FROM frolvlad/alpine-oraclejdk8:slim
MAINTAINER AtlasDB Team

# Prefer ADD to COPY because it does the tar handling
ADD timelock-server-*.tgz /timelock-server

# Remove possible version prefix
RUN mv "timelock-server/"$(ls "timelock-server") "timelock-server/"

WORKDIR /timelock-server

EXPOSE 8421 \
       8422

CMD ["service/bin/init.sh", "console"]
