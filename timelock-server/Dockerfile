FROM frolvlad/alpine-oraclejdk8:slim
MAINTAINER AtlasDB Team

# Bash is useful for monitoring; curl is used by our healthchecks.
RUN apk update && apk add bash curl

# Prefer ADD to COPY because it does the tar handling
ADD timelock-server-*.tgz /timelock-server

# Remove possible version prefix, and avoid unnecessary directory nesting
# Note: We can't use mv -T because busybox's mv doesn't have that option.
RUN mv "/timelock-server/"$(ls "/timelock-server/") "/timelock/" && \
    rmdir "/timelock-server/" && \
    mv "/timelock/" "/timelock-server/"

WORKDIR /timelock-server

EXPOSE 8421 \
       8422

HEALTHCHECK --interval=5s \
            --timeout=5s \
            --retries=5 \
            CMD curl -f http://localhost:8422/healthcheck || exit 1

CMD ["service/bin/init.sh", "console"]
