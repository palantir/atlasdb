FROM frolvlad/alpine-oraclejdk8:slim
MAINTAINER AtlasDB Team

# Bash is useful for monitoring; curl is used by our healthchecks.
RUN apk update && apk add bash curl

RUN curl -k https://www.yourkit.com/download/yjp-2017.02-b57.zip > yjp.zip && unzip yjp.zip

# Prefer ADD to COPY because it does the tar handling
ADD timelock-server*.tgz /

RUN mv /timelock-server* /timelock-server

WORKDIR /timelock-server

EXPOSE 8421 \
       8422 \
       10000

# Putting exit code as '1' on failure to adhere to docker specification
HEALTHCHECK --interval=5s \
            --timeout=5s \
            --retries=5 \
            CMD curl -f http://localhost:8422/healthcheck || exit 1

CMD ["service/bin/init.sh", "console"]
