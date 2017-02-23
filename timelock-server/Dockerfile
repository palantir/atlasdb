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

HEALTHCHECK --interval=10s \
            --retries=5 \
            CMD curl http://localhost:8422/healthcheck | \
                python -c "import sys, json; sys.exit(json.load(sys.stdin)['leader-ping']['healthy'] != True)" || \
                exit 1
