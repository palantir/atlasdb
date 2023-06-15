FROM scratch
MAINTAINER AtlasDB Team

ADD docker-compose.yml /
ADD compact.sh /
ADD flush.sh /
CMD chmod 555 compact.sh /
CMD chmod 555 flush.sh /
