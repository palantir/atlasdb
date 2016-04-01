FROM frolvlad/alpine-oraclejdk8:slim

RUN apk update && apk add libstdc++

EXPOSE 3828

ADD build/artifacts/atlasdb-timelock-server-all.jar /atlasdb/atlasdb-timelock-server-all.jar

ADD docker/conf/ /timelock/var/conf

CMD java -jar /atlasdb/atlasdb-timelock-server-all.jar server /timelock/var/conf/timelock.yml
