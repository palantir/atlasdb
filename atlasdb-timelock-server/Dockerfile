FROM frolvlad/alpine-oraclejdk8:slim

ADD build/artifacts/atlasdb-timelock-server-all.jar /atlasdb/atlasdb-timelock-server-all.jar

EXPOSE 3828

RUN apk update && apk add libstdc++

CMD sed "s/HOSTNAME/$(hostname)/g" </timelock/var/conf/timelock.yml >/tmp/timelock.conf && java -jar /atlasdb/atlasdb-timelock-server-all.jar server /tmp/timelock.conf
