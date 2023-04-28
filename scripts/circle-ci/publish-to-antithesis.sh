#!/bin/bash

if [[ -z "${ANTITHESIS_REPO_URL}" ]]; then
  echo "Antithesis image repository URL is not set as an environment variable, exiting."
  exit 1
fi

if [[ -z "${ANTITHESIS_LOGIN_JSON}" ]]; then
  echo "Antithesis json key is not set as an environment variable, exiting."
  exit 1
fi

printenv ANTITHESIS_LOGIN_JSON | base64 -d | docker login -u _json_key https://${ANTITHESIS_REPO_URL} --password-stdin

./gradlew dockerTag

VERSION=$(./gradlew -q printVersion)

docker pull palantirtechnologies/docker-cassandra-atlasdb:atlasdb-testing-palantir-cassandra
docker tag palantirtechnologies/docker-cassandra-atlasdb:atlasdb-testing-palantir-cassandra ${ANTITHESIS_REPO_URL}/docker-cassandra-atlasdb:atlasdb-testing-palantir-cassandra
docker push ${ANTITHESIS_REPO_URL}/docker-cassandra-atlasdb:atlasdb-testing-palantir-cassandra

docker tag palantirtechnologies/timelock-server-distribution:${VERSION} ${ANTITHESIS_REPO_URL}/timelock-server-distribution:${VERSION}
docker push ${ANTITHESIS_REPO_URL}/timelock-server-distribution:${VERSION}

docker tag palantirtechnologies/atlasdb-workload-server-distribution:${VERSION} ${ANTITHESIS_REPO_URL}/atlasdb-workload-server-distribution:${VERSION}
docker push ${ANTITHESIS_REPO_URL}/atlasdb-workload-server-distribution:${VERSION}

docker tag palantirtechnologies/atlasdb-workload-server-antithesis:${VERSION} ${ANTITHESIS_REPO_URL}/atlasdb-workload-server-antithesis:${VERSION}
docker push ${ANTITHESIS_REPO_URL}/atlasdb-workload-server-antithesis:${VERSION}