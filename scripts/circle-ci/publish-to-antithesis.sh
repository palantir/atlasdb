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

./gradlew docker

docker pull palantirtechnologies/docker-cassandra-atlasdb:atlasdb-testing-palantir-cassandra
docker tag palantirtechnologies/docker-cassandra-atlasdb:atlasdb-testing-palantir-cassandra ${ANTITHESIS_REPO_URL}/docker-cassandra-atlasdb:atlasdb-testing-palantir-cassandra
docker push ${ANTITHESIS_REPO_URL}/docker-cassandra-atlasdb:atlasdb-testing-palantir-cassandra

docker tag palantirtechnologies/timelock-server-distribution:unspecified ${ANTITHESIS_REPO_URL}/timelock-server-distribution:unspecified
docker push ${ANTITHESIS_REPO_URL}/timelock-server-distribution:unspecified

docker tag palantirtechnologies/atlasdb-workload-server-distribution:unspecified ${ANTITHESIS_REPO_URL}/atlasdb-workload-server-distribution:unspecified
docker push ${ANTITHESIS_REPO_URL}/atlasdb-workload-server-distribution:unspecified

LATEST_ANTITHESIS_TAG=$(docker image ls palantirtechnologies/atlasdb-workload-server-antithesis --format "{{.Tag}}")
docker tag palantirtechnologies/atlasdb-workload-server-antithesis:${LATEST_ANTITHESIS_TAG} ${ANTITHESIS_REPO_URL}/atlasdb-workload-server-antithesis:unspecified
docker push ${ANTITHESIS_REPO_URL}/atlasdb-workload-server-antithesis:unspecified