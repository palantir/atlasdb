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

./gradlew --scan dockerTag

VERSION=$(./gradlew -q printVersion)
EXPECTED_ANTITHESIS_TAG_DC1="migration-dc1"
EXPECTED_ANTITHESIS_TAG_DC2="migration-dc2"
EXPECTED_ANTITHESIS_TAG_OTHER="palantir_atlasdb__migration-6-nodes__short-latest"

docker pull palantirtechnologies/cassandra:2.2.18-1.143.0-rc11
docker tag palantirtechnologies/cassandra:2.2.18-1.143.0-rc11 ${ANTITHESIS_REPO_URL}/cassandra:${EXPECTED_ANTITHESIS_TAG_DC1}
docker push ${ANTITHESIS_REPO_URL}/cassandra:${EXPECTED_ANTITHESIS_TAG_DC1}

docker pull palantirtechnologies/cassandra:2.2.18-1.143.0-rc14
docker tag palantirtechnologies/cassandra:2.2.18-1.143.0-rc14 ${ANTITHESIS_REPO_URL}/cassandra:${EXPECTED_ANTITHESIS_TAG_DC2}
docker push ${ANTITHESIS_REPO_URL}/cassandra:${EXPECTED_ANTITHESIS_TAG_DC2}

docker tag palantirtechnologies/timelock-server-distribution:${VERSION} ${ANTITHESIS_REPO_URL}/timelock-server-distribution:${EXPECTED_ANTITHESIS_TAG_OTHER}
docker push ${ANTITHESIS_REPO_URL}/timelock-server-distribution:${EXPECTED_ANTITHESIS_TAG_OTHER}

docker tag palantirtechnologies/atlasdb-workload-server-distribution:${VERSION} ${ANTITHESIS_REPO_URL}/atlasdb-workload-server-distribution:${EXPECTED_ANTITHESIS_TAG_OTHER}
docker push ${ANTITHESIS_REPO_URL}/atlasdb-workload-server-distribution:${EXPECTED_ANTITHESIS_TAG_OTHER}

docker tag palantirtechnologies/atlasdb-workload-server-antithesis:${VERSION} ${ANTITHESIS_REPO_URL}/atlasdb-workload-server-antithesis:${EXPECTED_ANTITHESIS_TAG_OTHER}
docker push ${ANTITHESIS_REPO_URL}/atlasdb-workload-server-antithesis:${EXPECTED_ANTITHESIS_TAG_OTHER}
