#!/bin/bash

if [[ -z "${ANTITHESIS_REPO_URL}" ]]; then
  echo "Antithesis image repository URL is not set as an environment variable, exiting."
  exit 1
fi

if [[ -z "${ANTITHESIS_LOGIN_JSON}" ]]; then
  echo "Antithesis json key is not set as an environment variable, exiting."
  exit 1
fi

if [[ -z "${ANTITHESIS_WEBHOOK_PASSWORD}" ]]; then
  echo "Antithesis webhook password is not set as an environment variable, exiting."
  exit 1
fi

printenv ANTITHESIS_LOGIN_JSON | base64 -d | docker login -u _json_key https://${ANTITHESIS_REPO_URL} --password-stdin

./gradlew --scan dockerTag

VERSION=$(./gradlew -q printVersion)
EXPECTED_ANTITHESIS_TAG="latest"

docker pull palantirtechnologies/cassandra:2.2.18-1.116.0
docker tag palantirtechnologies/cassandra:2.2.18-1.116.0 ${ANTITHESIS_REPO_URL}/cassandra:${EXPECTED_ANTITHESIS_TAG}
docker push ${ANTITHESIS_REPO_URL}/cassandra:${EXPECTED_ANTITHESIS_TAG}

docker tag palantirtechnologies/timelock-server-distribution:${VERSION} ${ANTITHESIS_REPO_URL}/timelock-server-distribution:${EXPECTED_ANTITHESIS_TAG}
docker push ${ANTITHESIS_REPO_URL}/timelock-server-distribution:${EXPECTED_ANTITHESIS_TAG}

docker tag palantirtechnologies/atlasdb-workload-server-distribution:${VERSION} ${ANTITHESIS_REPO_URL}/atlasdb-workload-server-distribution:${EXPECTED_ANTITHESIS_TAG}
docker push ${ANTITHESIS_REPO_URL}/atlasdb-workload-server-distribution:${EXPECTED_ANTITHESIS_TAG}

docker tag palantirtechnologies/atlasdb-workload-server-antithesis:${VERSION} ${ANTITHESIS_REPO_URL}/atlasdb-workload-server-antithesis:${EXPECTED_ANTITHESIS_TAG}
docker push ${ANTITHESIS_REPO_URL}/atlasdb-workload-server-antithesis:${EXPECTED_ANTITHESIS_TAG}

WEBHOOK_LOCATOR="palantir_atlasdb__smoketest__latest
echo "Triggering simulation on Antithesis via the ${WEBHOOK_LOCATOR} webhook."
curl -v -u "palantir:${ANTITHESIS_WEBHOOK_PASSWORD}" -X POST https://palantir.antithesis.com/api/v1/launch_experiment/${WEBHOOK_LOCATOR}