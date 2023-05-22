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

docker pull palantirtechnologies/cassandra:2.2.18-1.112.0-rc5
docker tag palantirtechnologies/cassandra:2.2.18-1.112.0-rc5 ${ANTITHESIS_REPO_URL}/cassandra:2.2.18-1.112.0-rc5
docker push ${ANTITHESIS_REPO_URL}/cassandra:2.2.18-1.112.0-rc5

docker tag palantirtechnologies/timelock-server-distribution:${VERSION} ${ANTITHESIS_REPO_URL}/timelock-server-distribution:${VERSION}
docker push ${ANTITHESIS_REPO_URL}/timelock-server-distribution:${VERSION}

docker tag palantirtechnologies/atlasdb-workload-server-distribution:${VERSION} ${ANTITHESIS_REPO_URL}/atlasdb-workload-server-distribution:${VERSION}
docker push ${ANTITHESIS_REPO_URL}/atlasdb-workload-server-distribution:${VERSION}

docker tag palantirtechnologies/atlasdb-workload-server-antithesis:${VERSION} ${ANTITHESIS_REPO_URL}/atlasdb-workload-server-antithesis:${VERSION}
docker push ${ANTITHESIS_REPO_URL}/atlasdb-workload-server-antithesis:${VERSION}