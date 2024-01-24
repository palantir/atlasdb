#!/bin/bash

echo "Preparing images for Antithesis test"

VERSION=$1
EXPECTED_ANTITHESIS_TEST_TAG="unspecified"

docker pull palantirtechnologies/cassandra:2.2.18-1.116.0
docker tag palantirtechnologies/cassandra:2.2.18-1.116.0 cassandra:${EXPECTED_ANTITHESIS_TEST_TAG}
docker tag palantirtechnologies/timelock-server-distribution:${VERSION} timelock-server-distribution:${EXPECTED_ANTITHESIS_TEST_TAG}
docker tag palantirtechnologies/atlasdb-workload-server-distribution:${VERSION} atlasdb-workload-server-distribution:${EXPECTED_ANTITHESIS_TEST_TAG}
docker tag palantirtechnologies/atlasdb-workload-server-antithesis:${VERSION} atlasdb-workload-server-antithesis:${EXPECTED_ANTITHESIS_TEST_TAG}
