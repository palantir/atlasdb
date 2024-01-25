#!/bin/bash

echo "Preparing images for Antithesis test"

VERSION=$1
EXPECTED_ANTITHESIS_TEST_TAG="unspecified"

docker pull palantirtechnologies/cassandra:2.2.18-1.116.0
docker tag palantirtechnologies/cassandra:2.2.18-1.116.0 palantirtechnologies/cassandra:${EXPECTED_ANTITHESIS_TEST_TAG}
docker tag palantirtechnologies/timelock-server-distribution:latest palantirtechnologies/timelock-server-distribution:${EXPECTED_ANTITHESIS_TEST_TAG}
docker tag palantirtechnologies/atlasdb-workload-server-distribution:latest palantirtechnologies/atlasdb-workload-server-distribution:${EXPECTED_ANTITHESIS_TEST_TAG}
docker tag palantirtechnologies/atlasdb-workload-server-antithesis:${VERSION} palantirtechnologies/atlasdb-workload-server-antithesis:${EXPECTED_ANTITHESIS_TEST_TAG}
