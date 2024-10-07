#!/bin/bash

if [[ -z "${ANTITHESIS_WEBHOOK_PASSWORD}" ]]; then
  echo "Antithesis webhook password is not set as an environment variable, exiting."
  exit 1
fi

if [[ -z "${ANTITHESIS_REPORT_RECIPIENT}" ]]; then
  echo "Antithesis report recipient is not set as an environment variable, exiting."
  exit 1
fi

WEBHOOK_LOCATOR="atlasdb"
DURATION=$([ "$CIRCLE_TAG" ] && echo "12" || echo "3")
echo "Triggering simulation on Antithesis via the ${WEBHOOK_LOCATOR} webhook."
curl -v -u "palantir:${ANTITHESIS_WEBHOOK_PASSWORD}" -X POST https://palantir.antithesis.com/api/v1/launch_experiment/${WEBHOOK_LOCATOR} -d \
'{ "params": {
    "antithesis.images":"palantirtechnologies/cassandra:latest;palantirtechnologies/timelock-server-distribution:latest;palantirtechnologies/atlasdb-workload-server-distribution:latest",
    "antithesis.config_image":"palantirtechnologies/atlasdb-workload-server-antithesis:latest",
    "custom.duration":"'${DURATION}'",
    "antithesis.report.recipients":"'${ANTITHESIS_REPORT_RECIPIENT}'"
} }'
