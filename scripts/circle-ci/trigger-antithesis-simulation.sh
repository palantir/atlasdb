#!/bin/bash

if [[ -z "${ANTITHESIS_WEBHOOK_PASSWORD}" ]]; then
  echo "Antithesis webhook password is not set as an environment variable, exiting."
  exit 1
fi

# TODO(lmeireles): should update webhook locator to palantir_atlasdb__baseline__latest when we confirm test works.
WEBHOOK_LOCATOR="palantir_atlasdb__smoketest__latest"
echo "Triggering simulation on Antithesis via the ${WEBHOOK_LOCATOR} webhook."
curl -v -u "palantir:${ANTITHESIS_WEBHOOK_PASSWORD}" -X POST https://palantir.antithesis.com/api/v1/launch_experiment/${WEBHOOK_LOCATOR}
