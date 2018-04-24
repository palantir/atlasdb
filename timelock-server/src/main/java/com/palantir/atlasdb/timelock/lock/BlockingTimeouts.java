/*
 * (c) Copyright 2017 Palantir Technologies Inc. All rights reserved.
 *
 * Licensed under the BSD-3 License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://opensource.org/licenses/BSD-3-Clause
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.palantir.atlasdb.timelock.lock;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.palantir.atlasdb.timelock.config.TimeLimiterConfiguration;
import com.palantir.atlasdb.timelock.config.TimeLockServerConfiguration;

import io.dropwizard.util.Duration;

public final class BlockingTimeouts {
    private static final Logger log = LoggerFactory.getLogger(BlockingTimeouts.class);

    public static final long DEFAULT_IDLE_TIMEOUT = 30 * 1000;

    private static final String SERVER = "server";
    private static final String APPLICATION_CONNECTORS = "applicationConnectors";
    private static final String IDLE_TIMEOUT = "idleTimeout";

    private BlockingTimeouts() {
        // utility
    }

    public static long getBlockingTimeout(ObjectMapper mapper, TimeLockServerConfiguration configuration) {
        // TODO (jkong): Need to decide if this is worth the pain to enforce our invariant, or if we should just
        // let users do it manually!
        Preconditions.checkNotNull(mapper, "ObjectMapper should not be null");
        Preconditions.checkNotNull(configuration, "TimeLockServerConfiguration should not be null");
        try {
            List<Map<String, String>> connectorData = getConnectorData(mapper, configuration);
            long minimumTimeout = getMinimumTimeout(connectorData);
            return scaleForErrorMargin(minimumTimeout, getErrorMargin(configuration));
        } catch (IOException e) {
            return scaleForErrorMargin(logAndGetDefault(), getErrorMargin(configuration));
        }
    }

    @SuppressWarnings("unchecked") // Any map can be deserialized as Map<String, String>.
    private static List<Map<String, String>> getConnectorData(ObjectMapper mapper,
            TimeLockServerConfiguration configuration) throws JsonProcessingException {
        JsonNode configurationJson = mapper.valueToTree(configuration);
        JsonNode connectorJson = configurationJson.get(SERVER).get(APPLICATION_CONNECTORS);
        return Arrays.asList(mapper.treeToValue(connectorJson, Map[].class));
    }

    private static long getMinimumTimeout(List<Map<String, String>> connectorData) {
        Optional<Long> minimum = connectorData.stream()
                .map(connectorDatum -> connectorDatum.get(IDLE_TIMEOUT))
                .map(idleTimeout -> Duration.parse(idleTimeout).toMilliseconds())
                .min(Long::compareTo);

        if (minimum.isPresent()) {
            return minimum.get();
        }
        return logAndGetDefault();
    }

    private static long logAndGetDefault() {
        log.warn("Couldn't figure out the idle timeout from configuration. Will assume this to be the"
                + " default of {} ms", DEFAULT_IDLE_TIMEOUT);
        return DEFAULT_IDLE_TIMEOUT;
    }

    private static double getErrorMargin(TimeLockServerConfiguration configuration) {
        TimeLimiterConfiguration timeLimiterConfiguration = configuration.timeLimiterConfiguration();
        Preconditions.checkState(timeLimiterConfiguration.enableTimeLimiting(),
                "Tried to get the error margin on a configuration where time limiting was not enabled");
        return configuration.timeLimiterConfiguration().blockingTimeoutErrorMargin();
    }

    @VisibleForTesting
    static long scaleForErrorMargin(long idleTimeout, double errorMargin) {
        // The reason for this method is that we want to be able to tell the client to retry on us *before* Jetty
        // kills our thread. If we lose the race this is not a problem though; the server will retry on the same node
        // anyway. Even if this happens 3 times in a row we are fine, since we will fail over to non-leaders and they
        // will redirect us back.
        return Math.round(idleTimeout * (1 - errorMargin));
    }
}
