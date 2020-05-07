/*
 * (c) Copyright 2020 Palantir Technologies Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.palantir.atlasdb.http.v2;

import java.time.Duration;
import java.util.Map;

import com.google.common.annotations.VisibleForTesting;
import com.palantir.atlasdb.config.AuxiliaryRemotingParameters;
import com.palantir.atlasdb.config.ServerListConfig;
import com.palantir.common.streams.KeyedStream;
import com.palantir.conjure.java.api.config.service.HumanReadableDuration;
import com.palantir.conjure.java.api.config.service.PartialServiceConfiguration;
import com.palantir.conjure.java.api.config.service.ServicesConfigBlock;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;

public final class DialogueClientOptions {
    // Under standard settings, throws after expected outages of 1/2 * 0.01 * (2^13 - 1) = 40.96 s
    private static final HumanReadableDuration STANDARD_BACKOFF_SLOT_SIZE = HumanReadableDuration.milliseconds(10);
    private static final int STANDARD_MAX_RETRIES = 13;
    private static final int NO_RETRIES = 0;

    private static final HumanReadableDuration CONNECT_TIMEOUT = HumanReadableDuration.milliseconds(500);

    // The read timeout controls how long the client waits to receive the first byte from the server before giving up,
    // so in general read timeouts should not be set to less than what is considered an acceptable time for the server
    // to give a suitable response.
    // In the context of TimeLock, this timeout must be longer than how long an AwaitingLeadershipProxy takes to
    // decide whether a node is the leader and still has a quorum.
    // This is an odd number for debugging.
    private static final HumanReadableDuration NON_BLOCKING_READ_TIMEOUT = HumanReadableDuration.milliseconds(12566);

    // Should not be reduced below 65 seconds to support workflows involving locking.
    private static final HumanReadableDuration BLOCKING_READ_TIMEOUT = HumanReadableDuration.seconds(65);

    private DialogueClientOptions() {
        // No
    }

    static ServicesConfigBlock toServicesConfigBlock(
            Map<String, RemoteServiceConfiguration> serviceNameToRemoteConfiguration) {
        Map<String, PartialServiceConfiguration> configMap = KeyedStream.stream(serviceNameToRemoteConfiguration)
                .map(remoteServiceConfiguration -> toPartialServiceConfiguration(
                        remoteServiceConfiguration.serverList(), remoteServiceConfiguration.remotingParameters()))
                .collectToMap();
        return ServicesConfigBlock.builder().putAllServices(configMap).build();
    }

    private static PartialServiceConfiguration toPartialServiceConfiguration(
            ServerListConfig serverListConfig,
            AuxiliaryRemotingParameters remotingParameters) {
        return populateConfigurationWithAtlasDbDefaults()
                .addAllUris(serverListConfig.servers())
                .proxyConfiguration(serverListConfig.proxyConfiguration())
                .security(serverListConfig.sslConfiguration()
                        .orElseThrow(() -> new SafeIllegalStateException(
                                "Dialogue must be configured with SSL.")))
                .maxNumRetries(remotingParameters.shouldRetry() ? NO_RETRIES : STANDARD_MAX_RETRIES)
                .readTimeout(remotingParameters.shouldSupportBlockingOperations()
                        ? BLOCKING_READ_TIMEOUT
                        : NON_BLOCKING_READ_TIMEOUT)
                .build();
    }

    private static PartialServiceConfiguration.Builder populateConfigurationWithAtlasDbDefaults() {
        return PartialServiceConfiguration.builder()
                .connectTimeout(CONNECT_TIMEOUT)
                .backoffSlotSize(STANDARD_BACKOFF_SLOT_SIZE)
                .enableGcmCipherSuites(true);
    }
}