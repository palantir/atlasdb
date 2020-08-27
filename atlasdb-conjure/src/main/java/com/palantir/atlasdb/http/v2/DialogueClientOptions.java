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

import com.palantir.atlasdb.config.AuxiliaryRemotingParameters;
import com.palantir.atlasdb.config.ServerListConfig;
import com.palantir.common.streams.KeyedStream;
import com.palantir.conjure.java.api.config.service.PartialServiceConfiguration;
import com.palantir.conjure.java.api.config.service.ServicesConfigBlock;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;
import java.util.Map;

public final class DialogueClientOptions {
    private DialogueClientOptions() {
        // No
    }

    public static ServicesConfigBlock toServicesConfigBlock(
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
                .maxNumRetries(remotingParameters.definitiveRetryIndication()
                        ? ClientOptionsConstants.STANDARD_MAX_RETRIES
                        : ClientOptionsConstants.NO_RETRIES)
                .readTimeout(remotingParameters.shouldUseExtendedTimeout()
                        ? ClientOptionsConstants.LONG_READ_TIMEOUT
                        : ClientOptionsConstants.SHORT_READ_TIMEOUT)
                .build();
    }

    private static PartialServiceConfiguration.Builder populateConfigurationWithAtlasDbDefaults() {
        return PartialServiceConfiguration.builder()
                .connectTimeout(ClientOptionsConstants.CONNECT_TIMEOUT)
                .backoffSlotSize(ClientOptionsConstants.STANDARD_BACKOFF_SLOT_SIZE)
                .enableGcmCipherSuites(true);
    }
}
