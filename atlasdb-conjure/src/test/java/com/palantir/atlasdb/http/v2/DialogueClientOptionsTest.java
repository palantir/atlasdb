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

import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.palantir.atlasdb.config.AuxiliaryRemotingParameters;
import com.palantir.atlasdb.config.ImmutableServerListConfig;
import com.palantir.atlasdb.config.ServerListConfig;
import com.palantir.conjure.java.api.config.service.PartialServiceConfiguration;
import com.palantir.conjure.java.api.config.service.ServicesConfigBlock;
import com.palantir.conjure.java.api.config.service.UserAgent;
import com.palantir.conjure.java.api.config.ssl.SslConfiguration;
import java.nio.file.Paths;
import java.util.List;
import org.junit.Test;

public class DialogueClientOptionsTest {
    private static final String SERVICE_NAME = "service";
    private static final UserAgent USER_AGENT = UserAgent.of(UserAgent.Agent.of(SERVICE_NAME, "1.2.3"));
    private static final List<String> SERVERS = ImmutableList.of("apple", "banana", "cherry", "dewberry");
    private static final SslConfiguration SSL_CONFIGURATION = SslConfiguration.of(Paths.get("a", "b"));

    private static final AuxiliaryRemotingParameters REMOTING_PARAMETERS_EXTENDED_TIMEOUT
            = AuxiliaryRemotingParameters.builder()
            .userAgent(USER_AGENT)
            .shouldRetry(true)
            .shouldLimitPayload(true)
            .shouldUseExtendedTimeout(true)
            .build();
    private static final AuxiliaryRemotingParameters REMOTING_PARAMETERS_SHORT_TIMEOUT
            = AuxiliaryRemotingParameters.builder()
            .userAgent(USER_AGENT)
            .shouldRetry(true)
            .shouldLimitPayload(true)
            .shouldUseExtendedTimeout(false)
            .build();

    private static final ServerListConfig SERVER_LIST_CONFIG = ImmutableServerListConfig.builder()
            .sslConfiguration(SSL_CONFIGURATION)
            .addAllServers(SERVERS)
            .build();

    private static final RemoteServiceConfiguration REMOTE_SERVICE_CONFIGURATION_EXTENDED_TIMEOUT
            = ImmutableRemoteServiceConfiguration.builder()
            .serverList(SERVER_LIST_CONFIG)
            .remotingParameters(REMOTING_PARAMETERS_EXTENDED_TIMEOUT)
            .build();
    private static final RemoteServiceConfiguration REMOTE_SERVICE_CONFIGURATION_SHORT_TIMEOUT
            = ImmutableRemoteServiceConfiguration.builder()
            .serverList(SERVER_LIST_CONFIG)
            .remotingParameters(REMOTING_PARAMETERS_SHORT_TIMEOUT)
            .build();

    @Test
    public void serviceConfigBlockGeneration() {
        ServicesConfigBlock servicesConfigBlock = DialogueClientOptions.toServicesConfigBlock(
                ImmutableMap.of(SERVICE_NAME, REMOTE_SERVICE_CONFIGURATION_EXTENDED_TIMEOUT));

        assertThat(servicesConfigBlock.services()).containsOnlyKeys(SERVICE_NAME);

        PartialServiceConfiguration partialServiceConfiguration = servicesConfigBlock.services().get(SERVICE_NAME);
        assertThat(partialServiceConfiguration.uris()).hasSameElementsAs(SERVERS);
        assertThat(partialServiceConfiguration.security()).contains(SSL_CONFIGURATION);
        assertThat(partialServiceConfiguration.proxyConfiguration()).isEmpty();
        assertThat(partialServiceConfiguration.backoffSlotSize())
                .contains(ClientOptionsConstants.STANDARD_BACKOFF_SLOT_SIZE);
        assertThat(partialServiceConfiguration.connectTimeout()).contains(ClientOptionsConstants.CONNECT_TIMEOUT);
        assertThat(partialServiceConfiguration.maxNumRetries()).contains(ClientOptionsConstants.STANDARD_MAX_RETRIES);
        assertThat(partialServiceConfiguration.readTimeout()).contains(ClientOptionsConstants.LONG_READ_TIMEOUT);
    }

    @Test
    public void differentlyKeyedServicesTreatedDifferently() {
        String otherServiceName = "tom";
        ServicesConfigBlock servicesConfigBlock = DialogueClientOptions.toServicesConfigBlock(
                ImmutableMap.of(
                        SERVICE_NAME, REMOTE_SERVICE_CONFIGURATION_EXTENDED_TIMEOUT,
                        otherServiceName, REMOTE_SERVICE_CONFIGURATION_SHORT_TIMEOUT));

        assertThat(servicesConfigBlock.services()).containsOnlyKeys(SERVICE_NAME, otherServiceName);

        PartialServiceConfiguration partialServiceConfiguration = servicesConfigBlock.services().get(SERVICE_NAME);
        assertThat(partialServiceConfiguration.readTimeout()).contains(ClientOptionsConstants.LONG_READ_TIMEOUT);

        PartialServiceConfiguration otherPartialServiceConfiguration
                = servicesConfigBlock.services().get(otherServiceName);
        assertThat(otherPartialServiceConfiguration.readTimeout()).contains(ClientOptionsConstants.SHORT_READ_TIMEOUT);
    }
}
