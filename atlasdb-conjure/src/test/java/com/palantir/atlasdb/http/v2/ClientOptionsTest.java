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

import java.time.Duration;

import org.junit.Test;

import com.palantir.atlasdb.config.AuxiliaryRemotingParameters;
import com.palantir.conjure.java.api.config.service.UserAgent;
import com.palantir.conjure.java.client.config.ClientConfiguration;

public class ClientOptionsTest {
    private static final UserAgent USER_AGENT = UserAgent.of(UserAgent.Agent.of("tom", "1.2.3"));

    // Throws after expected outages of 1/2 * 0.01 * (2^13 - 1) = 40.96 s
    private static final ClientOptions DEFAULT_RETRYING = ImmutableClientOptions.builder()
            .connectTimeout(Duration.ofMillis(500))
            .readTimeout(Duration.ofSeconds(65))
            .backoffSlotSize(Duration.ofMillis(10))
            .failedUrlCooldown(Duration.ofMillis(100))
            .maxNumRetries(13)
            .clientQoS(ClientConfiguration.ClientQoS.DANGEROUS_DISABLE_SYMPATHETIC_CLIENT_QOS)
            .build();

    private static final ClientOptions DEFAULT_NO_RETRYING = ImmutableClientOptions.builder()
            .connectTimeout(Duration.ofMillis(500))
            .readTimeout(Duration.ofSeconds(65))
            .backoffSlotSize(Duration.ofMillis(10))
            .failedUrlCooldown(Duration.ofMillis(1))
            .maxNumRetries(0)
            .clientQoS(ClientConfiguration.ClientQoS.DANGEROUS_DISABLE_SYMPATHETIC_CLIENT_QOS)
            .build();

    @Test
    public void proxyShouldSupportBlockingReadTimeoutIfUnspecified() {
        ClientOptions clientOptions = ClientOptions.fromRemotingParameters(AuxiliaryRemotingParameters.builder()
                .shouldLimitPayload(true)
                .shouldRetry(true)
                .userAgent(USER_AGENT)
                .build());

        assertThat(clientOptions.readTimeout()).isEqualTo(ClientOptions.BLOCKING_READ_TIMEOUT);
    }

    @Test
    public void proxyShouldSupportBlockingReadTimeoutIfExplicitlyConfigured() {
        ClientOptions clientOptions = ClientOptions.fromRemotingParameters(AuxiliaryRemotingParameters.builder()
                .shouldLimitPayload(true)
                .shouldRetry(true)
                .shouldSupportBlockingOperations(true)
                .userAgent(USER_AGENT)
                .build());

        assertThat(clientOptions.readTimeout()).isEqualTo(ClientOptions.BLOCKING_READ_TIMEOUT);
    }

    @Test
    public void proxyShouldSupportNonBlockingReadTimeoutIfExplicitlyConfigured() {
        ClientOptions clientOptions = ClientOptions.fromRemotingParameters(AuxiliaryRemotingParameters.builder()
                .shouldLimitPayload(true)
                .shouldRetry(true)
                .shouldSupportBlockingOperations(false)
                .userAgent(USER_AGENT)
                .build());

        assertThat(clientOptions.readTimeout()).isEqualTo(ClientOptions.NON_BLOCKING_READ_TIMEOUT);
    }

    @Test
    public void defaultRetryingOptionsShouldMatchLegacyBehaviour() {
        ClientOptions clientOptions = ClientOptions.fromRemotingParameters(AuxiliaryRemotingParameters.builder()
                .shouldLimitPayload(true)
                .shouldRetry(true)
                .userAgent(USER_AGENT)
                .build());

        assertThat(clientOptions).isEqualTo(DEFAULT_RETRYING);
    }

    @Test
    public void defaultNonRetryingOptionsShouldMatchLegacyBehaviour() {
        ClientOptions clientOptions = ClientOptions.fromRemotingParameters(AuxiliaryRemotingParameters.builder()
                .shouldLimitPayload(true)
                .shouldRetry(false)
                .userAgent(USER_AGENT)
                .build());

        assertThat(clientOptions).isEqualTo(DEFAULT_NO_RETRYING);
    }
}
