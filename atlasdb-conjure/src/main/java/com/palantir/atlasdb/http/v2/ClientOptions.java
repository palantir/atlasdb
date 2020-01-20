/*
 * (c) Copyright 2019 Palantir Technologies Inc. All rights reserved.
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

import java.net.ProxySelector;
import java.time.Duration;
import java.util.List;
import java.util.Optional;

import org.immutables.value.Value;

import com.google.common.annotations.VisibleForTesting;
import com.palantir.atlasdb.config.AuxiliaryRemotingParameters;
import com.palantir.atlasdb.config.ServerListConfig;
import com.palantir.conjure.java.api.config.service.ServiceConfiguration;
import com.palantir.conjure.java.client.config.ClientConfiguration;
import com.palantir.conjure.java.client.config.ClientConfigurations;
import com.palantir.conjure.java.config.ssl.TrustContext;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;

@Value.Immutable
public abstract class ClientOptions {
    private static final Duration CONNECT_TIMEOUT = Duration.ofMillis(500);

    @VisibleForTesting
    static final Duration NON_BLOCKING_READ_TIMEOUT = Duration.ofMillis(12566); // Odd number for debugging
    static final Duration BLOCKING_READ_TIMEOUT = Duration.ofSeconds(65);

    private static final Duration STANDARD_BACKOFF_SLOT_SIZE = Duration.ofMillis(10);

    private static final int STANDARD_MAX_RETRIES = 13;
    private static final int NO_RETRIES = 0;

    private static final Duration STANDARD_FAILED_URL_COOLDOWN = Duration.ofMillis(100);
    private static final Duration NON_RETRY_FAILED_URL_COOLDOWN = Duration.ofMillis(1);

    static final ClientOptions FAST_RETRYING_FOR_TEST = ImmutableClientOptions.builder()
            .connectTimeout(Duration.ofMillis(100))
            .readTimeout(Duration.ofSeconds(65))
            .backoffSlotSize(Duration.ofMillis(5))
            .failedUrlCooldown(Duration.ofMillis(1))
            .maxNumRetries(5)
            .clientQoS(ClientConfiguration.ClientQoS.DANGEROUS_DISABLE_SYMPATHETIC_CLIENT_QOS)
            .build();

    public abstract Duration connectTimeout();
    public abstract Duration readTimeout();
    public abstract Duration backoffSlotSize();
    public abstract Duration failedUrlCooldown();
    public abstract int maxNumRetries();
    @Value.Default
    public ClientConfiguration.ClientQoS clientQoS() {
        return ClientConfiguration.ClientQoS.ENABLED;
    }
    @Value.Default
    public ClientConfiguration.ServerQoS serverQoS() {
        return ClientConfiguration.ServerQoS.AUTOMATIC_RETRY;
    }

    public ClientConfiguration serverListToClient(ServerListConfig serverConfig) {
        ServiceConfiguration partialConfig = ServiceConfiguration.builder()
                .security(serverConfig.sslConfiguration().orElseThrow(
                        () -> new SafeIllegalStateException("CJR must be configured with SSL",
                                SafeArg.of("serverConfig", serverConfig))))
                .addAllUris(serverConfig.servers())
                .proxy(serverConfig.proxyConfiguration())
                .connectTimeout(connectTimeout())
                .readTimeout(readTimeout())
                .backoffSlotSize(backoffSlotSize())
                .maxNumRetries(maxNumRetries())
                .build();
        return ClientConfiguration.builder()
                .from(ClientConfigurations.of(partialConfig))
                .failedUrlCooldown(failedUrlCooldown())
                .enableGcmCipherSuites(true)
                .fallbackToCommonNameVerification(true)
                .clientQoS(clientQoS())
                .build();
    }

    public ClientConfiguration create(
            List<String> endpointUris,
            Optional<ProxySelector> proxy,
            TrustContext trustContext) {
        ClientConfiguration partialConfig = ClientConfigurations.of(endpointUris,
                trustContext.sslSocketFactory(), trustContext.x509TrustManager());

        ClientConfiguration.Builder builder = ClientConfiguration.builder().from(partialConfig)
                .connectTimeout(connectTimeout())
                .readTimeout(readTimeout())
                .backoffSlotSize(backoffSlotSize())
                .failedUrlCooldown(failedUrlCooldown())
                .maxNumRetries(maxNumRetries())
                .enableGcmCipherSuites(true)
                .fallbackToCommonNameVerification(true)
                .clientQoS(clientQoS())
                .serverQoS(serverQoS());

        proxy.ifPresent(builder::proxy);
        return builder.build();
    }

    public static ClientOptions of(Duration connect, Duration read, Duration backoff, Duration coolDown, int retries) {
        return ImmutableClientOptions.builder()
                .connectTimeout(connect)
                .readTimeout(read)
                .backoffSlotSize(backoff)
                .failedUrlCooldown(coolDown)
                .maxNumRetries(retries)
                .build();
    }

    static ClientOptions fromRemotingParameters(AuxiliaryRemotingParameters parameters) {
        ImmutableClientOptions.Builder builder = ImmutableClientOptions.builder();

        setupTimeouts(builder, parameters);
        setupRetrying(builder, parameters);

        return builder.clientQoS(ClientConfiguration.ClientQoS.DANGEROUS_DISABLE_SYMPATHETIC_CLIENT_QOS)
                .build();
    }

    /**
     * If {@link AuxiliaryRemotingParameters#shouldSupportBlockingOperations()} is configured to true, sets the read
     * timeout of operations to {@link ClientOptions#BLOCKING_READ_TIMEOUT}; otherwise sets this to
     * {@link ClientOptions#NON_BLOCKING_READ_TIMEOUT}. This controls how long the client waits to receive the first
     * byte from the server before giving up, so in general read timeouts should not be set to less than what is
     * considered an acceptable time for the server to give a suitable response.
     *
     * Connect timeouts are not affected by the value of {@param parameters}.
     */
    private static void setupTimeouts(ImmutableClientOptions.Builder builder, AuxiliaryRemotingParameters parameters) {
        builder.connectTimeout(CONNECT_TIMEOUT)
                .readTimeout(parameters.shouldSupportBlockingOperations()
                        ? BLOCKING_READ_TIMEOUT : NON_BLOCKING_READ_TIMEOUT);
    }

    private static void setupRetrying(ImmutableClientOptions.Builder builder, AuxiliaryRemotingParameters parameters) {
        builder.backoffSlotSize(STANDARD_BACKOFF_SLOT_SIZE)
                .maxNumRetries(parameters.shouldRetry() ? STANDARD_MAX_RETRIES : NO_RETRIES)
                .failedUrlCooldown(parameters.shouldRetry() ? STANDARD_FAILED_URL_COOLDOWN
                        : NON_RETRY_FAILED_URL_COOLDOWN);
    }
}
