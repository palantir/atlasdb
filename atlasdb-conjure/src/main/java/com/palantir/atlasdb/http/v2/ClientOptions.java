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

import com.palantir.atlasdb.config.ServerListConfig;
import com.palantir.conjure.java.api.config.service.ServiceConfiguration;
import com.palantir.conjure.java.client.config.ClientConfiguration;
import com.palantir.conjure.java.client.config.ClientConfigurations;
import com.palantir.conjure.java.config.ssl.TrustContext;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;

@Value.Immutable
public abstract class ClientOptions {
    // TODO (jkong): Re-enable client QoS after response body leaks are handled correctly.
    // Throws after expected outages of 1/2 * 0.01 * (2^11 - 1) = 10.24 s
    public static final ClientOptions DEFAULT_RETRYING = ImmutableClientOptions.builder()
            .connectTimeout(Duration.ofSeconds(10))
            .readTimeout(Duration.ofSeconds(65))
            .backoffSlotSize(Duration.ofMillis(10))
            .failedUrlCooldown(Duration.ofMillis(100))
            .maxNumRetries(13)
            .clientQoS(ClientConfiguration.ClientQoS.DANGEROUS_DISABLE_SYMPATHETIC_CLIENT_QOS)
            .build();

    // TODO (jkong): Re-enable client QoS after response body leaks are handled correctly.
    public static final ClientOptions DEFAULT_NO_RETRYING = ImmutableClientOptions.builder()
            .connectTimeout(Duration.ofSeconds(10))
            .readTimeout(Duration.ofSeconds(65))
            .backoffSlotSize(Duration.ofMillis(100))
            .failedUrlCooldown(Duration.ofMillis(1))
            .maxNumRetries(0)
            .clientQoS(ClientConfiguration.ClientQoS.DANGEROUS_DISABLE_SYMPATHETIC_CLIENT_QOS)
            .build();

    public static final ClientOptions FAST_RETRYING_FOR_TEST = ImmutableClientOptions.builder()
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
}
