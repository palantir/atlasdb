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

import com.palantir.atlasdb.config.AuxiliaryRemotingParameters;
import com.palantir.atlasdb.config.ServerListConfig;
import com.palantir.conjure.java.api.config.service.ServiceConfiguration;
import com.palantir.conjure.java.client.config.ClientConfiguration;
import com.palantir.conjure.java.client.config.ClientConfigurations;
import com.palantir.conjure.java.config.ssl.TrustContext;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;
import java.net.ProxySelector;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import org.immutables.value.Value;

@Value.Immutable
public abstract class ClientOptions {

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
                .security(serverConfig
                        .sslConfiguration()
                        .orElseThrow(() -> new SafeIllegalStateException(
                                "CJR must be configured with SSL", SafeArg.of("serverConfig", serverConfig))))
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
                .enableHttp2(true)
                .clientQoS(clientQoS())
                .build();
    }

    public ClientConfiguration create(
            List<String> endpointUris, Optional<ProxySelector> proxy, TrustContext trustContext) {
        ClientConfiguration partialConfig =
                ClientConfigurations.of(endpointUris, trustContext.sslSocketFactory(), trustContext.x509TrustManager());

        ClientConfiguration.Builder builder = ClientConfiguration.builder()
                .from(partialConfig)
                .connectTimeout(connectTimeout())
                .readTimeout(readTimeout())
                .backoffSlotSize(backoffSlotSize())
                .failedUrlCooldown(failedUrlCooldown())
                .maxNumRetries(maxNumRetries())
                .enableGcmCipherSuites(true)
                .enableHttp2(true)
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

    private static void setupTimeouts(ImmutableClientOptions.Builder builder, AuxiliaryRemotingParameters parameters) {
        builder.connectTimeout(ClientOptionsConstants.CONNECT_TIMEOUT.toJavaDuration())
                .readTimeout(
                        parameters.shouldUseExtendedTimeout()
                                ? ClientOptionsConstants.LONG_READ_TIMEOUT.toJavaDuration()
                                : ClientOptionsConstants.SHORT_READ_TIMEOUT.toJavaDuration());
    }

    private static void setupRetrying(ImmutableClientOptions.Builder builder, AuxiliaryRemotingParameters parameters) {
        builder.backoffSlotSize(ClientOptionsConstants.STANDARD_BACKOFF_SLOT_SIZE.toJavaDuration())
                .maxNumRetries(
                        parameters.definitiveRetryIndication()
                                ? ClientOptionsConstants.STANDARD_MAX_RETRIES
                                : ClientOptionsConstants.NO_RETRIES)
                .failedUrlCooldown(
                        parameters.definitiveRetryIndication()
                                ? ClientOptionsConstants.STANDARD_FAILED_URL_COOLDOWN.toJavaDuration()
                                : ClientOptionsConstants.NON_RETRY_FAILED_URL_COOLDOWN.toJavaDuration());
    }
}
