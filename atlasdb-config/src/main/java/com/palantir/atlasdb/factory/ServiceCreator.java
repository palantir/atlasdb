/*
 * (c) Copyright 2018 Palantir Technologies Inc. All rights reserved.
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
package com.palantir.atlasdb.factory;

import com.codahale.metrics.MetricRegistry;
import com.palantir.atlasdb.config.AuxiliaryRemotingParameters;
import com.palantir.atlasdb.config.ImmutableAuxiliaryRemotingParameters;
import com.palantir.atlasdb.config.RemotingClientConfig;
import com.palantir.atlasdb.config.ServerListConfig;
import com.palantir.atlasdb.http.AtlasDbHttpClients;
import com.palantir.atlasdb.util.AtlasDbMetrics;
import com.palantir.atlasdb.util.MetricsManager;
import com.palantir.conjure.java.api.config.service.UserAgent;
import com.palantir.conjure.java.api.config.ssl.SslConfiguration;
import com.palantir.conjure.java.config.ssl.SslSocketFactories;
import com.palantir.conjure.java.config.ssl.TrustContext;
import java.util.Optional;
import java.util.function.Supplier;

public final class ServiceCreator {
    private final MetricsManager metricsManager;
    private final Supplier<ServerListConfig> servers;
    private final AuxiliaryRemotingParameters parameters;

    private ServiceCreator(MetricsManager metricsManager,
            Supplier<ServerListConfig> servers,
            AuxiliaryRemotingParameters parameters) {
        this.metricsManager = metricsManager;
        this.servers = servers;
        this.parameters = parameters;
    }

    /**
     * Creates clients without client-side restrictions on payload size.
     */
    public static ServiceCreator noPayloadLimiter(
            MetricsManager metrics,
            Supplier<ServerListConfig> serverList,
            UserAgent userAgent,
            Supplier<RemotingClientConfig> remotingClientConfigSupplier) {
        return new ServiceCreator(
                metrics, serverList, toAuxiliaryRemotingParameters(userAgent, remotingClientConfigSupplier, false));
    }

    /**
     * Creates clients that intercept requests with payload greater than
     * {@link com.palantir.atlasdb.http.AtlasDbInterceptors#MAX_PAYLOAD_SIZE} bytes. This ServiceCreator should be used
     * for clients to servers that impose payload limits.
     */
    public static ServiceCreator withPayloadLimiter(
            MetricsManager metrics,
            Supplier<ServerListConfig> serverList,
            UserAgent userAgent,
            Supplier<RemotingClientConfig> remotingClientConfigSupplier) {
        return new ServiceCreator(
                metrics, serverList, toAuxiliaryRemotingParameters(userAgent, remotingClientConfigSupplier, true));
    }

    public <T> T createService(Class<T> serviceClass) {
        return create(metricsManager, servers, serviceClass, parameters);
    }

    public <T> T createServiceWithShortTimeout(Class<T> serviceClass) {
        AuxiliaryRemotingParameters blockingUnsupportedParameters
                = ImmutableAuxiliaryRemotingParameters.copyOf(parameters).withShouldUseExtendedTimeout(false);
        return create(metricsManager, servers, serviceClass, blockingUnsupportedParameters);
    }

    /**
     * Utility method for transforming an optional {@link SslConfiguration} into an optional {@link TrustContext}.
     */
    @SuppressWarnings("OptionalUsedAsFieldOrParameterType") // Just mapping
    public static Optional<TrustContext> createTrustContext(Optional<SslConfiguration> sslConfiguration) {
        return sslConfiguration.map(SslSocketFactories::createTrustContext);
    }

    private static <T> T create(
            MetricsManager metricsManager,
            Supplier<ServerListConfig> serverListConfigSupplier,
            Class<T> type,
            AuxiliaryRemotingParameters parameters) {
        return AtlasDbHttpClients.createLiveReloadingProxyWithFailover(
                metricsManager,
                serverListConfigSupplier,
                type,
                parameters);
    }

    public static <T> T instrumentService(MetricRegistry metricRegistry, T service, Class<T> serviceClass) {
        return AtlasDbMetrics.instrument(
                metricRegistry,
                serviceClass,
                service,
                MetricRegistry.name(serviceClass));
    }

    private static AuxiliaryRemotingParameters toAuxiliaryRemotingParameters(
            UserAgent userAgent,
            Supplier<RemotingClientConfig> remotingClientConfigSupplier,
            boolean shouldLimitPayload) {
        return AuxiliaryRemotingParameters.builder()
                .remotingClientConfig(remotingClientConfigSupplier)
                .userAgent(userAgent)
                .shouldLimitPayload(shouldLimitPayload)
                .shouldUseExtendedTimeout(true) // TODO (jkong): Figure out when to migrate safely
                .shouldRetry(true)
                .build();
    }
}
