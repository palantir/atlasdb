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

import java.util.Optional;
import java.util.function.Supplier;

import com.codahale.metrics.MetricRegistry;
import com.palantir.atlasdb.config.AuxiliaryRemotingParameters;
import com.palantir.atlasdb.config.ImmutableAuxiliaryRemotingParameters;
import com.palantir.atlasdb.config.RemotingClientConfig;
import com.palantir.atlasdb.config.ServerListConfig;
import com.palantir.atlasdb.http.AtlasDbHttpProtocolVersion;
import com.palantir.atlasdb.http.AtlasDbRemotingConstants;
import com.palantir.atlasdb.http.DialogueHttpServiceCreationStrategy;
import com.palantir.atlasdb.http.HttpServiceCreationStrategy;
import com.palantir.atlasdb.http.LegacyHttpServiceCreationStrategy;
import com.palantir.atlasdb.util.AtlasDbMetrics;
import com.palantir.atlasdb.util.MetricsManager;
import com.palantir.conjure.java.api.config.service.UserAgent;
import com.palantir.conjure.java.api.config.ssl.SslConfiguration;
import com.palantir.conjure.java.config.ssl.SslSocketFactories;
import com.palantir.conjure.java.config.ssl.TrustContext;
import com.palantir.dialogue.clients.DialogueClients;
import com.palantir.refreshable.Refreshable;

public final class ServiceCreator {
    private final HttpServiceCreationStrategy httpServiceCreationStrategy;
    private final AuxiliaryRemotingParameters parameters;

    private ServiceCreator(HttpServiceCreationStrategy httpServiceCreationStrategy,
            AuxiliaryRemotingParameters parameters) {
        this.httpServiceCreationStrategy = httpServiceCreationStrategy;
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
                // TODO(jkong): Consider plumbing this through. We're not supporting other strategies here yet though.
                new LegacyHttpServiceCreationStrategy(metrics, serverList),
                toAuxiliaryRemotingParameters(userAgent, remotingClientConfigSupplier, false));
    }

    /**
     * Creates clients that intercept requests with payload greater than
     * {@link com.palantir.atlasdb.http.AtlasDbInterceptors#MAX_PAYLOAD_SIZE} bytes. This ServiceCreator should be used
     * for clients to servers that impose payload limits.
     */
    public static ServiceCreator withPayloadLimiter(
            MetricsManager metrics,
            Refreshable<ServerListConfig> serverList,
            UserAgent userAgent,
            Supplier<RemotingClientConfig> remotingClientConfigSupplier) {
        return withPayloadLimiter(metrics, serverList, userAgent, remotingClientConfigSupplier, Optional.empty());
    }

    public static ServiceCreator withPayloadLimiter(
            MetricsManager metrics,
            Refreshable<ServerListConfig> serverList,
            UserAgent userAgent,
            Supplier<RemotingClientConfig> remotingClientConfigSupplier,
            Optional<DialogueClients.ReloadingFactory> reloadingFactory) {
        HttpServiceCreationStrategy legacyCreationStrategy = new LegacyHttpServiceCreationStrategy(metrics, serverList);
        AuxiliaryRemotingParameters parameters = toAuxiliaryRemotingParameters(
                userAgent, remotingClientConfigSupplier, true);

        HttpServiceCreationStrategy creationStrategyToUse = reloadingFactory
                .map(factory -> {
                    UserAgent agentWithHttpHeader
                            = userAgent.addAgent(AtlasDbRemotingConstants.ATLASDB_HTTP_CLIENT_AGENT);
                    return factory.withUserAgent(agentWithHttpHeader);
                })
                .<HttpServiceCreationStrategy>map(factory ->
                        new DialogueHttpServiceCreationStrategy(serverList, factory, legacyCreationStrategy))
                .orElse(legacyCreationStrategy);
        return new ServiceCreator(creationStrategyToUse, parameters);
    }

    public <T> T createService(Class<T> serviceClass) {
        return create(serviceClass, httpServiceCreationStrategy, parameters, serviceClass.getSimpleName());
    }

    public <T> T createServiceWithoutBlockingOperations(Class<T> serviceClass) {
        AuxiliaryRemotingParameters blockingUnsupportedParameters
                = ImmutableAuxiliaryRemotingParameters.copyOf(parameters).withShouldSupportBlockingOperations(false);
        return create(serviceClass, httpServiceCreationStrategy, blockingUnsupportedParameters,
                serviceClass.getSimpleName() + "-blocking");
    }

    /**
     * Utility method for transforming an optional {@link SslConfiguration} into an optional {@link TrustContext}.
     */
    @SuppressWarnings("OptionalUsedAsFieldOrParameterType") // Just mapping
    public static Optional<TrustContext> createTrustContext(Optional<SslConfiguration> sslConfiguration) {
        return sslConfiguration.map(SslSocketFactories::createTrustContext);
    }

    private static <T> T create(
            Class<T> type,
            HttpServiceCreationStrategy clientCreationStrategy,
            AuxiliaryRemotingParameters parameters,
            String serviceName) {
        return clientCreationStrategy.createLiveReloadingProxyWithFailover(type, parameters, serviceName);
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
                .shouldSupportBlockingOperations(true) // TODO (jkong): Figure out when to migrate safely
                .shouldRetry(true)
                .build();
    }
}
