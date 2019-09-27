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
package com.palantir.atlasdb.http;

import java.util.Optional;
import java.util.function.Supplier;

import com.codahale.metrics.MetricRegistry;
import com.google.common.annotations.VisibleForTesting;
import com.palantir.atlasdb.config.AuxiliaryRemotingParameters;
import com.palantir.atlasdb.config.ServerListConfig;
import com.palantir.atlasdb.http.v2.ConjureJavaRuntimeTargetFactory;
import com.palantir.atlasdb.util.AtlasDbMetrics;
import com.palantir.conjure.java.config.ssl.TrustContext;
import com.palantir.tritium.metrics.registry.TaggedMetricRegistry;

public final class AtlasDbHttpClients {
    public static final TargetFactory DEFAULT_TARGET_FACTORY = AtlasDbFeignTargetFactory.DEFAULT;
    private static final TargetFactory TESTING_TARGET_FACTORY = AtlasDbFeignTargetFactory.FAST_FAIL_FOR_TESTING;

    private AtlasDbHttpClients() {
        // Utility class
    }

    public static <T> T createProxy(
            MetricRegistry metricRegistry,
            Optional<TrustContext> trustContext,
            String uri,
            Class<T> type,
            AuxiliaryRemotingParameters parameters) {
        return AtlasDbMetrics.instrument(
                metricRegistry,
                type,
                DEFAULT_TARGET_FACTORY.createProxy(trustContext, uri, type, parameters).instance(),
                MetricRegistry.name(type));
    }

    /**
     * Constructs an HTTP-invoking dynamic proxy for the specified type that will cycle through the list of supplied
     * endpoints after encountering an exception or connection failure, using the supplied SSL factory if it is
     * present. Also use the supplied the proxy selector to set the proxy on the clients if present.
     * <p>
     * Failover will continue to cycle through the supplied endpoint list indefinitely.
     */
    public static <T> T createProxyWithFailover(
            MetricRegistry metricRegistry,
            ServerListConfig serverListConfig,
            Class<T> type,
            AuxiliaryRemotingParameters parameters) {
        return AtlasDbMetrics.instrument(
                metricRegistry,
                type,
                DEFAULT_TARGET_FACTORY.createProxyWithFailover(serverListConfig, type, parameters).instance(),
                MetricRegistry.name(type));
    }

    public static <T> T createLiveReloadingProxyWithFailover(
            TaggedMetricRegistry taggedMetricRegistry,
            Supplier<ServerListConfig> serverListConfigSupplier,
            Class<T> type,
            AuxiliaryRemotingParameters clientParameters) {
        return VersionSelectingClients.createVersionSelectingClient(
                taggedMetricRegistry,
                // TODO (jkong): Replace the new client with the CJR one; also I wish there was a way to curry stuff
                ConjureJavaRuntimeTargetFactory.DEFAULT.createLiveReloadingProxyWithFailover(
                        serverListConfigSupplier,
                        type,
                        clientParameters),
                DEFAULT_TARGET_FACTORY.createLiveReloadingProxyWithFailover(
                        serverListConfigSupplier,
                        type,
                        clientParameters),
                () -> clientParameters.remotingClientConfig().get().maximumConjureRemotingProbability(),
                type);
    }

    @VisibleForTesting
    static <T> T createLiveReloadingProxyWithQuickFailoverForTesting(
            MetricRegistry metricRegistry,
            Supplier<ServerListConfig> serverListConfigSupplier,
            Class<T> type,
            AuxiliaryRemotingParameters parameters) {
        return AtlasDbMetrics.instrument(
                metricRegistry,
                type,
                TESTING_TARGET_FACTORY.createLiveReloadingProxyWithFailover(
                        serverListConfigSupplier,
                        type,
                        parameters).instance(),
                MetricRegistry.name(type));
    }

    @VisibleForTesting
    static <T> T createProxyWithQuickFailoverForTesting(
            MetricRegistry metricRegistry,
            ServerListConfig serverListConfig,
            Class<T> type,
            AuxiliaryRemotingParameters parameters) {
        return AtlasDbMetrics.instrument(
                metricRegistry,
                type,
                TESTING_TARGET_FACTORY.createProxyWithFailover(
                        serverListConfig,
                        type,
                        parameters).instance(),
                MetricRegistry.name(type, "atlasdb-testing"));
    }
}
