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

import java.net.ProxySelector;
import java.util.Collection;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;

import com.codahale.metrics.MetricRegistry;
import com.google.common.annotations.VisibleForTesting;
import com.palantir.atlasdb.config.ServerListConfig;
import com.palantir.conjure.java.api.config.service.ProxyConfiguration;
import com.palantir.conjure.java.api.config.ssl.SslConfiguration;
import com.palantir.conjure.java.config.ssl.TrustContext;

public final class AtlasDbHttpClients {
    private static final AtlasDbProxyFactory proxyFactory = new LegacyAtlasDbProxyFactory();

    private AtlasDbHttpClients() {
        // Utility class
    }

    /**
     * Constructs a dynamic proxy for the specified type, using the supplied SSL factory if is present, and the
     * default Feign HTTP client.
     */
    public static <T> T createProxy(
            MetricRegistry metricRegistry,
            Optional<TrustContext> trustContext,
            String uri,
            Class<T> type) {
        return proxyFactory.createProxy(metricRegistry, trustContext, uri, type, UserAgents.DEFAULT_USER_AGENT, false);
    }

    public static <T> T createProxy(
            MetricRegistry metricRegistry,
            Optional<TrustContext> trustContext,
            String uri,
            Class<T> type,
            String userAgent,
            boolean limitPayloadSize) {
        return proxyFactory.createProxy(metricRegistry, trustContext, uri, type, userAgent, limitPayloadSize);
    }

    public static <T> T createProxyWithoutRetrying(
            MetricRegistry metricRegistry,
            Optional<TrustContext> trustContext,
            String uri,
            Class<T> type,
            String userAgent,
            boolean limitPayloadSize) {
        return proxyFactory.createProxyWithoutRetrying(
                metricRegistry, trustContext, uri, type, userAgent, limitPayloadSize);
    }

    public static <T> T createProxyWithFailover(
            MetricRegistry metricRegistry,
            Optional<TrustContext> trustContext,
            Optional<ProxySelector> proxySelector,
            Collection<String> endpointUris,
            String userAgent,
            Class<T> type) {
        return proxyFactory.createProxyWithFailover(
                metricRegistry, trustContext, proxySelector, endpointUris, userAgent, type);
    }

    public static <T> T createLiveReloadingProxyWithFailover(
            MetricRegistry metricRegistry,
            Supplier<ServerListConfig> serverListConfigSupplier,
            Function<SslConfiguration, TrustContext> trustContextCreator,
            Function<ProxyConfiguration, ProxySelector> proxySelectorCreator,
            Class<T> type,
            String userAgent,
            boolean limitPayload) {
        return proxyFactory.createLiveReloadingProxyWithFailover(
                metricRegistry,
                serverListConfigSupplier,
                trustContextCreator,
                proxySelectorCreator,
                type,
                userAgent,
                limitPayload);
    }

    @VisibleForTesting
    static <T> T createLiveReloadingProxyWithQuickFailoverForTesting(
            MetricRegistry metricRegistry,
            Supplier<ServerListConfig> serverListConfigSupplier,
            Function<SslConfiguration, TrustContext> trustContextCreator,
            Function<ProxyConfiguration, ProxySelector> proxySelectorCreator,
            Class<T> type,
            String userAgent) {
        return proxyFactory.createLiveReloadingProxyWithQuickFailoverForTesting(
                metricRegistry, serverListConfigSupplier, trustContextCreator, proxySelectorCreator, type, userAgent);
    }

    @VisibleForTesting
    static <T> T createProxyWithQuickFailoverForTesting(
            MetricRegistry metricRegistry,
            Optional<TrustContext> trustContext,
            Optional<ProxySelector> proxySelector,
            Collection<String> endpointUris,
            Class<T> type) {
        return proxyFactory.createProxyWithQuickFailoverForTesting(
                metricRegistry, trustContext, proxySelector, endpointUris, type);
    }
}
