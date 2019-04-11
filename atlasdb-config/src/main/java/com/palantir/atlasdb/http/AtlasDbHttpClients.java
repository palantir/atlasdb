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
import com.palantir.atlasdb.util.AtlasDbMetrics;
import com.palantir.conjure.java.api.config.service.ProxyConfiguration;
import com.palantir.conjure.java.api.config.ssl.SslConfiguration;
import com.palantir.conjure.java.config.ssl.TrustContext;

public final class AtlasDbHttpClients {
    // add some padding to the feign timeout, as in many cases lock requests default to a 60 second timeout,
    // and we don't want it to exactly align with the feign timeout
    private static final int DEFAULT_CONNECT_TIMEOUT_MILLIS = 10_000;
    private static final int DEFAULT_READ_TIMEOUT_MILLIS = 65_000;

    private static final int QUICK_FEIGN_TIMEOUT_MILLIS = 100;
    private static final int QUICK_MAX_BACKOFF_MILLIS = 100;

    private AtlasDbHttpClients() {
        // Utility class
    }

    /**
     * Constructs a dynamic proxy for the specified type. Uses a Feign HTTP client with no retrying.
     *
     * @param trustContext if present, the proxy will use the supplied SSL factory
     * @param limitPayloadSize if true, request payload size will be limited
     */
    public static <T> T createProxyWithoutRetrying(
            MetricRegistry metricRegistry,
            Optional<TrustContext> trustContext,
            String uri,
            Class<T> type,
            String userAgent,
            boolean limitPayloadSize) {
        return AtlasDbMetrics.instrument(
                metricRegistry,
                type,
                AtlasDbFeignTargetFactory
                        .createProxyWithoutRetrying(trustContext, uri, type, userAgent, limitPayloadSize),
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
            Optional<TrustContext> trustContext,
            Optional<ProxySelector> proxySelector,
            Collection<String> endpointUris,
            Class<T> type) {
        return AtlasDbMetrics.instrument(
                metricRegistry,
                type,
                AtlasDbFeignTargetFactory.createProxyWithFailover(
                        trustContext,
                        proxySelector,
                        endpointUris,
                        DEFAULT_CONNECT_TIMEOUT_MILLIS,
                        DEFAULT_READ_TIMEOUT_MILLIS,
                        FailoverFeignTarget.DEFAULT_MAX_BACKOFF_MILLIS,
                        type,
                        UserAgents.DEFAULT_USER_AGENT,
                        false),
                MetricRegistry.name(type));
    }

    public static <T> T createLiveReloadingProxyWithFailover(
            MetricRegistry metricRegistry,
            Supplier<ServerListConfig> serverListConfigSupplier,
            Function<SslConfiguration, TrustContext> trustContextCreator,
            Function<ProxyConfiguration, ProxySelector> proxySelectorCreator,
            Class<T> type,
            String userAgent,
            boolean limitPayload) {
        return AtlasDbMetrics.instrument(
                metricRegistry,
                type,
                AtlasDbFeignTargetFactory.createLiveReloadingProxyWithFailover(
                        serverListConfigSupplier,
                        trustContextCreator,
                        proxySelectorCreator,
                        DEFAULT_CONNECT_TIMEOUT_MILLIS,
                        DEFAULT_READ_TIMEOUT_MILLIS,
                        FailoverFeignTarget.DEFAULT_MAX_BACKOFF_MILLIS,
                        type,
                        userAgent,
                        limitPayload),
                MetricRegistry.name(type));
    }

    @VisibleForTesting
    static <T> T createLiveReloadingProxyWithQuickFailoverForTesting(
            MetricRegistry metricRegistry,
            Supplier<ServerListConfig> serverListConfigSupplier,
            Function<SslConfiguration, TrustContext> trustContextCreator,
            Function<ProxyConfiguration, ProxySelector> proxySelectorCreator,
            Class<T> type,
            String userAgent) {
        return AtlasDbMetrics.instrument(
                metricRegistry,
                type,
                AtlasDbFeignTargetFactory.createLiveReloadingProxyWithFailover(
                        serverListConfigSupplier,
                        trustContextCreator,
                        proxySelectorCreator,
                        QUICK_FEIGN_TIMEOUT_MILLIS,
                        QUICK_FEIGN_TIMEOUT_MILLIS,
                        QUICK_MAX_BACKOFF_MILLIS,
                        type,
                        userAgent,
                        false),
                MetricRegistry.name(type));
    }

    @VisibleForTesting
    static <T> T createProxyWithQuickFailoverForTesting(
            MetricRegistry metricRegistry,
            Optional<TrustContext> trustContext,
            Optional<ProxySelector> proxySelector,
            Collection<String> endpointUris,
            Class<T> type) {
        return AtlasDbMetrics.instrument(
                metricRegistry,
                type,
                AtlasDbFeignTargetFactory.createProxyWithFailover(
                        trustContext,
                        proxySelector,
                        endpointUris,
                        QUICK_FEIGN_TIMEOUT_MILLIS,
                        QUICK_FEIGN_TIMEOUT_MILLIS,
                        QUICK_MAX_BACKOFF_MILLIS,
                        type,
                        UserAgents.DEFAULT_USER_AGENT,
                        false),
                MetricRegistry.name(type, "atlasdb-testing"));
    }
}
