/*
 * (c) Copyright 2015 Palantir Technologies Inc. All rights reserved.
 *
 * Licensed under the BSD-3 License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://opensource.org/licenses/BSD-3-Clause
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
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;

import javax.net.ssl.SSLSocketFactory;

import com.codahale.metrics.MetricRegistry;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.palantir.atlasdb.config.ServerListConfig;
import com.palantir.atlasdb.util.AtlasDbMetrics;
import com.palantir.remoting.api.config.service.ProxyConfiguration;
import com.palantir.remoting.api.config.ssl.SslConfiguration;

public final class AtlasDbHttpClients {
    private static final int QUICK_FEIGN_TIMEOUT_MILLIS = 100;
    private static final int QUICK_MAX_BACKOFF_MILLIS = 100;

    private AtlasDbHttpClients() {
        // Utility class
    }

    /**
     * Constructs a dynamic proxy for the specified type, using the supplied SSL factory if is present, and the
     * default Feign HTTP client.
     */
    public static <T> T createProxy(Optional<SSLSocketFactory> sslSocketFactory, String uri, Class<T> type) {
        return createProxy(sslSocketFactory, uri, type, UserAgents.DEFAULT_USER_AGENT);
    }

    public static <T> T createProxy(
            Optional<SSLSocketFactory> sslSocketFactory,
            String uri,
            Class<T> type,
            String userAgent) {
        return AtlasDbMetrics.instrument(
                type,
                AtlasDbFeignTargetFactory.createProxy(sslSocketFactory, uri, type, userAgent),
                MetricRegistry.name(type));
    }

    public static <T> T createProxy(
            Optional<SSLSocketFactory> sslSocketFactory,
            String uri,
            boolean refreshingHttpClient,
            Class<T> type,
            String userAgent) {
        return AtlasDbMetrics.instrument(
                type,
                AtlasDbFeignTargetFactory.createProxy(sslSocketFactory, uri, refreshingHttpClient, type, userAgent),
                MetricRegistry.name(type));
    }

    /**
     * Constructs a list, corresponding to the iteration order of the supplied endpoints, of dynamic proxies for the
     * specified type, using the supplied SSL factory if it is present.
     */
    public static <T> List<T> createProxies(
            Optional<SSLSocketFactory> sslSocketFactory, Collection<String> endpointUris, Class<T> type) {
        return createProxies(sslSocketFactory, endpointUris, type, UserAgents.DEFAULT_USER_AGENT);
    }

    public static <T> List<T> createProxies(
            Optional<SSLSocketFactory> sslSocketFactory,
            Collection<String> endpointUris,
            Class<T> type,
            String userAgent) {
        List<T> ret = Lists.newArrayListWithCapacity(endpointUris.size());
        for (String uri : endpointUris) {
            ret.add(createProxy(sslSocketFactory, uri, type, userAgent));
        }
        return ret;
    }

    public static <T> List<T> createProxies(
            Optional<SSLSocketFactory> sslSocketFactory,
            Collection<String> endpointUris,
            boolean refreshingHttpClient,
            Class<T> type,
            String userAgent) {
        List<T> ret = Lists.newArrayListWithCapacity(endpointUris.size());
        for (String uri : endpointUris) {
            ret.add(createProxy(sslSocketFactory, uri, refreshingHttpClient, type, userAgent));
        }
        return ret;
    }

    /**
     * @deprecated please use {@link #createProxyWithFailover(Optional, Optional, Collection, Class)}, which requires
     * you to specify the ProxySelector parameter.
     */
    @Deprecated
    public static <T> T createProxyWithFailover(
            Optional<SSLSocketFactory> sslSocketFactory,
            Collection<String> endpointUris,
            Class<T> type) {
        return createProxyWithFailover(
                sslSocketFactory,
                Optional.empty(),
                endpointUris,
                type,
                UserAgents.DEFAULT_USER_AGENT);
    }

    /**
     * @deprecated please use {@link #createProxyWithFailover(Optional, Optional, Collection, Class, String)}, which
     * requires you to specify the ProxySelector parameter.
     */
    @Deprecated
    public static <T> T createProxyWithFailover(
            Optional<SSLSocketFactory> sslSocketFactory,
            Collection<String> endpointUris,
            Class<T> type,
            String userAgent) {
        return createProxyWithFailover(
                sslSocketFactory,
                Optional.empty(),
                endpointUris,
                type,
                userAgent);
    }

    /**
     * Constructs an HTTP-invoking dynamic proxy for the specified type that will cycle through the list of supplied
     * endpoints after encountering an exception or connection failure, using the supplied SSL factory if it is
     * present. Also use the supplied the proxy selector to set the proxy on the clients if present.
     * <p>
     * Failover will continue to cycle through the supplied endpoint list indefinitely.
     */
    public static <T> T createProxyWithFailover(
            Optional<SSLSocketFactory> sslSocketFactory,
            Optional<ProxySelector> proxySelector,
            Collection<String> endpointUris,
            Class<T> type) {
        return createProxyWithFailover(
                sslSocketFactory,
                proxySelector,
                endpointUris,
                type,
                UserAgents.DEFAULT_USER_AGENT);
    }

    public static <T> T createProxyWithFailover(
            Optional<SSLSocketFactory> sslSocketFactory,
            Optional<ProxySelector> proxySelector,
            Collection<String> endpointUris,
            Class<T> type,
            String userAgent) {
        return AtlasDbMetrics.instrument(
                type,
                AtlasDbFeignTargetFactory.createProxyWithFailover(
                        sslSocketFactory, proxySelector, endpointUris, type, userAgent),
                MetricRegistry.name(type));
    }

    public static <T> T createLiveReloadingProxyWithFailover(
            Supplier<ServerListConfig> serverListConfigSupplier,
            Function<SslConfiguration, SSLSocketFactory> sslSocketFactoryCreator,
            Function<ProxyConfiguration, ProxySelector> proxySelectorCreator,
            Class<T> type,
            String userAgent) {
        return AtlasDbMetrics.instrument(
                type,
                AtlasDbFeignTargetFactory.createLiveReloadingProxyWithFailover(
                        serverListConfigSupplier, sslSocketFactoryCreator, proxySelectorCreator, type, userAgent),
                MetricRegistry.name(type));
    }

    @VisibleForTesting
    static <T> T createLiveReloadingProxyWithQuickFailoverForTesting(
            Supplier<ServerListConfig> serverListConfigSupplier,
            Function<SslConfiguration, SSLSocketFactory> sslSocketFactoryCreator,
            Function<ProxyConfiguration, ProxySelector> proxySelectorCreator,
            Class<T> type,
            String userAgent) {
        return AtlasDbMetrics.instrument(
                type,
                AtlasDbFeignTargetFactory.createLiveReloadingProxyWithFailover(
                        serverListConfigSupplier,
                        sslSocketFactoryCreator,
                        proxySelectorCreator,
                        QUICK_FEIGN_TIMEOUT_MILLIS,
                        QUICK_FEIGN_TIMEOUT_MILLIS,
                        QUICK_MAX_BACKOFF_MILLIS,
                        type,
                        userAgent),
                MetricRegistry.name(type));
    }

    @VisibleForTesting
    static <T> T createProxyWithQuickFailoverForTesting(
            Optional<SSLSocketFactory> sslSocketFactory,
            Optional<ProxySelector> proxySelector, Collection<String> endpointUris, Class<T> type) {
        return AtlasDbMetrics.instrument(
                type,
                AtlasDbFeignTargetFactory.createProxyWithFailover(
                        sslSocketFactory,
                        proxySelector,
                        endpointUris,
                        QUICK_FEIGN_TIMEOUT_MILLIS,
                        QUICK_FEIGN_TIMEOUT_MILLIS,
                        QUICK_MAX_BACKOFF_MILLIS,
                        type,
                        UserAgents.DEFAULT_USER_AGENT),
                MetricRegistry.name(type, "atlasdb-testing"));
    }
}
