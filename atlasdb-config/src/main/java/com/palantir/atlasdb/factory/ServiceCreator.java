/*
 * Copyright 2017 Palantir Technologies, Inc. All rights reserved.
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
package com.palantir.atlasdb.factory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.net.ProxySelector;
import java.net.SocketAddress;
import java.net.URI;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

import javax.net.ssl.SSLSocketFactory;

import com.codahale.metrics.MetricRegistry;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.net.HostAndPort;
import com.palantir.atlasdb.config.ServerListConfig;
import com.palantir.atlasdb.http.AtlasDbHttpClients;
import com.palantir.atlasdb.util.AtlasDbMetrics;
import com.palantir.remoting.api.config.service.ProxyConfiguration;
import com.palantir.remoting.api.config.ssl.SslConfiguration;
import com.palantir.remoting3.config.ssl.SslSocketFactories;

public class ServiceCreator<T> implements Function<ServerListConfig, T> {
    private final Class<T> serviceClass;
    private final String userAgent;

    public ServiceCreator(Class<T> serviceClass, String userAgent) {
        this.serviceClass = serviceClass;
        this.userAgent = userAgent;
    }

    @Override
    public T apply(ServerListConfig input) {
        return applyDynamic(() -> input);
    }

    // Semi-horrible, but given that we create ServiceCreators explicitly and I'd rather not API break our
    // implementation of Function, leaving this here for now.
    public T applyDynamic(Supplier<ServerListConfig> input) {
        return createService(
                input,
                SslSocketFactories::createSslSocketFactory,
                ServiceCreator::createProxySelector,
                serviceClass,
                userAgent);
    }

    /**
     * Utility method for transforming an optional {@link SslConfiguration} into an optional {@link SSLSocketFactory}.
     */
    public static Optional<SSLSocketFactory> createSslSocketFactory(Optional<SslConfiguration> sslConfiguration) {
        return sslConfiguration.map(config -> SslSocketFactories.createSslSocketFactory(config));
    }

    private static <T> T createService(
            Supplier<ServerListConfig> serverListConfigSupplier,
            java.util.function.Function<SslConfiguration, SSLSocketFactory> sslSocketFactoryCreator,
            java.util.function.Function<ProxyConfiguration, ProxySelector> proxySelectorCreator,
            Class<T> type,
            String userAgent) {
        return AtlasDbHttpClients.createLiveReloadingProxyWithFailover(
                serverListConfigSupplier, sslSocketFactoryCreator, proxySelectorCreator, type, userAgent);
    }

    public static <T> T createInstrumentedService(T service, Class<T> serviceClass) {
        return AtlasDbMetrics.instrument(
                serviceClass,
                service,
                MetricRegistry.name(serviceClass));
    }

    /**
     * The code below is copied from http-remoting and should be removed when we switch the clients to use remoting.
     */
    public static ProxySelector createProxySelector(ProxyConfiguration proxyConfig) {
        switch (proxyConfig.type()) {
            case DIRECT:
                return fixedProxySelectorFor(Proxy.NO_PROXY);
            case HTTP:
                HostAndPort hostAndPort = HostAndPort.fromString(proxyConfig.hostAndPort()
                        .orElseThrow(() -> new IllegalArgumentException(
                                "Expected to find proxy hostAndPort configuration for HTTP proxy")));
                InetSocketAddress addr = new InetSocketAddress(hostAndPort.getHost(), hostAndPort.getPort());
                return fixedProxySelectorFor(new Proxy(Proxy.Type.HTTP, addr));
            default:
                // fall through
        }

        throw new IllegalStateException("Failed to create ProxySelector for proxy configuration: " + proxyConfig);
    }

    private static ProxySelector fixedProxySelectorFor(Proxy proxy) {
        return new ProxySelector() {
            @Override
            public List<Proxy> select(URI uri) {
                return ImmutableList.of(proxy);
            }

            @Override
            public void connectFailed(URI uri, SocketAddress sa, IOException ioe) {}
        };

    }
}
