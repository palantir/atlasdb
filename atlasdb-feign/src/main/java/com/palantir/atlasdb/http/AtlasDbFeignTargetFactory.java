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

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.net.ProxySelector;
import java.net.SocketAddress;
import java.net.URI;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.google.common.collect.ImmutableList;
import com.google.common.net.HostAndPort;
import com.google.common.reflect.Reflection;
import com.palantir.atlasdb.config.AuxiliaryRemotingParameters;
import com.palantir.atlasdb.config.ServerListConfig;
import com.palantir.common.remoting.ServiceNotAvailableException;
import com.palantir.conjure.java.api.config.service.ProxyConfiguration;
import com.palantir.conjure.java.api.config.service.UserAgent;
import com.palantir.conjure.java.config.ssl.TrustContext;
import com.palantir.conjure.java.ext.refresh.RefreshableProxyInvocationHandler;
import com.palantir.logsafe.exceptions.SafeIllegalArgumentException;

import feign.Client;
import feign.Contract;
import feign.Feign;
import feign.Request;
import feign.Retryer;
import feign.codec.Decoder;
import feign.codec.Encoder;
import feign.codec.ErrorDecoder;
import feign.jaxrs.JAXRSContract;

public final class AtlasDbFeignTargetFactory implements TargetFactory {
    // add some padding to the feign timeout, as in many cases lock requests default to a 60 second timeout,
    // and we don't want it to exactly align with the feign timeout
    private static final int DEFAULT_CONNECT_TIMEOUT_MILLIS = 10_000;
    private static final int DEFAULT_READ_TIMEOUT_MILLIS = 65_000;

    private static final int QUICK_FEIGN_TIMEOUT_MILLIS = 100;
    private static final int QUICK_MAX_BACKOFF_MILLIS = 100;

    // TODO (jkong): Decide what to do with usage in benchmarking infrastructure
    public static final TargetFactory DEFAULT = new AtlasDbFeignTargetFactory(
            DEFAULT_CONNECT_TIMEOUT_MILLIS,
            DEFAULT_READ_TIMEOUT_MILLIS,
            FailoverFeignTarget.DEFAULT_MAX_BACKOFF.toMillis());
    static final TargetFactory FAST_FAIL_FOR_TESTING = new AtlasDbFeignTargetFactory(
            QUICK_FEIGN_TIMEOUT_MILLIS,
            QUICK_FEIGN_TIMEOUT_MILLIS,
            QUICK_MAX_BACKOFF_MILLIS);

    private static final ObjectMapper mapper = new ObjectMapper()
            .registerModule(new Jdk8Module())
            .registerModule(new JavaTimeModule())
            .registerModule(new GuavaModule());
    private static final Contract contract = new JAXRSContract();
    private static final Encoder encoder = new AtlasDbJacksonEncoder(mapper);
    private static final Decoder decoder = new TextDelegateDecoder(
            new OptionalAwareDecoder(new AtlasDbJacksonDecoder(mapper)));
    private static final ErrorDecoder errorDecoder = new AtlasDbErrorDecoder();
    public static final String CLIENT_VERSION_STRING = "AtlasDB-Feign";

    private final int connectTimeout;
    private final int readTimeout;
    private final long maxBackoffMillis;

    private AtlasDbFeignTargetFactory(int connectTimeout, int readTimeout, long maxBackoffMillis) {
        this.connectTimeout = connectTimeout;
        this.readTimeout = readTimeout;
        this.maxBackoffMillis = maxBackoffMillis;
    }

    @Override
    public <T> InstanceAndVersion<T> createProxy(
            Optional<TrustContext> trustContext,
            String uri,
            Class<T> type,
            AuxiliaryRemotingParameters parameters) {
        return wrapWithVersion(Feign.builder()
                .contract(contract)
                .encoder(encoder)
                .decoder(decoder)
                .errorDecoder(errorDecoder)
                .retryer(parameters.shouldRetry() ? new InterruptHonoringRetryer() : Retryer.NEVER_RETRY)
                .client(createClient(trustContext, parameters))
                .target(type, uri));
    }

    @Override
    public <T> InstanceAndVersion<T> createProxyWithFailover(
            ServerListConfig serverListConfig,
            Class<T> type,
            AuxiliaryRemotingParameters parameters) {
        FailoverFeignTarget<T> failoverFeignTarget = new FailoverFeignTarget<>(
                serverListConfig.servers(), maxBackoffMillis, type);
        Client client = failoverFeignTarget.wrapClient(
                FeignOkHttpClients.newRefreshingOkHttpClient(
                        serverListConfig.trustContext(),
                        serverListConfig.proxyConfiguration().map(AtlasDbFeignTargetFactory::createProxySelector),
                        parameters));

        return wrapWithVersion(Feign.builder()
                .contract(contract)
                .encoder(encoder)
                .decoder(decoder)
                .errorDecoder(errorDecoder)
                .client(client)
                .retryer(failoverFeignTarget)
                .options(new Request.Options(connectTimeout, readTimeout))
                .target(failoverFeignTarget));
    }

    @Override
    public <T> InstanceAndVersion<T> createLiveReloadingProxyWithFailover(
            Supplier<ServerListConfig> serverListConfigSupplier,
            Class<T> type,
            AuxiliaryRemotingParameters parameters) {
        PollingRefreshable<ServerListConfig> configPollingRefreshable =
                PollingRefreshable.create(serverListConfigSupplier);
        return wrapWithVersion(Reflection.newProxy(
                type,
                RefreshableProxyInvocationHandler.create(
                        configPollingRefreshable.getRefreshable(),
                        serverListConfig -> {
                            if (serverListConfig.hasAtLeastOneServer()) {
                                return createProxyWithFailover(serverListConfig, type, parameters).instance();
                            }
                            return createProxyForZeroNodes(type);
                        })));
    }

    public static <T> T createRsProxy(
            Optional<TrustContext> trustContext,
            String uri,
            Class<T> type,
            UserAgent userAgent) {
        AuxiliaryRemotingParameters remotingParameters = AuxiliaryRemotingParameters.builder()
                .userAgent(userAgent)
                .shouldLimitPayload(false) // Only used for leader blocks
                .shouldRetry(true)
                .build();
        return Feign.builder()
                .contract(contract)
                .encoder(encoder)
                .decoder(decoder)
                .errorDecoder(new RsErrorDecoder())
                .client(createClient(trustContext, remotingParameters))
                .target(type, uri);
    }

    private static Client createClient(
            Optional<TrustContext> trustContext,
            AuxiliaryRemotingParameters parameters) {
        return FeignOkHttpClients.newRefreshingOkHttpClient(trustContext, Optional.empty(), parameters);
    }

    private static <T> T createProxyForZeroNodes(Class<T> type) {
        return Reflection.newProxy(type, (unused1, unused2, unused3) -> {
            throw new ServiceNotAvailableException("The " + type.getSimpleName() + " is currently unavailable,"
                    + " because configuration contains zero servers.");
        });
    }

    /**
     * The code below is copied from http-remoting and should be removed when we switch the clients to use remoting.
     */
    private static ProxySelector createProxySelector(ProxyConfiguration proxyConfig) {
        switch (proxyConfig.type()) {
            case DIRECT:
                return fixedProxySelectorFor(Proxy.NO_PROXY);
            case HTTP:
                HostAndPort hostAndPort = HostAndPort.fromString(proxyConfig.hostAndPort()
                        .orElseThrow(() -> new SafeIllegalArgumentException(
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

    private static <T> InstanceAndVersion<T> wrapWithVersion(T instance) {
        return ImmutableInstanceAndVersion.of(instance, CLIENT_VERSION_STRING);
    }
}
