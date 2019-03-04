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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.google.common.reflect.Reflection;
import com.palantir.atlasdb.config.ServerListConfig;
import com.palantir.common.remoting.ServiceNotAvailableException;
import com.palantir.conjure.java.api.config.service.ProxyConfiguration;
import com.palantir.conjure.java.api.config.ssl.SslConfiguration;
import com.palantir.conjure.java.config.ssl.TrustContext;
import com.palantir.conjure.java.ext.refresh.RefreshableProxyInvocationHandler;

import feign.Client;
import feign.Contract;
import feign.Feign;
import feign.Request;
import feign.codec.Decoder;
import feign.codec.Encoder;
import feign.codec.ErrorDecoder;
import feign.jackson.JacksonDecoder;
import feign.jackson.JacksonEncoder;
import feign.jaxrs.JAXRSContract;

public final class AtlasDbFeignTargetFactory {
    // add some padding to the feign timeout, as in many cases lock requests default to a 60 second timeout,
    // and we don't want it to exactly align with the feign timeout
    private static final Request.Options DEFAULT_FEIGN_OPTIONS = new Request.Options(
            10_000, 65_000);

    private static final ObjectMapper mapper = new ObjectMapper()
            .registerModule(new Jdk8Module())
            .registerModule(new JavaTimeModule())
            .registerModule(new GuavaModule());
    private static final Contract contract = new JAXRSContract();
    private static final Encoder encoder = new JacksonEncoder(mapper);
    private static final Decoder decoder = new TextDelegateDecoder(
            new OptionalAwareDecoder(new JacksonDecoder(mapper)));
    private static final ErrorDecoder errorDecoder = new AtlasDbErrorDecoder();

    private AtlasDbFeignTargetFactory() {
        // factory
    }

    public static <T> T createProxy(
            Optional<TrustContext> trustContext,
            String uri,
            boolean refreshingHttpClient,
            Class<T> type,
            String userAgent,
            boolean limitPayloadSize) {
        return Feign.builder()
                .contract(contract)
                .encoder(encoder)
                .decoder(decoder)
                .errorDecoder(errorDecoder)
                .retryer(new InterruptHonoringRetryer())
                .client(createClient(trustContext, refreshingHttpClient, userAgent, limitPayloadSize))
                .target(type, uri);
    }

    private static Client createClient(
            Optional<TrustContext> trustContext,
            boolean refreshingHttpClient,
            String userAgent,
            boolean limitPayload) {
        if (!refreshingHttpClient) {
            return FeignOkHttpClients.newOkHttpClient(trustContext, Optional.empty(), userAgent, limitPayload);
        }
        return FeignOkHttpClients.newRefreshingOkHttpClient(trustContext, Optional.empty(), userAgent, limitPayload);
    }

    public static <T> T createRsProxy(
            Optional<TrustContext> trustContext,
            String uri,
            Class<T> type,
            String userAgent) {
        return Feign.builder()
                .contract(contract)
                .encoder(encoder)
                .decoder(decoder)
                .errorDecoder(new RsErrorDecoder())
                .client(FeignOkHttpClients.newOkHttpClient(trustContext, Optional.empty(), userAgent, false))
                .target(type, uri);
    }

    public static <T> T createProxyWithFailover(
            Optional<TrustContext> trustContext,
            Optional<ProxySelector> proxySelector,
            Collection<String> endpointUris,
            Class<T> type,
            String userAgent) {
        return createProxyWithFailover(
                trustContext,
                proxySelector,
                endpointUris,
                DEFAULT_FEIGN_OPTIONS,
                FailoverFeignTarget.DEFAULT_MAX_BACKOFF_MILLIS,
                type,
                userAgent,
                false);
    }

    public static <T> T createProxyWithFailover(
            Optional<TrustContext> trustContext,
            Optional<ProxySelector> proxySelector,
            Collection<String> endpointUris,
            int feignConnectTimeout,
            int feignReadTimeout,
            int maxBackoffMillis,
            Class<T> type,
            String userAgent,
            boolean limitPayloadSize) {
        return createProxyWithFailover(
                trustContext,
                proxySelector,
                endpointUris,
                new Request.Options(feignConnectTimeout, feignReadTimeout),
                maxBackoffMillis,
                type,
                userAgent,
                limitPayloadSize);
    }

    private static <T> T createProxyWithFailover(
            Optional<TrustContext> trustContext,
            Optional<ProxySelector> proxySelector,
            Collection<String> endpointUris,
            Request.Options feignOptions,
            int maxBackoffMillis,
            Class<T> type,
            String userAgent,
            boolean limitPayloadSize) {
        FailoverFeignTarget<T> failoverFeignTarget = new FailoverFeignTarget<>(endpointUris, maxBackoffMillis, type);
        Client client = failoverFeignTarget.wrapClient(
                FeignOkHttpClients.newRefreshingOkHttpClient(trustContext, proxySelector, userAgent, limitPayloadSize));
        return Feign.builder()
                .contract(contract)
                .encoder(encoder)
                .decoder(decoder)
                .errorDecoder(errorDecoder)
                .client(client)
                .retryer(failoverFeignTarget)
                .options(feignOptions)
                .target(failoverFeignTarget);
    }

    public static <T> T createLiveReloadingProxyWithFailover(
            Supplier<ServerListConfig> serverListConfigSupplier,
            Function<SslConfiguration, TrustContext> trustContextCreator,
            Function<ProxyConfiguration, ProxySelector> proxySelectorCreator,
            Class<T> type,
            String userAgent,
            boolean limitPayload) {
        return createLiveReloadingProxyWithFailover(
                serverListConfigSupplier,
                trustContextCreator,
                proxySelectorCreator,
                DEFAULT_FEIGN_OPTIONS.connectTimeoutMillis(),
                DEFAULT_FEIGN_OPTIONS.readTimeoutMillis(),
                FailoverFeignTarget.DEFAULT_MAX_BACKOFF_MILLIS,
                type,
                userAgent,
                limitPayload);
    }

    public static <T> T createLiveReloadingProxyWithFailover(
            Supplier<ServerListConfig> serverListConfigSupplier,
            Function<SslConfiguration, TrustContext> trustContextCreator,
            Function<ProxyConfiguration, ProxySelector> proxySelectorCreator,
            int feignConnectTimeout,
            int feignReadTimeout,
            int maxBackoffMillis,
            Class<T> type,
            String userAgent,
            boolean limitPayload) {
        PollingRefreshable<ServerListConfig> configPollingRefreshable =
                PollingRefreshable.create(serverListConfigSupplier);
        return Reflection.newProxy(
                type,
                RefreshableProxyInvocationHandler.create(
                        configPollingRefreshable.getRefreshable(),
                        serverListConfig -> {
                            if (serverListConfig.hasAtLeastOneServer()) {
                                return createProxyWithFailover(
                                        serverListConfig.sslConfiguration().map(trustContextCreator),
                                        serverListConfig.proxyConfiguration().map(proxySelectorCreator),
                                        serverListConfig.servers(),
                                        feignConnectTimeout,
                                        feignReadTimeout,
                                        maxBackoffMillis,
                                        type,
                                        userAgent,
                                        limitPayload);
                            }
                            return createProxyForZeroNodes(type);
                        }));
    }

    private static <T> T createProxyForZeroNodes(Class<T> type) {
        return Reflection.newProxy(type, (unused1, unused2, unused3) -> {
            throw new ServiceNotAvailableException("The " + type.getSimpleName() + " is currently unavailable,"
                    + " because configuration contains zero servers.");
        });
    }
}
