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
import java.time.Duration;
import java.util.Collection;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;

import javax.net.ssl.SSLSocketFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.google.common.collect.ImmutableList;
import com.google.common.reflect.Reflection;
import com.palantir.atlasdb.config.ServerListConfig;
import com.palantir.common.remoting.ServiceNotAvailableException;
import com.palantir.conjure.java.api.config.service.ProxyConfiguration;
import com.palantir.conjure.java.api.config.service.ServiceConfiguration;
import com.palantir.conjure.java.api.config.service.UserAgent;
import com.palantir.conjure.java.api.config.ssl.SslConfiguration;
import com.palantir.conjure.java.client.config.ClientConfiguration;
import com.palantir.conjure.java.client.config.ClientConfigurations;
import com.palantir.conjure.java.client.config.NodeSelectionStrategy;
import com.palantir.conjure.java.client.jaxrs.JaxRsClient;
import com.palantir.conjure.java.config.ssl.SslSocketFactories;
import com.palantir.conjure.java.config.ssl.TrustContext;
import com.palantir.conjure.java.ext.refresh.Refreshable;
import com.palantir.conjure.java.ext.refresh.RefreshableProxyInvocationHandler;
import com.palantir.conjure.java.okhttp.HostMetricsRegistry;

import feign.Client;
import feign.Contract;
import feign.Feign;
import feign.Request;
import feign.Retryer;
import feign.codec.Decoder;
import feign.codec.Encoder;
import feign.codec.ErrorDecoder;
import feign.jackson.JacksonDecoder;
import feign.jackson.JacksonEncoder;
import feign.jaxrs.JAXRSContract;

public final class AtlasDbFeignTargetFactory {

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

    public static <T> T createProxyWithoutRetrying(
            Optional<TrustContext> trustContext,
            String uri,
            Class<T> type,
            String userAgent,
            boolean limitPayloadSize) {

        return createProxyWithFailover(trustContext, Optional.empty(), ImmutableList.of(uri), 10_000, 10_000, 10_000, type, userAgent, limitPayloadSize);

//        return Feign.builder()
//                .contract(contract)
//                .encoder(encoder)
//                .decoder(decoder)
//                .errorDecoder(errorDecoder)
//                .retryer(Retryer.NEVER_RETRY)
//                .client(createClient(trustContext, userAgent, limitPayloadSize))
//                .target(type, uri);
    }

    public static <T> T createProxy(
            Optional<TrustContext> trustContext,
            String uri,
            Class<T> type,
            String userAgent,
            boolean limitPayloadSize) {
        return createProxyNoFailover(trustContext, Optional.empty(), ImmutableList.of(uri), 10_000, 10_000, 10_000, type, userAgent, limitPayloadSize);

//        return Feign.builder()
//                .contract(contract)
//                .encoder(encoder)
//                .decoder(decoder)
//                .errorDecoder(errorDecoder)
//                .retryer(new InterruptHonoringRetryer())
//                .client(createClient(trustContext, userAgent, limitPayloadSize))
//                .target(type, uri);
    }

    public static <T> T createRsProxy(
            Optional<TrustContext> trustContext,
            String uri,
            Class<T> type,
            String userAgent) {
        return createProxyWithFailover(trustContext, Optional.empty(), ImmutableList.of(uri), 10_000, 10_000, 10_000, type, userAgent, false);
//        return Feign.builder()
//                .contract(contract)
//                .encoder(encoder)
//                .decoder(decoder)
//                .errorDecoder(new RsErrorDecoder())
//                .client(createClient(trustContext, userAgent, false))
//                .target(type, uri);
    }

    public static <T> T createProxyNoFailover(
            Optional<TrustContext> trustContext,
            Optional<ProxySelector> proxySelector,
            Collection<String> endpointUris,
            int feignConnectTimeout,
            int feignReadTimeout,
            long maxBackoffMillis,
            Class<T> type,
            String userAgent,
            boolean limitPayloadSize) {

        ClientConfiguration.Builder actual = ClientConfiguration.builder()
                .uris(endpointUris)
                .sslSocketFactory(trustContext.get().sslSocketFactory())
                .trustManager(trustContext.get().x509TrustManager())
                .connectTimeout(Duration.ofMillis(feignConnectTimeout))
                .readTimeout(Duration.ofMillis(feignReadTimeout))
                .writeTimeout(Duration.ofSeconds(10))
                .enableGcmCipherSuites(true)
                .fallbackToCommonNameVerification(true)
                .proxy(proxySelector.orElseGet(ProxySelector::getDefault))
                .maxNumRetries(1)
                .nodeSelectionStrategy(NodeSelectionStrategy.PIN_UNTIL_ERROR)
                .failedUrlCooldown(Duration.ofMillis(100))
                .backoffSlotSize(Duration.ofMillis(1));
        //        proxySelector.ifPresent(actual::proxy);

        return JaxRsClient.create(type, createAgent(userAgent), new HostMetricsRegistry(), actual.build());
    }

    public static <T> T createProxyWithFailover(
            Optional<TrustContext> trustContext,
            Optional<ProxySelector> proxySelector,
            Collection<String> endpointUris,
            int feignConnectTimeout,
            int feignReadTimeout,
            long maxBackoffMillis,
            Class<T> type,
            String userAgent,
            boolean limitPayloadSize) {

        ClientConfiguration.Builder actual = ClientConfiguration.builder()
                .uris(endpointUris)
                .sslSocketFactory(trustContext.get().sslSocketFactory())
                .trustManager(trustContext.get().x509TrustManager())
                .connectTimeout(Duration.ofMillis(feignConnectTimeout))
                .readTimeout(Duration.ofMillis(feignReadTimeout))
                .writeTimeout(Duration.ofSeconds(10))
                .enableGcmCipherSuites(true)
                .fallbackToCommonNameVerification(true)
                .proxy(proxySelector.orElseGet(ProxySelector::getDefault))
                .maxNumRetries(14)
                .nodeSelectionStrategy(NodeSelectionStrategy.PIN_UNTIL_ERROR)
                .failedUrlCooldown(Duration.ofMillis(100))
                .backoffSlotSize(Duration.ofMillis(1));
//        proxySelector.ifPresent(actual::proxy);

        return JaxRsClient.create(type, createAgent(userAgent), new HostMetricsRegistry(), actual.build());



//        JaxRsClient.create(type, UserAgent.of(UserAgent.Agent.of(userAgent, "test")), )
//
//            return ClientConfigurations.of(serviceConfiguration);
//        };
//
//        Refreshable<ClientConfiguration> refreshableConfig = PollingRefreshable
//                .createComposed(serverListConfigSupplier, Duration.ofSeconds(5L), transform)
//                .getRefreshable();
//
//
//        FailoverFeignTarget<T> failoverFeignTarget = new FailoverFeignTarget<>(endpointUris, maxBackoffMillis, type);
//        Client client = failoverFeignTarget.wrapClient(
//                FeignOkHttpClients.newRefreshingOkHttpClient(trustContext, proxySelector, userAgent, limitPayloadSize));
//
//
//
//        ClientConfiguration clientConfiguration = ClientConfigurations.of(
//                ImmutableList.copyOf(endpointUris),
//                SS)
//
//
//        return Feign.builder()
//                .contract(contract)
//                .encoder(encoder)
//                .decoder(decoder)
//                .errorDecoder(errorDecoder)
//                .client(client)
//                .retryer(failoverFeignTarget)
//                .options(new Request.Options(feignConnectTimeout, feignReadTimeout))
//                .target(failoverFeignTarget);
    }

    static <T> T createLiveReloadingProxyWithFailover(
            Supplier<ServerListConfig> serverListConfigSupplier,
            Function<SslConfiguration, TrustContext> trustContextCreator,
            Function<ProxyConfiguration, ProxySelector> proxySelectorCreator,
            int feignConnectTimeout,
            int feignReadTimeout,
            long maxBackoffMillis,
            Class<T> type,
            String userAgent,
            boolean limitPayload) {

        Function<ServerListConfig, ClientConfiguration> transform = serverConfig -> {
            ServiceConfiguration serviceConfiguration = ServiceConfiguration.builder()
                    .security(serverConfig.sslConfiguration().get())
                    .addAllUris(serverConfig.servers())
                    .proxy(serverConfig.proxyConfiguration())
                    .connectTimeout(Duration.ofMillis(feignConnectTimeout))
                    .readTimeout(Duration.ofMillis(feignReadTimeout))
                    .build();
            return ClientConfigurations.of(serviceConfiguration);
        };

        Refreshable<ClientConfiguration> refreshableConfig = PollingRefreshable
                .createComposed(serverListConfigSupplier, Duration.ofSeconds(5L), transform)
                .getRefreshable();

        return JaxRsClient.create(type, createAgent(userAgent), new HostMetricsRegistry(), refreshableConfig);



//        Supplier<ClientConfiguration> clientConfigurationSupplier
//
//        PollingRefreshable<ServerListConfig> configPollingRefreshable =
//                PollingRefreshable.create(serverListConfigSupplier);
//        Refreshable<ClientConfiguration> refreshableClientConfig
//
//
//        SSLSocketFactory sslSocketFactory = SslSocketFactories.createSslSocketFactory(se)
//        return Reflection.newProxy(
//                type,
//                RefreshableProxyInvocationHandler.create(
//                        configPollingRefreshable.getRefreshable(),
//                        serverListConfig -> {
//                            if (serverListConfig.hasAtLeastOneServer()) {
//                                return createProxyWithFailover(
//                                        serverListConfig.sslConfiguration().map(trustContextCreator),
//                                        serverListConfig.proxyConfiguration().map(proxySelectorCreator),
//                                        serverListConfig.servers(),
//                                        feignConnectTimeout,
//                                        feignReadTimeout,
//                                        maxBackoffMillis,
//                                        type,
//                                        userAgent,
//                                        limitPayload);
//                            }
//                            return createProxyForZeroNodes(type);
//                        }));
    }

    private static UserAgent createAgent(String userAgent) {
        return UserAgent.of(UserAgent.Agent.of(userAgent.replace('.', '-'), "0.0.0"));
    }

    private static Client createClient(
            Optional<TrustContext> trustContext,
            String userAgent,
            boolean limitPayload) {
        return FeignOkHttpClients.newRefreshingOkHttpClient(trustContext, Optional.empty(), userAgent, limitPayload);
    }

    private static <T> T createProxyForZeroNodes(Class<T> type) {
        return Reflection.newProxy(type, (unused1, unused2, unused3) -> {
            throw new ServiceNotAvailableException("The " + type.getSimpleName() + " is currently unavailable,"
                    + " because configuration contains zero servers.");
        });
    }
}
