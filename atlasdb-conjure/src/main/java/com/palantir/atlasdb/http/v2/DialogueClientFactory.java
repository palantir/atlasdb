/*
 * (c) Copyright 2020 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.http.v2;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

import org.immutables.value.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.Futures;
import com.palantir.atlasdb.config.ServerListConfig;
import com.palantir.conjure.java.api.config.service.UserAgent;
import com.palantir.conjure.java.client.config.ClientConfiguration;
import com.palantir.conjure.java.client.jaxrs.JaxRsClient;
import com.palantir.conjure.java.dialogue.serde.DefaultConjureRuntime;
import com.palantir.dialogue.Channel;
import com.palantir.dialogue.ConjureRuntime;
import com.palantir.dialogue.core.DialogueChannel;
import com.palantir.dialogue.hc4.ApacheHttpClientChannels;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.exceptions.SafeIllegalArgumentException;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;
import com.palantir.refreshable.Refreshable;
import com.palantir.tritium.Tritium;
import com.palantir.tritium.metrics.registry.TaggedMetricRegistry;

final class DialogueClientFactory implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(DialogueClientFactory.class);
    private static final DefaultConjureRuntime DIALOGUE_RUNTIME =
            DefaultConjureRuntime.builder().build();
    private static final String SERVICE_NAME = "lock";
    private final TaggedMetricRegistry taggedMetricRegistry;
    private final Refreshable<Optional<ServerListConfig>> config;
    private final UserAgent userAgent;
    private final ConcurrentHashMap<CacheKey, CachedClient> cachedChannels = new ConcurrentHashMap<>();

    DialogueClientFactory(
            Refreshable<Optional<ServerListConfig>> config,
            TaggedMetricRegistry taggedMetricRegistry,
            UserAgent userAgent) {
        this.taggedMetricRegistry = taggedMetricRegistry;
        this.config = config;
        this.userAgent = userAgent;
    }

    <T> T dialogueClient(
            Class<T> dialogueInterface, StaticClientConfiguration staticConfig) {
        Channel liveReloadingChannel = liveReloadableChannel(staticConfig);

        T dialogueClient = callStaticFactoryMethod(dialogueInterface, liveReloadingChannel);

        return dialogueClient;
    }

    <T> T jaxrsClient(Class<T> jaxrsInterface, StaticClientConfiguration staticConfig) {
        Channel liveReloadingChannel = liveReloadableChannel(staticConfig);

        T jaxrsClient = JaxRsClient.create(jaxrsInterface, liveReloadingChannel, DIALOGUE_RUNTIME);
        // Instrument with Tritium to match CJR jaxrs client creation
        T instrumentedJaxrsClient = Tritium.instrument(jaxrsInterface, jaxrsClient, taggedMetricRegistry);

        return instrumentedJaxrsClient;
    }

    private Channel liveReloadableChannel(StaticClientConfiguration staticClientConfig) {
        Supplier<Channel> channelSupplier = config.map(maybeConfig -> {
            if (!maybeConfig.isPresent()) {
                return alwaysThrowingChannel(() -> new SafeIllegalStateException("Service not configured"));
            }

            // TODO(forozco): support cases when ssl config changes
            ClientConfiguration clientConfig = getClientConf(staticClientConfig, maybeConfig.get());
            CachedClient cachedClient = cachedChannels.computeIfAbsent(
                    CacheKey.of(SERVICE_NAME, staticClientConfig), cacheKey -> CachedClient.of(cacheKey, clientConfig));
            return cachedClient.channel()
                    .<Channel>map(channel -> {
                        channel.channel().updateUris(maybeConfig.get().servers());
                        return channel.channel();
                    }).orElseGet(() -> alwaysThrowingChannel(cachedClient.throwable()::get));
        });

        return new SupplierChannel(channelSupplier);
    }

    private ClientConfiguration getClientConf(
            StaticClientConfiguration staticClientConfig, ServerListConfig serverListConfig) {
        return ClientConfiguration.builder()
                .from(StaticClientConfigurations.apply(staticClientConfig, serverListConfig))
                .taggedMetricRegistry(taggedMetricRegistry)
                .userAgent(userAgent)
                .build();
    }

    @Override
    public void close() {
        for (Map.Entry<CacheKey, CachedClient> entry : cachedChannels.entrySet()) {
            CacheKey cacheKey = entry.getKey();
            CachedClient value = entry.getValue();
            cachedChannels.remove(cacheKey);
            closeCachedClient(value, cacheKey.serviceName());
        }
    }

    private void closeCachedClient(CachedClient cachedClient, String serviceName) {
        try {
            if (cachedClient.channel().isPresent()) {
                ApacheHttpClientChannels.CloseableClient toClose =
                        cachedClient.channel().get().closeableClient();
                toClose.close();
            }
        } catch (RuntimeException | IOException | Error e) {
            log.error("Failed to close client", SafeArg.of("serviceName", serviceName), e);
        }
    }

    static boolean isDialogue(Class<?> serviceInterface) {
        return getStaticOfMethod(serviceInterface).isPresent();
    }

    private static String toChannelName(CacheKey cacheKey) {
        return "atlasdb-dialogue-channel-" + SERVICE_NAME + staticConfigToString(cacheKey.staticConfig());
    }

    private static String staticConfigToString(StaticClientConfiguration config) {
        StringBuilder builder = new StringBuilder();
        config.clientQoS().ifPresent(value -> builder.append("-").append(value));
        config.maxNumRetries().ifPresent(value -> builder.append("-").append(value));
        builder.append("-").append(config.fastReadTimeOut());
        return builder.toString();
    }

    private static Channel alwaysThrowingChannel(Supplier<? extends Throwable> exceptionSupplier) {
        return (endpoint, request) -> Futures.immediateFailedFuture(exceptionSupplier.get());
    }

    private static Optional<Method> getStaticOfMethod(Class<?> dialogueInterface) {
        try {
            return Optional.ofNullable(dialogueInterface.getMethod("of", Channel.class, ConjureRuntime.class));
        } catch (NoSuchMethodException e) {
            return Optional.empty();
        }
    }

    private static <T> T callStaticFactoryMethod(Class<T> dialogueInterface, Channel channel) {
        try {
            Method method = getStaticOfMethod(dialogueInterface).orElseThrow(() -> new SafeIllegalStateException(
                    "A static of(Channel, ConjureRuntime) method on Dialogue interface is required",
                    SafeArg.of("dialogueInterface", dialogueInterface)));

            return dialogueInterface.cast(method.invoke(null, channel, DIALOGUE_RUNTIME));

        } catch (IllegalAccessException | InvocationTargetException e) {
            throw new SafeIllegalArgumentException(
                    "Failed to reflectively construct dialogue client. Please check the "
                            + "dialogue interface class has a static of(Channel, ConjureRuntime) method",
                    e,
                    SafeArg.of("dialogueInterface", dialogueInterface));
        }
    }

    @Value.Immutable
    interface CacheKey {
        String serviceName();

        StaticClientConfiguration staticConfig();

        static CacheKey of(String serviceName, StaticClientConfiguration staticConfig) {
            return ImmutableCacheKey.builder()
                    .serviceName(serviceName)
                    .staticConfig(staticConfig)
                    .build();
        }
    }


    @Value.Immutable
    interface CachedClient {
        StaticClientConfiguration staticConfig();

        @Value.Auxiliary
        Optional<CloseableChannel> channel();

        @Value.Auxiliary
        Optional<Throwable> throwable();

        static CachedClient of(CacheKey cacheKey, ClientConfiguration clientConfig) {
            ImmutableCachedClient.Builder builder = ImmutableCachedClient.builder()
                    .staticConfig(cacheKey.staticConfig());
            try {
                String channelName = toChannelName(cacheKey);
                ApacheHttpClientChannels.ClientBuilder clientBuilder = ApacheHttpClientChannels.clientBuilder()
                        .clientConfiguration(clientConfig)
                        .clientName(channelName);
                ApacheHttpClientChannels.CloseableClient client = clientBuilder.build();
                return builder.channel(CloseableChannel.of(
                        DialogueChannel.builder()
                                .channelName(channelName)
                                .clientConfiguration(clientConfig)
                                .channelFactory(uri -> ApacheHttpClientChannels.createSingleUri(uri, client))
                                .build(),
                        client))
                        .build();
            } catch (Exception | Error e) {
                return builder.throwable(e).build();
            }
        }
    }


    @Value.Immutable
    interface CloseableChannel {
        DialogueChannel channel();

        ApacheHttpClientChannels.CloseableClient closeableClient();

        static CloseableChannel of(DialogueChannel channel, ApacheHttpClientChannels.CloseableClient closeableClient) {
            return ImmutableCloseableChannel.builder()
                    .channel(channel)
                    .closeableClient(closeableClient)
                    .build();
        }
    }
}
