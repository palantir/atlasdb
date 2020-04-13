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

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

import org.immutables.value.Value;

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
import com.palantir.tritium.Tritium;
import com.palantir.tritium.metrics.registry.TaggedMetricRegistry;

final class DialogueClientFactory {
    private static final DefaultConjureRuntime DIALOGUE_RUNTIME =
            DefaultConjureRuntime.builder().build();
    private static final String SERVICE_NAME = "lock";
    private final TaggedMetricRegistry taggedMetricRegistry;
    private final Supplier<Optional<ServerListConfig>> config;
    private final UserAgent userAgent;
    private volatile ConcurrentHashMap<CacheKey, DialogueChannel> cachedChannels = new ConcurrentHashMap<>();

    DialogueClientFactory(
            Supplier<Optional<ServerListConfig>> config,
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
        Supplier<Channel> channelSupplier = new CachedComposingSupplier<>(
                config, maybeConfig -> {
            if (!maybeConfig.isPresent()) {
                return alwaysThrowingChannel(() -> new SafeIllegalStateException("Service not configured"));
            }

            // TODO(forozco): support cases when ssl config changes
            ClientConfiguration clientConfig = getClientConf(staticClientConfig, maybeConfig.get());
            DialogueChannel channel = cachedChannels.computeIfAbsent(CacheKey.of(SERVICE_NAME, staticClientConfig),
                    cacheKey -> {
                        String channelName = toChannelName(cacheKey);
                        ApacheHttpClientChannels.CloseableClient client = ApacheHttpClientChannels
                                .createCloseableHttpClient(clientConfig, channelName);
                        return DialogueChannel.builder()
                                .channelName(channelName)
                                .clientConfiguration(clientConfig)
                                .channelFactory(uri -> ApacheHttpClientChannels.createSingleUri(uri, client))
                                .build();
                    });
            channel.updateUris(maybeConfig.get().servers());
            return channel;
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
}
