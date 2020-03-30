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
import java.util.function.Supplier;

import com.google.common.collect.ImmutableList;
import com.palantir.atlasdb.config.AuxiliaryRemotingParameters;
import com.palantir.atlasdb.config.ImmutableAuxiliaryRemotingParameters;
import com.palantir.atlasdb.config.ServerListConfig;
import com.palantir.atlasdb.http.TargetFactory;
import com.palantir.conjure.java.client.config.ClientConfiguration;
import com.palantir.conjure.java.client.jaxrs.JaxRsClient;
import com.palantir.conjure.java.config.ssl.TrustContext;
import com.palantir.conjure.java.dialogue.serde.DefaultConjureRuntime;
import com.palantir.conjure.java.okhttp.HostMetricsRegistry;
import com.palantir.dialogue.Channel;
import com.palantir.dialogue.ConjureRuntime;
import com.palantir.dialogue.hc4.ApacheHttpClientChannels;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.exceptions.SafeIllegalArgumentException;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;
import com.palantir.util.CachedTransformingSupplier;

public enum DefaultTargetFactory implements TargetFactory {
    INSTANCE;

    private static final HostMetricsRegistry HOST_METRICS_REGISTRY = new HostMetricsRegistry();
    private static final DefaultConjureRuntime DIALOGUE_RUNTIME =
            DefaultConjureRuntime.builder().build();

    @Override
    public <T> T createProxy(Optional<TrustContext> trustContext, String uri, Class<T> type,
            AuxiliaryRemotingParameters parameters) {
        ClientOptions relevantOptions = ClientOptions.fromRemotingParameters(parameters);
        ClientConfiguration clientConfiguration = relevantOptions.create(
                ImmutableList.of(uri),
                Optional.empty(),
                trustContext.orElseThrow(() -> new IllegalStateException("CJR requires a trust context")));
        return client(type, clientConfiguration);
    }

    @Override
    public <T> T createProxyWithFailover(ServerListConfig serverListConfig, Class<T> type,
            AuxiliaryRemotingParameters parameters) {
        // It doesn't make sense to create a proxy with the capacity to failover that doesn't retry.
        ClientOptions clientOptions = getClientOptionsForFailoverProxy(parameters);
        return client(type, clientOptions.serverListToClient(serverListConfig));
    }

    @Override
    public <T> T createLiveReloadingProxyWithFailover(Supplier<ServerListConfig> serverListConfigSupplier,
            Class<T> type, AuxiliaryRemotingParameters parameters) {
        ClientOptions options = getClientOptionsForFailoverProxy(parameters);
        Supplier<T> clientSupplier = new CachedTransformingSupplier<>(
                serverListConfigSupplier,
                serverListConfig -> client(type, options.serverListToClient(serverListConfig)));
        return FastFailoverProxy.newProxyInstance(type, clientSupplier);
    }

    public <T> T createProxyWithQuickFailoverForTesting(
            ServerListConfig serverListConfig,
            Class<T> type,
            AuxiliaryRemotingParameters parameters) {
        return client(type, ClientOptions.FAST_RETRYING_FOR_TEST.serverListToClient(serverListConfig));
    }

    private static ClientOptions getClientOptionsForFailoverProxy(AuxiliaryRemotingParameters parameters) {
        return ClientOptions.fromRemotingParameters(
                ImmutableAuxiliaryRemotingParameters.copyOf(parameters).withShouldRetry(true));
    }

    private static <T> T client(Class<T> clazz, ClientConfiguration clientConfiguration) {
        if (isDialogue(clazz)) {
            return dialogueClient(clazz, clientConfiguration);
        }
        return conjureClient(clazz, clientConfiguration);
    }

    private static <T> T conjureClient(Class<T> clazz, ClientConfiguration clientConfiguration) {
        return JaxRsClient.create(
                clazz,
                clientConfiguration.userAgent()
                        .orElseThrow(() -> new SafeIllegalStateException("User agent must be provided")),
                HOST_METRICS_REGISTRY,
                clientConfiguration);
    }

    private static <T> T dialogueClient(Class<T> clazz, ClientConfiguration clientConfiguration) {
        return callStaticFactoryMethod(clazz, ApacheHttpClientChannels.create(clientConfiguration));
    }


    private static boolean isDialogue(Class<?> serviceInterface) {
        return getStaticOfMethod(serviceInterface).isPresent();
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
}
