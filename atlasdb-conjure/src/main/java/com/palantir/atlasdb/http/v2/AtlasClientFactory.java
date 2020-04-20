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

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.time.Duration;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;

import com.palantir.atlasdb.config.ServerListConfig;
import com.palantir.common.proxy.ReplaceIfExceptionMatchingProxy;
import com.palantir.conjure.java.api.config.service.UserAgent;
import com.palantir.conjure.java.client.config.ClientConfiguration;
import com.palantir.conjure.java.client.jaxrs.JaxRsClient;
import com.palantir.conjure.java.okhttp.HostEventsSink;
import com.palantir.logsafe.Preconditions;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;
import com.palantir.refreshable.Refreshable;
import com.palantir.tritium.Tritium;
import com.palantir.tritium.metrics.registry.TaggedMetricRegistry;

public class AtlasClientFactory implements AutoCloseable {
    static final StaticClientConfiguration EMPTY_STATIC_CONFIG =
            StaticClientConfiguration.builder().build();

    private final boolean useDialogueJaxrs;
    private final Refreshable<Optional<ServerListConfig>> clientConfig;
    private final TaggedMetricRegistry taggedMetricRegistry;
    private final UserAgent userAgent;
    private final HostEventsSink hostEventsSink;
    private final DialogueClientFactory dialogueClientFactory;

    public AtlasClientFactory(
            Refreshable<ServerListConfig> clientConfig,
            TaggedMetricRegistry taggedMetricRegistry,
            UserAgent userAgent,
            HostEventsSink hostEventsSink,
            boolean useDialogueJaxrs) {
        this.useDialogueJaxrs = useDialogueJaxrs;
        this.clientConfig = clientConfig.map(c -> c.hasAtLeastOneServer() ? Optional.of(c) : Optional.empty());
        this.taggedMetricRegistry = taggedMetricRegistry;
        this.userAgent = userAgent;
        this.hostEventsSink = hostEventsSink;
        this.dialogueClientFactory = new DialogueClientFactory(this.clientConfig, taggedMetricRegistry, userAgent);
    }

    public <T> T client(Class<T> serviceClass) {
        return client(serviceClass, EMPTY_STATIC_CONFIG);
    }

    // TODO(forozco): support multiple services
    public <T> T client(Class<T> serviceClass, StaticClientConfiguration staticConfig) {
        SafeArg<String> serviceClassSafeArg = SafeArg.of("serviceClass", serviceClass.getName());
        Preconditions.checkArgument(
                serviceClass.isInterface(), "serviceClass must be an interface", serviceClassSafeArg);

        if (DialogueClientFactory.isDialogue(serviceClass)) {
            return dialogueClientFactory.dialogueClient(serviceClass, staticConfig);
        } else if (useDialogueJaxrs) {
            return dialogueClientFactory.jaxrsClient(serviceClass, staticConfig);
        }
        return jaxrsInternal(serviceClass, staticConfig);
    }

    private <T> T jaxrsInternal(Class<T> serviceClass, StaticClientConfiguration staticClientConfig) {
        Function<ServerListConfig, T> clientFactory = jaxrsFactory(serviceClass,
                staticClientConfig);
        Supplier<Optional<T>> client = clientConfig.map(maybeConf ->
                maybeConf.map(conf ->
                        ReplaceIfExceptionMatchingProxy.create(
                                serviceClass,
                                () -> clientFactory.apply(conf),
                                Duration.ofMinutes(20),
                                OkHttpBugs::isPossiblyOkHttpTimeoutBug)));
        T alwaysThrowingService = serviceProxy(serviceClass, (proxy, method, args) -> {
            throw new SafeIllegalStateException("Service not configured");
        });


        return serviceProxy(serviceClass, (proxy, method, args) -> {
            T service = client.get().orElse(alwaysThrowingService);
            try {
                return method.invoke(service, args);
            } catch (InvocationTargetException e) {
                throw e.getCause();
            }
        });
    }

    private <T> Function<ServerListConfig, T> jaxrsFactory(
            Class<T> serviceClass, StaticClientConfiguration staticConfig) {
        return conf -> {
            ClientConfiguration clientConfiguration = toClientConfig(staticConfig, conf);
            T service = JaxRsClient.create(serviceClass, userAgent, hostEventsSink, clientConfiguration);
            return Tritium.instrument(serviceClass, service, taggedMetricRegistry);
        };
    }

    private <T> T serviceProxy(Class<T> serviceClass, InvocationHandler handler) {
        Object service = Proxy.newProxyInstance(serviceClass.getClassLoader(), new Class[] {serviceClass}, handler);
        return serviceClass.cast(service);
    }

    private ClientConfiguration toClientConfig(
            StaticClientConfiguration staticClientConfig, ServerListConfig serverListConfig) {
        return ClientConfiguration.builder()
                .from(StaticClientConfigurations.apply(staticClientConfig, serverListConfig))
                .taggedMetricRegistry(taggedMetricRegistry)
                .userAgent(userAgent)
                .build();
    }

    @Override
    public void close() {
        dialogueClientFactory.close();
    }
}
