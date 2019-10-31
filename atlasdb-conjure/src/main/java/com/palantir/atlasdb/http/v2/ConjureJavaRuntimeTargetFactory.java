/*
 * (c) Copyright 2019 Palantir Technologies Inc. All rights reserved.
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

import java.time.Duration;
import java.util.Optional;
import java.util.function.Supplier;

import com.google.common.collect.ImmutableList;
import com.palantir.atlasdb.config.AuxiliaryRemotingParameters;
import com.palantir.atlasdb.config.ServerListConfig;
import com.palantir.atlasdb.http.AtlasDbRemotingConstants;
import com.palantir.atlasdb.http.ImmutableInstanceAndVersion;
import com.palantir.atlasdb.http.PollingRefreshable;
import com.palantir.atlasdb.http.TargetFactory;
import com.palantir.conjure.java.api.config.service.UserAgent;
import com.palantir.conjure.java.client.config.ClientConfiguration;
import com.palantir.conjure.java.client.jaxrs.JaxRsClient;
import com.palantir.conjure.java.config.ssl.TrustContext;
import com.palantir.conjure.java.ext.refresh.Refreshable;
import com.palantir.conjure.java.okhttp.HostMetricsRegistry;
import com.palantir.util.CachedTransformingSupplier;

public final class ConjureJavaRuntimeTargetFactory implements TargetFactory {
    private static final HostMetricsRegistry HOST_METRICS_REGISTRY = new HostMetricsRegistry();

    public static final ConjureJavaRuntimeTargetFactory DEFAULT = new ConjureJavaRuntimeTargetFactory();
    public static final String CLIENT_VERSION_STRING = "Conjure-Java-Runtime";

    private ConjureJavaRuntimeTargetFactory() {
        // Use the instances.
    }

    @Override
    public <T> InstanceAndVersion<T> createProxy(
            Optional<TrustContext> trustContext,
            String uri,
            Class<T> type,
            AuxiliaryRemotingParameters parameters) {
        ClientOptions relevantOptions = parameters.shouldRetry()
                ? ClientOptions.DEFAULT_RETRYING
                : ClientOptions.DEFAULT_NO_RETRYING;
        ClientConfiguration clientConfiguration = relevantOptions.create(
                ImmutableList.of(uri),
                Optional.empty(),
                trustContext.orElseThrow(() -> new IllegalStateException("CJR requires a trust context")));
        T client = JaxRsClient.create(
                type,
                addAtlasDbRemotingAgent(parameters.userAgent()),
                HOST_METRICS_REGISTRY,
                clientConfiguration);
        if (parameters.shouldRetry()) {
            client = FastFailoverProxy.newProxyInstance(type, client);
        }
        return wrapWithVersion(client);
    }

    @Override
    public <T> InstanceAndVersion<T> createProxyWithFailover(
            ServerListConfig serverListConfig,
            Class<T> type,
            AuxiliaryRemotingParameters parameters) {
        ClientConfiguration clientConfiguration = ClientOptions.DEFAULT_RETRYING.serverListToClient(serverListConfig);

        return wrapWithVersion(JaxRsClient.create(
                type,
                addAtlasDbRemotingAgent(parameters.userAgent()),
                HOST_METRICS_REGISTRY,
                clientConfiguration));
    }

    @Override
    public <T> InstanceAndVersion<T> createLiveReloadingProxyWithFailover(
            Supplier<ServerListConfig> serverListConfigSupplier,
            Class<T> type,
            AuxiliaryRemotingParameters parameters) {
        // TODO (jkong): Thread leak for days. Though no regression over existing implementation in Feign.
        Refreshable<ClientConfiguration> refreshableConfig = PollingRefreshable
                .create(
                        new CachedTransformingSupplier<>(
                                serverListConfigSupplier, ClientOptions.DEFAULT_RETRYING::serverListToClient),
                        Duration.ofSeconds(5L))
                .getRefreshable();
        T client = JaxRsClient.create(
                type,
                addAtlasDbRemotingAgent(parameters.userAgent()),
                HOST_METRICS_REGISTRY,
                refreshableConfig);
        if (parameters.shouldRetry()) {
            client = FastFailoverProxy.newProxyInstance(type, client);
        }
        return wrapWithVersion(client);
    }

    private static UserAgent addAtlasDbRemotingAgent(UserAgent agent) {
        return agent.addAgent(AtlasDbRemotingConstants.ATLASDB_HTTP_CLIENT_AGENT);
    }

    private static <T> InstanceAndVersion<T> wrapWithVersion(T instance) {
        return ImmutableInstanceAndVersion.of(instance, CLIENT_VERSION_STRING);
    }
}
