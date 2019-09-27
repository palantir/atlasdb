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

import java.net.ProxySelector;
import java.time.Duration;
import java.util.Collection;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;

import com.google.common.collect.ImmutableList;
import com.palantir.atlasdb.config.AuxiliaryRemotingParameters;
import com.palantir.atlasdb.config.ImmutableServerListConfig;
import com.palantir.atlasdb.config.ServerListConfig;
import com.palantir.atlasdb.http.AtlasDbRemotingConstants;
import com.palantir.atlasdb.http.PollingRefreshable;
import com.palantir.atlasdb.http.TargetFactory;
import com.palantir.conjure.java.api.config.service.ProxyConfiguration;
import com.palantir.conjure.java.api.config.service.UserAgent;
import com.palantir.conjure.java.api.config.ssl.SslConfiguration;
import com.palantir.conjure.java.client.config.ClientConfiguration;
import com.palantir.conjure.java.client.jaxrs.JaxRsClient;
import com.palantir.conjure.java.config.ssl.TrustContext;
import com.palantir.conjure.java.ext.refresh.Refreshable;
import com.palantir.conjure.java.okhttp.HostMetricsRegistry;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;

public final class ConjureJavaRuntimeTargetFactory implements TargetFactory {
    private static final HostMetricsRegistry HOST_METRICS_REGISTRY = new HostMetricsRegistry();

    public static final ConjureJavaRuntimeTargetFactory DEFAULT = new ConjureJavaRuntimeTargetFactory();

    private ConjureJavaRuntimeTargetFactory() {
        // Use the instances.
    }

    @Override
    public <T> T createProxyWithoutRetrying(
            Optional<TrustContext> trustContext,
            String uri,
            Class<T> type,
            AuxiliaryRemotingParameters clientParameters) {
        ClientConfiguration clientConfiguration = ClientOptions.DEFAULT_NO_RETRYING.create(
                ImmutableList.of(uri),
                Optional.empty(),
                trustContext.orElseThrow(() -> new IllegalStateException("CJR requires a trust context")));
        return JaxRsClient.create(
                type,
                addAtlasDbRemotingAgent(clientParameters.userAgent()),
                new HostMetricsRegistry(),
                clientConfiguration);
    }

    @Override
    public <T> T createProxy(Optional<TrustContext> trustContext, String uri, Class<T> type,
            AuxiliaryRemotingParameters parameters) {
        ClientConfiguration clientConfiguration = ClientOptions.DEFAULT_RETRYING.create(
                ImmutableList.of(uri),
                Optional.empty(),
                trustContext.orElseThrow(() -> new IllegalStateException("CJR requires a trust context")));
        return JaxRsClient.create(
                type,
                addAtlasDbRemotingAgent(parameters.userAgent()),
                new HostMetricsRegistry(),
                clientConfiguration);
    }

    @Override
    public <T> T createProxyWithFailover(
            Optional<TrustContext> trustContext,
            Optional<ProxySelector> proxySelector,
            Collection<String> endpointUris,
            Class<T> type,
            AuxiliaryRemotingParameters parameters) {
        ClientConfiguration clientConfiguration = ClientOptions.FAST_RETRYING_FOR_TEST.create(
                ImmutableList.copyOf(endpointUris),
                proxySelector,
                trustContext.orElseThrow(() -> new SafeIllegalStateException("CJR requires a trust context")));

        return JaxRsClient.create(
                type,
                addAtlasDbRemotingAgent(parameters.userAgent()),
                HOST_METRICS_REGISTRY,
                clientConfiguration);
    }

    @Override
    public <T> T createLiveReloadingProxyWithFailover(
            Supplier<ServerListConfig> serverListConfigSupplier,
            Function<SslConfiguration, TrustContext> trustContextCreator,
            Function<ProxyConfiguration, ProxySelector> proxySelectorCreator,
            Class<T> type,
            AuxiliaryRemotingParameters parameters) {
        // TODO (jkong): Thread leak for days. To be fair no regression from AtlasDbFeignTargetFactory
        // TODO (jkong): Add RetryOtherRetryingProxy layer.
        Supplier<ServerListConfig> nonEmptyServerList = () -> injectDummyServer(serverListConfigSupplier);
        Refreshable<ClientConfiguration> refreshableConfig = PollingRefreshable
                .createComposed(nonEmptyServerList,
                        Duration.ofSeconds(5L),
                        ClientOptions.FAST_RETRYING_FOR_TEST::serverListToClient)
                .getRefreshable();
        return JaxRsClient.create(
                type,
                addAtlasDbRemotingAgent(parameters.userAgent()),
                HOST_METRICS_REGISTRY,
                refreshableConfig);
    }

    @Override
    public String getClientVersion() {
        return null;
    }

    // TODO (gmaretic): This is a hack because CJR doesn't like configurations with 0 servers, yet we claim
    // that we might encounter such configurations in k8s when the first node discovers other available remotes.
    private static ServerListConfig injectDummyServer(Supplier<ServerListConfig> serverListConfigSupplier) {
        ServerListConfig originalConfig = serverListConfigSupplier.get();
        if (originalConfig.hasAtLeastOneServer()) {
            return originalConfig;
        }
        return ImmutableServerListConfig.builder().from(serverListConfigSupplier.get())
                .addServers("http://dummy")
                .build();
    }

    private static UserAgent addAtlasDbRemotingAgent(UserAgent agent) {
        return agent.addAgent(AtlasDbRemotingConstants.ATLASDB_HTTP_CLIENT_AGENT);
    }
}
