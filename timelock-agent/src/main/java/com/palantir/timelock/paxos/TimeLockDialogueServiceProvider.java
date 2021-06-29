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

package com.palantir.timelock.paxos;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.palantir.atlasdb.config.AuxiliaryRemotingParameters;
import com.palantir.atlasdb.config.ImmutableAuxiliaryRemotingParameters;
import com.palantir.atlasdb.config.ImmutableServerListConfig;
import com.palantir.atlasdb.config.ServerListConfig;
import com.palantir.atlasdb.http.AtlasDbHttpClients;
import com.palantir.atlasdb.http.AtlasDbRemotingConstants;
import com.palantir.atlasdb.http.v2.DialogueClientOptions;
import com.palantir.atlasdb.http.v2.ImmutableRemoteServiceConfiguration;
import com.palantir.atlasdb.http.v2.RemoteServiceConfiguration;
import com.palantir.atlasdb.util.MetricsManager;
import com.palantir.common.streams.KeyedStream;
import com.palantir.conjure.java.api.config.service.UserAgent;
import com.palantir.conjure.java.client.config.ClientConfiguration;
import com.palantir.conjure.java.client.config.NodeSelectionStrategy;
import com.palantir.dialogue.clients.DialogueClients;
import com.palantir.dialogue.clients.DialogueClients.ReloadingFactory;
import com.palantir.refreshable.Refreshable;
import com.palantir.tritium.metrics.registry.TaggedMetricRegistry;
import java.util.Map;
import java.util.stream.Stream;

public final class TimeLockDialogueServiceProvider {
    private final DialogueClients.ReloadingFactory reloadingFactory;
    private final TaggedMetricRegistry taggedMetricRegistry;
    private final MetricsManager metricsManager;
    private final ServerListConfig serverListConfig;
    private final UserAgent userAgent;

    private TimeLockDialogueServiceProvider(
            UserAgent versionedAgent,
            ServerListConfig serverListConfig,
            ReloadingFactory reloadingFactory,
            MetricsManager metricsManager) {
        this.userAgent = versionedAgent;
        this.serverListConfig = serverListConfig;
        this.reloadingFactory = reloadingFactory;
        this.metricsManager = metricsManager;
        this.taggedMetricRegistry = metricsManager.getTaggedRegistry();
    }

    public static TimeLockDialogueServiceProvider create(
            MetricsManager metricsManager,
            DialogueClients.ReloadingFactory baseFactory,
            ServerListConfig serverListConfig,
            AuxiliaryRemotingParameters parameters) {
        UserAgent versionedAgent = parameters.userAgent().addAgent(AtlasDbRemotingConstants.ATLASDB_HTTP_CLIENT_AGENT);
        Map<String, RemoteServiceConfiguration> remoteServiceConfigurations =
                createRemoteServiceConfigurations(serverListConfig, versionedAgent, parameters);
        DialogueClients.ReloadingFactory reloadingFactory = decorate(
                        baseFactory, Refreshable.only(remoteServiceConfigurations))
                .withUserAgent(versionedAgent);
        return new TimeLockDialogueServiceProvider(versionedAgent, serverListConfig, reloadingFactory, metricsManager);
    }

    private static Map<String, RemoteServiceConfiguration> createRemoteServiceConfigurations(
            ServerListConfig serverListConfig, UserAgent versionedAgent, AuxiliaryRemotingParameters parameters) {
        return KeyedStream.of(serverListConfig.servers())
                .map(server -> ImmutableServerListConfig.builder()
                        .from(serverListConfig)
                        .servers(ImmutableList.of(server))
                        .build())
                .flatMapEntries((uri, singleServerConfig) -> Stream.of(false, true)
                        .map(retry -> createSingleServiceConfigurationMapping(
                                uri, singleServerConfig, parameters, versionedAgent, retry)))
                .collectToMap();
    }

    /**
     * Creates a proxy to a single node. It is expected that this single node is one of the servers in the
     * {@link ServerListConfig} provided to this {@link TimeLockDialogueServiceProvider}.
     */
    public <T> T createSingleNodeInstrumentedProxy(String uri, Class<T> clazz, boolean shouldRetry) {
        return AtlasDbHttpClients.createDialogueProxy(
                taggedMetricRegistry, clazz, reloadingFactory.getChannel(getServiceNameForTimeLock(uri, shouldRetry)));
    }

    public <T> T createSingleProxyForNodes(Class<T> clazz) {
        return AtlasDbHttpClients.createProxyWithFailover(
                metricsManager,
                serverListConfig,
                clazz,
                AuxiliaryRemotingParameters.builder()
                        .shouldUseExtendedTimeout(true)
                        .shouldLimitPayload(true)
                        .shouldRetry(true)
                        .userAgent(userAgent)
                        .build());
    }

    private static Map.Entry<String, RemoteServiceConfiguration> createSingleServiceConfigurationMapping(
            String uri,
            ServerListConfig singleServerConfig,
            AuxiliaryRemotingParameters parameters,
            UserAgent userAgent,
            boolean shouldRetry) {
        RemoteServiceConfiguration proxy = ImmutableRemoteServiceConfiguration.builder()
                .remotingParameters(ImmutableAuxiliaryRemotingParameters.builder()
                        .from(parameters)
                        .userAgent(userAgent)
                        .shouldRetry(shouldRetry)
                        .build())
                .serverList(singleServerConfig)
                .build();
        return Maps.immutableEntry(getServiceNameForTimeLock(uri, shouldRetry), proxy);
    }

    private static DialogueClients.ReloadingFactory decorate(
            DialogueClients.ReloadingFactory baseFactory,
            Refreshable<Map<String, RemoteServiceConfiguration>> serviceToRemoteConfiguration) {
        return baseFactory
                .reloading(serviceToRemoteConfiguration.map(DialogueClientOptions::toServicesConfigBlock))
                .withNodeSelectionStrategy(NodeSelectionStrategy.PIN_UNTIL_ERROR_WITHOUT_RESHUFFLE)
                .withClientQoS(ClientConfiguration.ClientQoS.DANGEROUS_DISABLE_SYMPATHETIC_CLIENT_QOS);
    }

    private static String getServiceNameForTimeLock(String timelockUri, boolean shouldRetry) {
        return "timelock-" + (shouldRetry ? "retrying" : "nonRetrying") + "-" + timelockUri;
    }
}
