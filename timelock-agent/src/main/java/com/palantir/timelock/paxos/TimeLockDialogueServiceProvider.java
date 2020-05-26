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

import java.util.Map;

import com.google.common.collect.ImmutableList;
import com.palantir.atlasdb.config.AuxiliaryRemotingParameters;
import com.palantir.atlasdb.config.ImmutableAuxiliaryRemotingParameters;
import com.palantir.atlasdb.config.ImmutableServerListConfig;
import com.palantir.atlasdb.config.ServerListConfig;
import com.palantir.atlasdb.http.AtlasDbRemotingConstants;
import com.palantir.atlasdb.http.v2.DialogueClientOptions;
import com.palantir.atlasdb.http.v2.ImmutableRemoteServiceConfiguration;
import com.palantir.atlasdb.http.v2.RemoteServiceConfiguration;
import com.palantir.common.streams.KeyedStream;
import com.palantir.conjure.java.api.config.service.UserAgent;
import com.palantir.conjure.java.client.config.ClientConfiguration;
import com.palantir.conjure.java.client.config.NodeSelectionStrategy;
import com.palantir.dialogue.clients.DialogueClients;
import com.palantir.refreshable.Refreshable;
import com.palantir.tritium.metrics.registry.TaggedMetricRegistry;

public class TimeLockDialogueServiceProvider {
    private final DialogueClients.ReloadingFactory reloadingFactory;
    private final TaggedMetricRegistry taggedMetricRegistry;

    private TimeLockDialogueServiceProvider(
            DialogueClients.ReloadingFactory reloadingFactory,
            TaggedMetricRegistry taggedMetricRegistry) {
        this.reloadingFactory = reloadingFactory;
        this.taggedMetricRegistry = taggedMetricRegistry;
    }

    public static TimeLockDialogueServiceProvider create(
            TaggedMetricRegistry taggedMetricRegistry,
            DialogueClients.ReloadingFactory baseFactory,
            ServerListConfig serverListConfig,
            AuxiliaryRemotingParameters parameters) {
        UserAgent versionedAgent = parameters.userAgent().addAgent(AtlasDbRemotingConstants.ATLASDB_HTTP_CLIENT_AGENT);
        Map<String, RemoteServiceConfiguration> remoteServiceConfigurations
                = createRemoteServiceConfigurations(serverListConfig, versionedAgent, parameters);
        DialogueClients.ReloadingFactory reloadingFactory
                = decorate(baseFactory, Refreshable.only(remoteServiceConfigurations));
        return new TimeLockDialogueServiceProvider(reloadingFactory, taggedMetricRegistry);
    }

    private static Map<String, RemoteServiceConfiguration> createRemoteServiceConfigurations(
            ServerListConfig serverListConfig, UserAgent versionedAgent, AuxiliaryRemotingParameters parameters) {
        return KeyedStream.of(serverListConfig.servers())
                .map(server -> ImmutableServerListConfig.builder()
                        .from(serverListConfig)
                        .servers(ImmutableList.of(server))
                        .build())
                .<RemoteServiceConfiguration>map(singleServerConfig -> ImmutableRemoteServiceConfiguration.builder()
                        .remotingParameters(ImmutableAuxiliaryRemotingParameters.builder()
                                .from(parameters)
                                .userAgent(versionedAgent)
                                .build())
                        .serverList(singleServerConfig)
                        .build())
                .collectToMap();
    }

    private static DialogueClients.ReloadingFactory decorate(
            DialogueClients.ReloadingFactory baseFactory,
            Refreshable<Map<String, RemoteServiceConfiguration>> serviceToRemoteConfiguration) {
        return baseFactory.reloading(serviceToRemoteConfiguration.map(
                DialogueClientOptions::toServicesConfigBlock))
                .withNodeSelectionStrategy(NodeSelectionStrategy.PIN_UNTIL_ERROR_WITHOUT_RESHUFFLE)
                .withClientQoS(ClientConfiguration.ClientQoS.DANGEROUS_DISABLE_SYMPATHETIC_CLIENT_QOS);
    }
}
