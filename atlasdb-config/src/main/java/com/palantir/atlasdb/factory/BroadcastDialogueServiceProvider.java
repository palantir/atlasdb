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

package com.palantir.atlasdb.factory;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.palantir.atlasdb.config.AuxiliaryRemotingParameters;
import com.palantir.atlasdb.config.ImmutableAuxiliaryRemotingParameters;
import com.palantir.atlasdb.config.ImmutableServerListConfig;
import com.palantir.atlasdb.config.ServerListConfig;
import com.palantir.atlasdb.http.AtlasDbHttpProtocolVersion;
import com.palantir.atlasdb.http.v2.DialogueClientOptions;
import com.palantir.atlasdb.http.v2.DialogueShimFactory;
import com.palantir.atlasdb.http.v2.ImmutableRemoteServiceConfiguration;
import com.palantir.atlasdb.http.v2.RemoteServiceConfiguration;
import com.palantir.common.streams.KeyedStream;
import com.palantir.conjure.java.api.config.service.UserAgent;
import com.palantir.dialogue.Channel;
import com.palantir.dialogue.clients.DialogueClients;
import com.palantir.refreshable.Refreshable;

/**
 * Provides a mechanism for creating individual proxies that use Dialogue for communication for each node in the cluster.
 *
 * Furthermore, proxies should include in their {@link com.palantir.conjure.java.api.config.service.UserAgent}
 * information to allow client services to identify the protocol they are using to talk, via
 * {@link AtlasDbHttpProtocolVersion}.
 */
public final class BroadcastDialogueServiceProvider {
    DialogueClients.ReloadingFactory reloadingFactory;
    Refreshable<ServerListConfig> serverListConfigSupplier;

    private BroadcastDialogueServiceProvider(DialogueClients.ReloadingFactory reloadingFactory,
            Refreshable<ServerListConfig> serverListConfigSupplier) {
        this.reloadingFactory = reloadingFactory;
        this.serverListConfigSupplier = serverListConfigSupplier;
    }

    public static BroadcastDialogueServiceProvider create(
            DialogueClients.ReloadingFactory baseFactory,
            Refreshable<ServerListConfig> serverListConfigSupplier,
            UserAgent versionedAgent,
            AuxiliaryRemotingParameters parameters) {
        Refreshable<Map<String, RemoteServiceConfiguration>> timeLockRemoteConfigurations = serverListConfigSupplier
                .map(serverListConfig -> createRemoteServiceConfigurations(
                        serverListConfig,
                        versionedAgent,
                        parameters));

        DialogueClients.ReloadingFactory reloadingFactory = baseFactory.reloading(
                timeLockRemoteConfigurations.map(DialogueClientOptions::toServicesConfigBlock))
                .withUserAgent(versionedAgent);
        return new BroadcastDialogueServiceProvider(reloadingFactory, serverListConfigSupplier);
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
        return Maps.immutableEntry(getServiceName(uri, shouldRetry), proxy);
    }

    public <T> Refreshable<List<T>> getSingleNodeProxies(Class<T> clazz, boolean shouldRetry) {
        return this.serverListConfigSupplier.map(serverListConfig -> serverListConfig.servers()
                .stream()
                .map(uri -> createDialogueProxy(
                        clazz,
                        reloadingFactory.getChannel(getServiceName(uri, shouldRetry))))
                .collect(Collectors.toList()));
    }


    private static <T> T createDialogueProxy(Class<T> type, Channel channel) {
        return DialogueShimFactory.create(type, channel);
    }

    private static String getServiceName(String uri, boolean shouldRetry) {
        return (shouldRetry ? "retrying" : "nonRetrying") + "-" + uri;
    }
}
