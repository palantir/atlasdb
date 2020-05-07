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

import java.lang.reflect.Method;
import java.util.Map;
import java.util.Optional;

import com.google.common.collect.ImmutableMap;
import com.palantir.atlasdb.config.AuxiliaryRemotingParameters;
import com.palantir.atlasdb.config.ServerListConfig;
import com.palantir.atlasdb.factory.timelock.BlockingAndNonBlockingServices;
import com.palantir.atlasdb.factory.timelock.BlockingSensitiveConjureTimelockService;
import com.palantir.atlasdb.factory.timelock.ImmutableBlockingAndNonBlockingServices;
import com.palantir.atlasdb.http.AtlasDbHttpProtocolVersion;
import com.palantir.atlasdb.http.AtlasDbRemotingConstants;
import com.palantir.atlasdb.http.v2.AtlasDbDialogueFactories;
import com.palantir.atlasdb.http.v2.FastFailoverProxy;
import com.palantir.atlasdb.http.v2.ImmutableRemoteServiceConfiguration;
import com.palantir.atlasdb.http.v2.RemoteServiceConfiguration;
import com.palantir.atlasdb.timelock.api.ConjureTimelockService;
import com.palantir.atlasdb.timelock.api.ConjureTimelockServiceBlocking;
import com.palantir.conjure.java.api.config.service.UserAgent;
import com.palantir.dialogue.Channel;
import com.palantir.dialogue.ConjureRuntime;
import com.palantir.dialogue.clients.DialogueClients;
import com.palantir.lock.client.DialogueAdaptingConjureTimelockService;
import com.palantir.logsafe.Preconditions;
import com.palantir.refreshable.Refreshable;

/**
 * Provides a mechanism for accessing services that use Dialogue for communication. A service is defined as a cluster of
 * zero or more nodes, where contacting any of the nodes is legitimate (subject to redirects via 308s and 503s). If
 * working with heterogeneous nodes and/or broadcast is important (e.g. for Paxos Acceptor use cases), you should be
 * VERY careful when using this class.
 *
 * Proxies must be resilient to servers repeatedly returning 308s that are large in number, but persist for only a short
 * duration. Furthermore, proxies should include in their {@link com.palantir.conjure.java.api.config.service.UserAgent}
 * information to allow client services to identify the protocol they are using to talk, via
 * {@link AtlasDbHttpProtocolVersion}.
 */
final class AtlasDbDialogueServiceProvider {
    private static final String TIMELOCK_NON_BLOCKING = "timelock-non-blocking";
    private static final String TIMELOCK_BLOCKING = "timelock-blocking";
    private final DialogueClients.ReloadingFactory dialogueClientFactory;

    private AtlasDbDialogueServiceProvider(DialogueClients.ReloadingFactory dialogueClientFactory) {
        this.dialogueClientFactory = dialogueClientFactory;
    }

    static AtlasDbDialogueServiceProvider create(
            Refreshable<ServerListConfig> timeLockServerListConfig,
            DialogueClients.ReloadingFactory baseFactory,
            UserAgent userAgent) {
        UserAgent versionedAgent = userAgent.addAgent(AtlasDbRemotingConstants.ATLASDB_HTTP_CLIENT_AGENT);
        Refreshable<Map<String, RemoteServiceConfiguration>> timeLockRemoteConfigurations = timeLockServerListConfig
                .map(serverListConfig -> ImmutableMap.of(
                        TIMELOCK_NON_BLOCKING,
                        createRemoteServiceConfiguration(versionedAgent, serverListConfig, false),
                        TIMELOCK_BLOCKING,
                        createRemoteServiceConfiguration(versionedAgent, serverListConfig, true)));
        DialogueClients.ReloadingFactory reloadingFactory
                = AtlasDbDialogueFactories.decorateForFailoverServices(baseFactory, timeLockRemoteConfigurations)
                        .withUserAgent(versionedAgent);

        return new AtlasDbDialogueServiceProvider(reloadingFactory);
    }

    ConjureTimelockService getConjureTimelockService() {
        Preconditions.checkState(isDialogue(ConjureTimelockServiceBlocking.class),
                "Dialogue service provider attempted to provide a non-Dialogue class."
                        + " This is an AtlasDB bug.");
        ConjureTimelockServiceBlocking rawBlockingService
                = dialogueClientFactory.get(ConjureTimelockServiceBlocking.class, TIMELOCK_BLOCKING);
        ConjureTimelockServiceBlocking rawNonBlockingService
                = dialogueClientFactory.get(ConjureTimelockServiceBlocking.class, TIMELOCK_NON_BLOCKING);

        // Blocking here is overloaded, sigh: the inner one means that we wait for a response, the outer one
        // means that requests on the server side might block waiting for resources to be available...
        BlockingAndNonBlockingServices<ConjureTimelockService> blockingAndNonBlockingServices
                = ImmutableBlockingAndNonBlockingServices.<ConjureTimelockServiceBlocking>builder()
                        .blocking(rawBlockingService)
                        .nonBlocking(rawNonBlockingService)
                        .build()
                        .map(proxy -> FastFailoverProxy.newProxyInstance(
                                ConjureTimelockServiceBlocking.class, () -> proxy))
                        .map(DialogueAdaptingConjureTimelockService::new);

        return new BlockingSensitiveConjureTimelockService(blockingAndNonBlockingServices);
    }

    private static ImmutableRemoteServiceConfiguration createRemoteServiceConfiguration(
            UserAgent userAgent, ServerListConfig serverListConfig, boolean shouldSupportBlockingOperations) {
        return ImmutableRemoteServiceConfiguration.builder()
                .remotingParameters(getFailoverRemotingParameters(shouldSupportBlockingOperations, userAgent))
                .serverList(serverListConfig)
                .build();
    }

    private static AuxiliaryRemotingParameters getFailoverRemotingParameters(
            boolean shouldSupportBlockingOperations, UserAgent userAgent) {
        return AuxiliaryRemotingParameters.builder()
                .shouldLimitPayload(true)
                .shouldRetry(true)
                .shouldSupportBlockingOperations(shouldSupportBlockingOperations)
                .userAgent(userAgent)
                .build();
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
}
