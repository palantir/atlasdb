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

import com.google.common.collect.ImmutableMap;
import com.palantir.atlasdb.config.AuxiliaryRemotingParameters;
import com.palantir.atlasdb.config.ServerListConfig;
import com.palantir.atlasdb.factory.timelock.ImmutableShortAndLongTimeoutServices;
import com.palantir.atlasdb.factory.timelock.ShortAndLongTimeoutServices;
import com.palantir.atlasdb.factory.timelock.TimeoutSensitiveConjureTimelockService;
import com.palantir.atlasdb.factory.timelock.TimeoutSensitiveLockRpcClient;
import com.palantir.atlasdb.http.AtlasDbHttpClients;
import com.palantir.atlasdb.http.AtlasDbHttpProtocolVersion;
import com.palantir.atlasdb.http.AtlasDbRemotingConstants;
import com.palantir.atlasdb.http.v2.DialogueClientOptions;
import com.palantir.atlasdb.http.v2.FastFailoverProxy;
import com.palantir.atlasdb.http.v2.ImmutableRemoteServiceConfiguration;
import com.palantir.atlasdb.http.v2.RemoteServiceConfiguration;
import com.palantir.atlasdb.http.v2.RetryOnSocketTimeoutExceptionProxy;
import com.palantir.atlasdb.timelock.api.ConjureTimelockService;
import com.palantir.atlasdb.timelock.api.ConjureTimelockServiceBlocking;
import com.palantir.atlasdb.timelock.api.MultiClientConjureTimelockServiceBlocking;
import com.palantir.atlasdb.timelock.lock.watch.ConjureLockWatchingServiceBlocking;
import com.palantir.atlasdb.util.AtlasDbMetrics;
import com.palantir.conjure.java.api.config.service.UserAgent;
import com.palantir.conjure.java.client.config.ClientConfiguration;
import com.palantir.conjure.java.client.config.NodeSelectionStrategy;
import com.palantir.dialogue.Channel;
import com.palantir.dialogue.clients.DialogueClients;
import com.palantir.lock.ConjureLockV1ServiceBlocking;
import com.palantir.lock.LockRpcClient;
import com.palantir.lock.client.ConjureTimelockServiceBlockingMetrics;
import com.palantir.lock.client.DialogueAdaptingConjureTimelockService;
import com.palantir.lock.client.DialogueComposingLockRpcClient;
import com.palantir.lock.v2.TimelockRpcClient;
import com.palantir.refreshable.Refreshable;
import com.palantir.timestamp.TimestampManagementRpcClient;
import com.palantir.tritium.metrics.registry.TaggedMetricRegistry;
import java.util.Map;

/**
 * Provides a mechanism for accessing services that use Dialogue for communication. A service is defined as a cluster of
 * zero or more nodes, where contacting any of the nodes is legitimate (subject to redirects via 308s and 503s). If
 * working with heterogeneous nodes and/or broadcast is important (e.g. for Paxos Acceptor use cases), you should be
 * VERY careful when using this class.
 * <p>
 * Proxies must be resilient to servers repeatedly returning 308s that are large in number, but persist for only a short
 * duration. Furthermore, proxies should include in their {@link com.palantir.conjure.java.api.config.service.UserAgent}
 * information to allow client services to identify the protocol they are using to talk, via {@link
 * AtlasDbHttpProtocolVersion}.
 */
public final class AtlasDbDialogueServiceProvider {
    private static final String TIMELOCK_SHORT_TIMEOUT = "timelock-short-timeout";
    private static final String TIMELOCK_LONG_TIMEOUT = "timelock-long-timeout";

    private final DialogueClients.ReloadingFactory dialogueClientFactory;
    private final TaggedMetricRegistry taggedMetricRegistry;

    private AtlasDbDialogueServiceProvider(
            DialogueClients.ReloadingFactory dialogueClientFactory, TaggedMetricRegistry taggedMetricRegistry) {
        this.dialogueClientFactory = dialogueClientFactory;
        this.taggedMetricRegistry = taggedMetricRegistry;
    }

    public static AtlasDbDialogueServiceProvider create(
            Refreshable<ServerListConfig> timeLockServerListConfig,
            DialogueClients.ReloadingFactory baseFactory,
            UserAgent userAgent,
            TaggedMetricRegistry taggedMetricRegistry) {
        UserAgent versionedAgent = userAgent.addAgent(AtlasDbRemotingConstants.ATLASDB_HTTP_CLIENT_AGENT);
        Refreshable<Map<String, RemoteServiceConfiguration>> timeLockRemoteConfigurations =
                timeLockServerListConfig.map(
                        serverListConfig -> getServiceConfigurations(versionedAgent, serverListConfig));
        DialogueClients.ReloadingFactory reloadingFactory = decorateForFailoverServices(
                        baseFactory, timeLockRemoteConfigurations)
                .withUserAgent(versionedAgent);

        return new AtlasDbDialogueServiceProvider(reloadingFactory, taggedMetricRegistry);
    }

    public ConjureTimelockService getConjureTimelockService() {
        ConjureTimelockServiceBlocking longTimeoutService =
                dialogueClientFactory.get(ConjureTimelockServiceBlocking.class, TIMELOCK_LONG_TIMEOUT);
        ConjureTimelockServiceBlocking shortTimeoutService =
                dialogueClientFactory.get(ConjureTimelockServiceBlocking.class, TIMELOCK_SHORT_TIMEOUT);
        ConjureTimelockServiceBlockingMetrics conjureTimelockServiceBlockingMetrics =
                ConjureTimelockServiceBlockingMetrics.of(taggedMetricRegistry);

        ShortAndLongTimeoutServices<ConjureTimelockService> shortAndLongTimeoutServices =
                ImmutableShortAndLongTimeoutServices.<ConjureTimelockServiceBlocking>builder()
                        .longTimeout(longTimeoutService)
                        .shortTimeout(shortTimeoutService)
                        .build()
                        .map(proxy -> wrapInProxy(ConjureTimelockServiceBlocking.class, proxy))
                        .map(service -> AtlasDbMetrics.instrumentWithTaggedMetrics(
                                taggedMetricRegistry, ConjureTimelockServiceBlocking.class, service))
                        .map(instrumentedService -> new DialogueAdaptingConjureTimelockService(
                                instrumentedService, conjureTimelockServiceBlockingMetrics));

        return new TimeoutSensitiveConjureTimelockService(shortAndLongTimeoutServices);
    }

    MultiClientConjureTimelockServiceBlocking getMultiClientConjureTimelockService() {
        MultiClientConjureTimelockServiceBlocking blockingService =
                dialogueClientFactory.get(MultiClientConjureTimelockServiceBlocking.class, TIMELOCK_SHORT_TIMEOUT);
        return AtlasDbMetrics.instrumentWithTaggedMetrics(
                taggedMetricRegistry,
                MultiClientConjureTimelockServiceBlocking.class,
                wrapInProxy(MultiClientConjureTimelockServiceBlocking.class, blockingService));
    }

    TimestampManagementRpcClient getTimestampManagementRpcClient() {
        return AtlasDbHttpClients.createUninstrumentedDialogueProxy(
                TimestampManagementRpcClient.class, dialogueClientFactory.getChannel(TIMELOCK_SHORT_TIMEOUT));
    }

    public LockRpcClient getLockRpcClient() {
        ConjureLockV1ServiceBlocking shortTimeoutDialogueService =
                dialogueClientFactory.get(ConjureLockV1ServiceBlocking.class, TIMELOCK_SHORT_TIMEOUT);
        ConjureLockV1ServiceBlocking longTimeoutDialogueService =
                dialogueClientFactory.get(ConjureLockV1ServiceBlocking.class, TIMELOCK_LONG_TIMEOUT);

        ShortAndLongTimeoutServices<LockRpcClient> legacyRpcClients =
                ImmutableShortAndLongTimeoutServices.<LockRpcClient>builder()
                        .shortTimeout(createDialogueProxyWithShortTimeout(LockRpcClient.class))
                        .longTimeout(createDialogueProxyWithLongTimeout(LockRpcClient.class))
                        .build();

        return new TimeoutSensitiveLockRpcClient(
                ImmutableShortAndLongTimeoutServices.<ConjureLockV1ServiceBlocking>builder()
                        .shortTimeout(shortTimeoutDialogueService)
                        .longTimeout(longTimeoutDialogueService)
                        .build()
                        .map(proxy -> wrapInProxy(ConjureLockV1ServiceBlocking.class, proxy))
                        .map(service -> AtlasDbMetrics.instrumentWithTaggedMetrics(
                                taggedMetricRegistry, ConjureLockV1ServiceBlocking.class, service))
                        .zipWith(legacyRpcClients, DialogueComposingLockRpcClient::new));
    }

    public TimelockRpcClient getTimelockRpcClient() {
        return createDialogueProxyWithShortTimeout(TimelockRpcClient.class);
    }

    ConjureLockWatchingServiceBlocking getConjureLockWatchingService() {
        ConjureLockWatchingServiceBlocking blockingService =
                dialogueClientFactory.get(ConjureLockWatchingServiceBlocking.class, TIMELOCK_SHORT_TIMEOUT);
        return AtlasDbMetrics.instrumentWithTaggedMetrics(
                taggedMetricRegistry,
                ConjureLockWatchingServiceBlocking.class,
                wrapInProxy(ConjureLockWatchingServiceBlocking.class, blockingService));
    }

    private <T> T createDialogueProxyWithShortTimeout(Class<T> type) {
        return createDialogueProxy(type, dialogueClientFactory.getChannel(TIMELOCK_SHORT_TIMEOUT));
    }

    private <T> T createDialogueProxyWithLongTimeout(Class<T> type) {
        return createDialogueProxy(type, dialogueClientFactory.getChannel(TIMELOCK_LONG_TIMEOUT));
    }

    private <T> T createDialogueProxy(Class<T> type, Channel channel) {
        return AtlasDbHttpClients.createDialogueProxy(taggedMetricRegistry, type, channel);
    }

    private static <T> T wrapInProxy(Class<T> type, T service) {
        return RetryOnSocketTimeoutExceptionProxy.newProxyInstance(
                type, () -> FastFailoverProxy.newProxyInstance(type, () -> service));
    }

    private static ImmutableMap<String, RemoteServiceConfiguration> getServiceConfigurations(
            UserAgent versionedAgent, ServerListConfig serverListConfig) {
        return ImmutableMap.of(
                TIMELOCK_SHORT_TIMEOUT,
                createRemoteServiceConfiguration(versionedAgent, serverListConfig, false),
                TIMELOCK_LONG_TIMEOUT,
                createRemoteServiceConfiguration(versionedAgent, serverListConfig, true));
    }

    private static RemoteServiceConfiguration createRemoteServiceConfiguration(
            UserAgent userAgent, ServerListConfig serverListConfig, boolean shouldUseExtendedTimeout) {
        return ImmutableRemoteServiceConfiguration.builder()
                .remotingParameters(getFailoverRemotingParameters(shouldUseExtendedTimeout, userAgent))
                .serverList(serverListConfig)
                .build();
    }

    private static AuxiliaryRemotingParameters getFailoverRemotingParameters(
            boolean shouldUseExtendedTimeout, UserAgent userAgent) {
        return AuxiliaryRemotingParameters.builder()
                .shouldLimitPayload(true)
                .shouldRetry(true)
                .shouldUseExtendedTimeout(shouldUseExtendedTimeout)
                .userAgent(userAgent)
                .build();
    }

    private static DialogueClients.ReloadingFactory decorateForFailoverServices(
            DialogueClients.ReloadingFactory baseFactory,
            Refreshable<Map<String, RemoteServiceConfiguration>> serviceToRemoteConfiguration) {
        return baseFactory
                .reloading(serviceToRemoteConfiguration.map(DialogueClientOptions::toServicesConfigBlock))
                .withNodeSelectionStrategy(NodeSelectionStrategy.PIN_UNTIL_ERROR_WITHOUT_RESHUFFLE)
                .withClientQoS(ClientConfiguration.ClientQoS.DANGEROUS_DISABLE_SYMPATHETIC_CLIENT_QOS);
    }
}
