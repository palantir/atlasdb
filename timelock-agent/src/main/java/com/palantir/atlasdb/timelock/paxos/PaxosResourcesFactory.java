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

package com.palantir.atlasdb.timelock.paxos;

import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.immutables.value.Value;

import com.google.common.base.Suppliers;
import com.palantir.atlasdb.timelock.paxos.NetworkClientFactories.Factory;
import com.palantir.atlasdb.util.MetricsManager;
import com.palantir.common.proxy.PredicateSwitchedProxy;
import com.palantir.conjure.java.api.config.service.UserAgent;
import com.palantir.conjure.java.config.ssl.SslSocketFactories;
import com.palantir.conjure.java.config.ssl.TrustContext;
import com.palantir.leader.PingableLeader;
import com.palantir.paxos.CoalescingPaxosLatestRoundVerifier;
import com.palantir.paxos.PaxosAcceptorNetworkClient;
import com.palantir.paxos.PaxosLatestRoundVerifierImpl;
import com.palantir.paxos.PaxosLearnerNetworkClient;
import com.palantir.paxos.PaxosProposer;
import com.palantir.paxos.PaxosProposerImpl;
import com.palantir.timelock.config.PaxosInstallConfiguration.PaxosLeaderMode;
import com.palantir.timelock.config.PaxosRuntimeConfiguration;
import com.palantir.timelock.config.TimeLockInstallConfiguration;
import com.palantir.timelock.paxos.PaxosRemotingUtils;
import com.palantir.timestamp.ManagedTimestampService;
import com.palantir.timestamp.PersistentTimestampServiceImpl;
import com.palantir.timestamp.TimestampBoundStore;

public final class PaxosResourcesFactory {

    private PaxosResourcesFactory() { }

    public static PaxosResources create(
            TimelockPaxosInstallationContext install,
            MetricsManager metrics,
            Supplier<PaxosRuntimeConfiguration> paxosRuntime,
            ExecutorService sharedExecutor) {
        PaxosRemoteClients remoteClients = ImmutablePaxosRemoteClients.of(install, metrics);

        ImmutablePaxosResources.Builder resourcesBuilder =
                setupTimestampResources(install, metrics, paxosRuntime, sharedExecutor, remoteClients);

        if (install.useLeaderForEachClient()) {
            return configureLeaderForEachClient(
                    resourcesBuilder,
                    install,
                    metrics,
                    paxosRuntime,
                    sharedExecutor,
                    remoteClients);
        } else {
            return configureLeaderForAllClients(
                    resourcesBuilder,
                    install,
                    metrics,
                    paxosRuntime,
                    sharedExecutor,
                    remoteClients);
        }
    }

    private static PaxosResources configureLeaderForEachClient(
            ImmutablePaxosResources.Builder resourcesBuilder,
            TimelockPaxosInstallationContext install,
            MetricsManager metrics,
            Supplier<PaxosRuntimeConfiguration> paxosRuntime,
            ExecutorService sharedExecutor,
            PaxosRemoteClients remoteClients) {
        TimelockPaxosMetrics timelockMetrics =
                TimelockPaxosMetrics.of(PaxosUseCase.LEADER_FOR_EACH_CLIENT, metrics);

        Factories.LeaderPingHealthCheckFactory healthCheckPingersFactory = dependencies -> {
            BatchPingableLeader local = dependencies.components().batchPingableLeader();
            List<BatchPingableLeader> remotes = dependencies.remoteClients().batchPingableLeaders();
            return Stream.concat(Stream.of(local), remotes.stream())
                    .map(MultiLeaderHealthCheckPinger::new)
                    .collect(Collectors.toList());
        };

        // we do *not* use CoalescingPaxosLatestRoundVerifier because any coalescing will happen in the
        // AutobatchingPaxosAcceptorNetworkClient. This is for us to avoid context switching as much as possible on the
        // hot path since batching twice doesn't necessarily give us anything.
        Factories.PaxosLatestRoundVerifierFactory latestRoundVerifierFactory = acceptorClient ->
                new PaxosLatestRoundVerifierImpl(acceptorClient);

        LeadershipContextFactory factory = ImmutableLeadershipContextFactory.builder()
                .install(install)
                .sharedExecutor(sharedExecutor)
                .remoteClients(remoteClients)
                .runtime(paxosRuntime)
                .useCase(PaxosUseCase.LEADER_FOR_EACH_CLIENT)
                .metrics(timelockMetrics)
                .networkClientFactoryBuilder(ImmutableBatchingNetworkClientFactories.builder())
                .leaderPingerFactoryBuilder(ImmutableBatchingLeaderPingerFactory.builder())
                .healthCheckPingersFactory(healthCheckPingersFactory)
                .latestRoundVerifierFactory(latestRoundVerifierFactory)
                .build();

        return resourcesBuilder
                .leadershipContextFactory(factory)
                .putLeadershipBatchComponents(PaxosUseCase.LEADER_FOR_EACH_CLIENT, factory.components())
                .addAdhocResources(new BatchPingableLeaderResource(install.nodeUuid(), factory.components()))
                .build();
    }

    private static PaxosResources configureLeaderForAllClients(
            ImmutablePaxosResources.Builder resourcesBuilder,
            TimelockPaxosInstallationContext install,
            MetricsManager metrics,
            Supplier<PaxosRuntimeConfiguration> paxosRuntime,
            ExecutorService sharedExecutor,
            PaxosRemoteClients remoteClients) {
        TimelockPaxosMetrics timelockMetrics =
                TimelockPaxosMetrics.of(PaxosUseCase.LEADER_FOR_ALL_CLIENTS, metrics);

        Factories.LeaderPingHealthCheckFactory healthCheckPingersFactory = dependencies -> {
            PingableLeader local = dependencies.components().pingableLeader(PaxosUseCase.PSEUDO_LEADERSHIP_CLIENT);
            List<PingableLeader> remotes = dependencies.remoteClients().nonBatchPingableLeaders();
            return Stream.concat(Stream.of(local), remotes.stream())
                    .map(SingleLeaderHealthCheckPinger::new)
                    .collect(Collectors.toList());
        };

        Factories.PaxosLatestRoundVerifierFactory latestRoundVerifierFactory = acceptorClient ->
                new CoalescingPaxosLatestRoundVerifier(new PaxosLatestRoundVerifierImpl(acceptorClient));

        LeadershipContextFactory factory = ImmutableLeadershipContextFactory.builder()
                .install(install)
                .sharedExecutor(sharedExecutor)
                .remoteClients(remoteClients)
                .runtime(paxosRuntime)
                .useCase(PaxosUseCase.LEADER_FOR_ALL_CLIENTS)
                .metrics(timelockMetrics)
                .networkClientFactoryBuilder(ImmutableSingleLeaderNetworkClientFactories.builder()
                        .useBatchedEndpoints(() -> paxosRuntime.get().enableBatchingForSingleLeader()))
                .leaderPingerFactoryBuilder(ImmutableSingleLeaderPingerFactory.builder())
                .healthCheckPingersFactory(healthCheckPingersFactory)
                .latestRoundVerifierFactory(latestRoundVerifierFactory)
                .build();

        return resourcesBuilder
                .leadershipContextFactory(factory)
                .putLeadershipBatchComponents(PaxosUseCase.LEADER_FOR_ALL_CLIENTS, factory.components())
                .addAdhocResources(new BatchPingableLeaderResource(install.nodeUuid(), factory.components()))
                .addAdhocResources(
                        new LeaderAcceptorResource(
                                factory.components().acceptor(PaxosUseCase.PSEUDO_LEADERSHIP_CLIENT)),
                        new LeaderLearnerResource(factory.components().learner(PaxosUseCase.PSEUDO_LEADERSHIP_CLIENT)),
                        factory.components().pingableLeader(PaxosUseCase.PSEUDO_LEADERSHIP_CLIENT))
                .build();
    }

    private static ImmutablePaxosResources.Builder setupTimestampResources(
            TimelockPaxosInstallationContext install,
            MetricsManager metrics,
            Supplier<PaxosRuntimeConfiguration> paxosRuntime,
            ExecutorService sharedExecutor,
            PaxosRemoteClients remoteClients) {
        TimelockPaxosMetrics timelockMetrics =
                TimelockPaxosMetrics.of(PaxosUseCase.TIMESTAMP, metrics);

        LocalPaxosComponents paxosComponents = new LocalPaxosComponents(
                timelockMetrics,
                PaxosUseCase.TIMESTAMP,
                install.dataDirectory(),
                install.sqliteDataDirectory(),
                install.nodeUuid(),
                install.install().paxos().canCreateNewClients());

        NetworkClientFactories batchClientFactories = ImmutableBatchingNetworkClientFactories.builder()
                .useCase(PaxosUseCase.TIMESTAMP)
                .metrics(timelockMetrics)
                .remoteClients(remoteClients)
                .components(paxosComponents)
                .quorumSize(install.quorumSize())
                .sharedExecutor(sharedExecutor)
                .build();

        NetworkClientFactories singleLeaderClientFactories = ImmutableSingleLeaderNetworkClientFactories.builder()
                .useCase(PaxosUseCase.TIMESTAMP)
                .metrics(timelockMetrics)
                .remoteClients(remoteClients)
                .components(paxosComponents)
                .quorumSize(install.quorumSize())
                .sharedExecutor(sharedExecutor)
                .build();

        Supplier<Boolean> useBatchPaxosForTimestamps = Suppliers.compose(
                runtime -> runtime.timestampPaxos().useBatchPaxos(), paxosRuntime::get);

        NetworkClientFactories combinedNetworkClientFactories = ImmutableNetworkClientFactories.builder()
                .acceptor(client -> PredicateSwitchedProxy.newProxyInstance(
                        batchClientFactories.acceptor().create(client),
                        singleLeaderClientFactories.acceptor().create(client),
                        useBatchPaxosForTimestamps,
                        PaxosAcceptorNetworkClient.class))
                .learner(client -> PredicateSwitchedProxy.newProxyInstance(
                        batchClientFactories.learner().create(client),
                        singleLeaderClientFactories.learner().create(client),
                        useBatchPaxosForTimestamps,
                        PaxosLearnerNetworkClient.class))
                .addAllCloseables(batchClientFactories.closeables())
                .addAllCloseables(singleLeaderClientFactories.closeables())
                .build();

        Factory<PaxosProposer> proposerFactory = client -> {
            PaxosAcceptorNetworkClient acceptorNetworkClient = combinedNetworkClientFactories.acceptor().create(client);
            PaxosLearnerNetworkClient learnerNetworkClient = combinedNetworkClientFactories.learner().create(client);

            PaxosProposer paxosProposer = PaxosProposerImpl.newProposer(
                    acceptorNetworkClient,
                    learnerNetworkClient,
                    install.nodeUuid());

            return timelockMetrics.instrument(PaxosProposer.class, paxosProposer, client);
        };

        Factory<ManagedTimestampService> timestampFactory = client -> {
            // TODO (jkong): live reload ping
            TimestampBoundStore boundStore = timelockMetrics.instrument(
                    TimestampBoundStore.class,
                    new PaxosTimestampBoundStore(
                            proposerFactory.create(client),
                            paxosComponents.learner(client),
                            combinedNetworkClientFactories.acceptor().create(client),
                            combinedNetworkClientFactories.learner().create(client),
                            paxosRuntime.get().maximumWaitBeforeProposalMs()),
                    client);
            return PersistentTimestampServiceImpl.create(boundStore);
        };

        return ImmutablePaxosResources.builder()
                .addAdhocResources(new TimestampPaxosResource(paxosComponents))
                .timestampPaxosComponents(paxosComponents)
                .timestampServiceFactory(timestampFactory);
    }

    @Value.Immutable
    public interface TimelockPaxosInstallationContext {

        @Value.Parameter
        TimeLockInstallConfiguration install();

        @Value.Parameter
        UserAgent userAgent();

        @Value.Derived
        default UUID nodeUuid() {
            return UUID.randomUUID();
        }

        @Value.Derived
        default int quorumSize() {
            return PaxosRemotingUtils.getQuorumSize(clusterAddresses());
        }

        @Value.Derived
        default List<String> clusterAddresses() {
            return PaxosRemotingUtils.getClusterAddresses(install());
        }

        @Value.Derived
        default List<String> remoteUris() {
            return PaxosRemotingUtils.getRemoteServerPaths(install());
        }

        @Value.Derived
        default Path dataDirectory() {
            return install().paxos().dataDirectory().toPath();
        }

        @Value.Derived
        default Path sqliteDataDirectory() {
            return install().paxos().sqlitePersistence().dataDirectory().toPath();
        }

        @Value.Derived
        default Optional<TrustContext> trustContext() {
            return PaxosRemotingUtils
                    .getSslConfigurationOptional(install())
                    .map(SslSocketFactories::createTrustContext);
        }

        @Value.Derived
        default boolean useLeaderForEachClient() {
            return install().paxos().leaderMode() == PaxosLeaderMode.LEADER_PER_CLIENT;
        }

    }

}
