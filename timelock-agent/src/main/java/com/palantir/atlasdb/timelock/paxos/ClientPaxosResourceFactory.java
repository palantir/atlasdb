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
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.immutables.value.Value;

import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.palantir.atlasdb.http.AtlasDbHttpClients;
import com.palantir.atlasdb.timelock.paxos.NetworkClientFactories.Factory;
import com.palantir.atlasdb.util.MetricsManager;
import com.palantir.common.proxy.PredicateSwitchedProxy;
import com.palantir.conjure.java.config.ssl.SslSocketFactories;
import com.palantir.conjure.java.config.ssl.TrustContext;
import com.palantir.paxos.PaxosAcceptorNetworkClient;
import com.palantir.paxos.PaxosLearnerNetworkClient;
import com.palantir.paxos.PaxosProposer;
import com.palantir.paxos.PaxosProposerImpl;
import com.palantir.timelock.config.PaxosRuntimeConfiguration;
import com.palantir.timelock.config.TimeLockInstallConfiguration;
import com.palantir.timelock.paxos.PaxosRemotingUtils;
import com.palantir.timelock.paxos.TimelockPaxosAcceptorRpcClient;
import com.palantir.timelock.paxos.TimelockPaxosLearnerRpcClient;

public final class ClientPaxosResourceFactory {

    private ClientPaxosResourceFactory() { }

    public static Resources create(
            TimelockPaxosInstallationContext install,
            MetricsManager metrics,
            Supplier<PaxosRuntimeConfiguration> paxosRuntime,
            ExecutorService sharedExecutor) {
        Supplier<Boolean> useBatchPaxosForTimestamps = Suppliers.compose(
                runtime -> runtime.timestampPaxos().useBatchPaxos(), paxosRuntime::get);
        PaxosUseCaseContext timestampContext = useCaseSpecificContext(
                install,
                metrics,
                PaxosUseCase.TIMESTAMP,
                sharedExecutor,
                useBatchPaxosForTimestamps);

        return ImmutableResources.builder()
                .timestamp(timestampContext)
                .addResources(new PaxosResource(timestampContext.components()))
                .build();
    }

    private static PaxosUseCaseContext useCaseSpecificContext(
            TimelockPaxosInstallationContext install,
            MetricsManager metrics,
            PaxosUseCase useCase,
            ExecutorService sharedExecutor,
            Supplier<Boolean> useBatchPaxos) {
        TimelockPaxosMetrics timelockMetrics = TimelockPaxosMetrics.of(useCase, metrics.getTaggedRegistry());
        PaxosRemoteClients remoteClients = ImmutablePaxosRemoteClients.of(install, timelockMetrics);

        PaxosComponents paxosComponents = new PaxosComponents(
                timelockMetrics,
                useCase.logDirectoryRelativeToDataDirectory(install.dataDirectory()));

        SingleLeaderNetworkClientFactories singleClientFactories = ImmutableSingleLeaderNetworkClientFactories.builder()
                .useCase(useCase)
                .metrics(timelockMetrics)
                .remoteClients(remoteClients)
                .components(paxosComponents)
                .quorumSize(install.quorumSize())
                .sharedExecutor(sharedExecutor)
                .build();

        BatchingNetworkClientFactories batchClientFactories = ImmutableBatchingNetworkClientFactories.builder()
                .useCase(useCase)
                .metrics(timelockMetrics)
                .remoteClients(remoteClients)
                .components(paxosComponents)
                .quorumSize(install.quorumSize())
                .sharedExecutor(sharedExecutor)
                .build();

        NetworkClientFactories batchNetworkClientFactories = batchClientFactories.factories();
        NetworkClientFactories singleLeaderNetworkClientFactories = singleClientFactories.factories();
        NetworkClientFactories combinedNetworkClientFactories = ImmutableNetworkClientFactories.builder()
                .acceptor(client -> PredicateSwitchedProxy.newProxyInstance(
                        batchNetworkClientFactories.acceptor().create(client),
                        singleLeaderNetworkClientFactories.acceptor().create(client),
                        useBatchPaxos,
                        PaxosAcceptorNetworkClient.class))
                .learner(client -> PredicateSwitchedProxy.newProxyInstance(
                        batchNetworkClientFactories.learner().create(client),
                        singleLeaderNetworkClientFactories.learner().create(client),
                        useBatchPaxos,
                        PaxosLearnerNetworkClient.class))
                .addAllCloseables(batchNetworkClientFactories.closeables())
                .addAllCloseables(singleLeaderNetworkClientFactories.closeables())
                .build();

        Factory<PaxosProposer> proposerFactory = client -> {
            PaxosAcceptorNetworkClient acceptorNetworkClient = combinedNetworkClientFactories.acceptor().create(client);
            PaxosLearnerNetworkClient learnerNetworkClient = combinedNetworkClientFactories.learner().create(client);

            PaxosProposer paxosProposer = PaxosProposerImpl.newProposer(
                    acceptorNetworkClient,
                    learnerNetworkClient,
                    install.quorumSize(),
                    install.nodeUuid());

            return timelockMetrics.instrument(PaxosProposer.class, paxosProposer, "paxos-proposer", client);
        };

        return ImmutablePaxosUseCaseContext.builder()
                .metrics(timelockMetrics)
                .components(paxosComponents)
                .networkClientFactories(combinedNetworkClientFactories)
                .proposerFactory(proposerFactory)
                .build();
    }


    @Value.Immutable
    public interface PaxosUseCaseContext {
        TimelockPaxosMetrics metrics();
        PaxosComponents components();
        NetworkClientFactories networkClientFactories();
        Factory<PaxosProposer> proposerFactory();
    }

    @Value.Immutable
    public abstract static class Resources {
        public abstract PaxosUseCaseContext timestamp();
        // TODO(fdesouza): make non optional when ready to implement
        public abstract Optional<PaxosUseCaseContext> leadership();
        public abstract List<Object> resources();
        public abstract TimelockPaxosInstallationContext installContext();

        @Value.Derived
        public List<Object> allResources() {
            Map<PaxosUseCase, BatchPaxosResources> resources = ImmutableMap.<PaxosUseCase, BatchPaxosResources>builder()
                    .put(PaxosUseCase.TIMESTAMP, batchResourcesForUseCase(timestamp()))
                    .build();
            UseCaseAwareBatchPaxosResource combinedBatchResource =
                    new UseCaseAwareBatchPaxosResource(new EnumMap<>(resources));

            return ImmutableList.builder()
                    .addAll(resources())
                    .add(combinedBatchResource)
                    .build();
        }

        private static BatchPaxosResources batchResourcesForUseCase(PaxosUseCaseContext useCaseContext) {
            PaxosComponents components = useCaseContext.components();
            BatchPaxosAcceptorResource acceptorResource = new BatchPaxosAcceptorResource(components.batchAcceptor());
            BatchPaxosLearnerResource learnerResource = new BatchPaxosLearnerResource(components.batchLearner());
            return ImmutableBatchPaxosResources.of(acceptorResource, learnerResource);
        }
    }

    @Value.Immutable
    public interface TimelockPaxosInstallationContext {

        @Value.Parameter
        TimeLockInstallConfiguration install();

        @Value.Derived
        default UUID nodeUuid() {
            return UUID.randomUUID();
        }

        @Value.Derived
        default int quorumSize() {
            return PaxosRemotingUtils.getQuorumSize(clusterAddress());
        }

        @Value.Derived
        default Set<String> clusterAddress() {
            return PaxosRemotingUtils.getClusterAddresses(install());
        }

        @Value.Derived
        default Set<String> remoteUris() {
            return PaxosRemotingUtils.getRemoteServerPaths(install());
        }

        @Value.Derived
        default Path dataDirectory() {
            return install().paxos().dataDirectory().toPath();
        }

        @Value.Derived
        default Optional<TrustContext> trustContext() {
            return PaxosRemotingUtils
                    .getSslConfigurationOptional(install())
                    .map(SslSocketFactories::createTrustContext);
        }

    }

    @Value.Immutable
    public abstract static class PaxosRemoteClients {

        @Value.Parameter
        public abstract TimelockPaxosInstallationContext context();

        @Value.Parameter
        public abstract TimelockPaxosMetrics metrics();

        @Value.Derived
        public List<TimelockPaxosAcceptorRpcClient> nonBatchAcceptor() {
            return createInstrumentedRemoteProxies(TimelockPaxosAcceptorRpcClient.class, "paxos-acceptor-rpc-client");
        }

        @Value.Derived
        public List<TimelockPaxosLearnerRpcClient> nonBatchLearner() {
            return createInstrumentedRemoteProxies(TimelockPaxosLearnerRpcClient.class, "paxos-learner-rpc-client");
        }

        @Value.Derived
        public List<BatchPaxosAcceptorRpcClient> batchAcceptor() {
            return createInstrumentedRemoteProxies(
                    BatchPaxosAcceptorRpcClient.class,
                    "batch-paxos-acceptor-rpc-client");
        }

        @Value.Derived
        public List<BatchPaxosLearnerRpcClient> batchLearner() {
            return createInstrumentedRemoteProxies(
                    BatchPaxosLearnerRpcClient.class,
                    "batch-paxos-learner-rpc-client");
        }

        private <T> List<T> createInstrumentedRemoteProxies(Class<T> clazz, String name) {
            return context().remoteUris().stream()
                    .map(uri -> AtlasDbHttpClients.DEFAULT_TARGET_FACTORY.createProxy(
                            context().trustContext(),
                            uri,
                            clazz,
                            name,
                            false))
                    .map(proxy -> metrics().instrument(clazz, proxy, name))
                    .collect(Collectors.toList());
        }
    }

}
