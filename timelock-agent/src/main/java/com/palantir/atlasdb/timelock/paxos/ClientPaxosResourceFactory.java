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

import java.io.Closeable;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.immutables.value.Value;

import com.google.common.base.Suppliers;
import com.palantir.atlasdb.http.AtlasDbHttpClients;
import com.palantir.atlasdb.util.MetricsManager;
import com.palantir.common.proxy.PredicateSwitchedProxy;
import com.palantir.conjure.java.config.ssl.SslSocketFactories;
import com.palantir.conjure.java.config.ssl.TrustContext;
import com.palantir.paxos.PaxosAcceptorNetworkClient;
import com.palantir.paxos.PaxosLearnerNetworkClient;
import com.palantir.timelock.config.PaxosRuntimeConfiguration;
import com.palantir.timelock.config.TimeLockInstallConfiguration;
import com.palantir.timelock.paxos.PaxosRemotingUtils;
import com.palantir.timelock.paxos.TimelockPaxosAcceptorRpcClient;
import com.palantir.timelock.paxos.TimelockPaxosLearnerRpcClient;

public final class ClientPaxosResourceFactory {

    private ClientPaxosResourceFactory() { }

    public static ClientResources create(
            TimelockPaxosInstallationContext install,
            MetricsManager metrics,
            PaxosUseCase useCase,
            Supplier<PaxosRuntimeConfiguration> paxosRuntime,
            ExecutorService sharedExecutor) {
        TimelockPaxosMetrics timelockMetrics =
                TimelockPaxosMetrics.of(useCase, metrics.getTaggedRegistry());

        PaxosComponents paxosComponents = new PaxosComponents(
                timelockMetrics,
                useCase.logDirectoryRelativeToDataDirectory(install.dataDirectory()));

        AcceptorCache acceptorCache = timelockMetrics
                .instrument(AcceptorCache.class, new AcceptorCacheImpl(), "acceptor-cache");
        BatchPaxosAcceptor localBatchPaxosAcceptor = timelockMetrics.instrument(
                BatchPaxosAcceptor.class,
                new LocalBatchPaxosAcceptor(paxosComponents, acceptorCache),
                "local-batch-paxos-acceptor");
        BatchPaxosAcceptorResource clientAcceptorResource = new BatchPaxosAcceptorResource(localBatchPaxosAcceptor);

        BatchPaxosLearner localBatchPaxosLearner = timelockMetrics.instrument(
                BatchPaxosLearner.class,
                new LocalBatchPaxosLearner(paxosComponents),
                "local-batch-paxos-learner");
        BatchPaxosLearnerResource clientLearnerResource = new BatchPaxosLearnerResource(localBatchPaxosLearner);

        UseCaseAwareBatchPaxosResource batchPaxosResource =
                new UseCaseAwareBatchPaxosResource(clientAcceptorResource, clientLearnerResource);

        PaxosRemoteClients remoteClients = ImmutablePaxosRemoteClients.of(install, timelockMetrics);

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
                .resource(batchPaxosResource)
                .quorumSize(install.quorumSize())
                .sharedExecutor(sharedExecutor)
                .build();

        return ImmutableClientResources.builder()
                .quorumSize(install.quorumSize())
                .components(paxosComponents)
                .nonBatchedResource(new PaxosResource(paxosComponents))
                .batchedResource(batchPaxosResource)
                .networkClientFactories(factories(paxosRuntime, singleClientFactories, batchClientFactories))
                .build();
    }

    private static NetworkClientFactories factories(
            Supplier<PaxosRuntimeConfiguration> paxosRuntime,
            SingleLeaderNetworkClientFactories singleClientFactories,
            BatchingNetworkClientFactories batchClientFactories) {
        Supplier<Boolean> useBatchPaxos = Suppliers.compose(
                runtime -> runtime.timestampPaxos().useBatchPaxos(), paxosRuntime::get);

        NetworkClientFactories batchNetworkClientFactories = batchClientFactories.factories();
        NetworkClientFactories singleLeaderNetworkClientFactories = singleClientFactories.factories();
        return ImmutableNetworkClientFactories.builder()
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
    }

    @Value.Immutable
    public interface ClientResources {
        int quorumSize();
        PaxosComponents components();
        PaxosResource nonBatchedResource();
        UseCaseAwareBatchPaxosResource batchedResource();
        NetworkClientFactories networkClientFactories();

        @Value.Derived
        default List<Closeable> closeables() {
            return networkClientFactories().closeables();
        }
    }

    @Value.Immutable
    public interface TimelockPaxosInstallationContext {

        @Value.Parameter
        TimeLockInstallConfiguration install();

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
