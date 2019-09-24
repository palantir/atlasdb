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
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.function.Supplier;

import org.immutables.value.Value;

import com.google.common.base.Suppliers;
import com.palantir.atlasdb.util.MetricsManager;
import com.palantir.common.proxy.PredicateSwitchedProxy;
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
            MetricsManager metrics,
            TimeLockInstallConfiguration install,
            Supplier<PaxosRuntimeConfiguration> paxosRuntime,
            ExecutorService sharedExecutor, PaxosUseCase useCase) {

        TimelockPaxosMetrics timelockMetrics =
                TimelockPaxosMetrics.of(useCase, metrics.getTaggedRegistry());

        PaxosComponents paxosComponents = new PaxosComponents(
                timelockMetrics,
                useCase.logDirectoryRelativeToDataDirectory(install.paxos().dataDirectory().toPath()));

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

        int quorumSize = PaxosRemotingUtils.getQuorumSize(PaxosRemotingUtils.getClusterAddresses(install));

        TimelockProxyFactories proxyFactories = ImmutableTimelockProxyFactories.builder()
                .install(install)
                .metrics(timelockMetrics)
                .build();

        PaxosRemoteClients remoteClients = ImmutablePaxosRemoteClients.of(proxyFactories);

        SingleLeaderNetworkClientFactories singleClientFactories = ImmutableSingleLeaderNetworkClientFactories.builder()
                .proxyFactories(proxyFactories)
                .components(paxosComponents)
                .quorumSize(quorumSize)
                .sharedExecutor(sharedExecutor)
                .remoteClients(remoteClients)
                .useCase(useCase)
                .build();

        BatchingNetworkClientFactories batchClientFactories = ImmutableBatchingNetworkClientFactories.builder()
                .proxyFactories(proxyFactories)
                .quorumSize(quorumSize)
                .resource(batchPaxosResource)
                .sharedExecutor(sharedExecutor)
                .remoteClients(remoteClients)
                .useCase(useCase)
                .build();

        return ImmutableClientResources.builder()
                .quorumSize(quorumSize)
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
    public interface PaxosRemoteClients {

        @Value.Parameter
        TimelockProxyFactories factories();

        @Value.Derived
        default List<TimelockPaxosAcceptorRpcClient> nonBatchAcceptor() {
            return factories().createInstrumentedRemoteProxies(
                    TimelockPaxosAcceptorRpcClient.class,
                    "paxos-acceptor-rpc-client");
        }

        @Value.Derived
        default List<TimelockPaxosLearnerRpcClient> nonBatchLearner() {
            return factories().createInstrumentedRemoteProxies(
                    TimelockPaxosLearnerRpcClient.class,
                    "paxos-learner-rpc-client");
        }

        @Value.Derived
        default List<BatchPaxosAcceptorRpcClient> batchAcceptor() {
            return factories().createInstrumentedRemoteProxies(
                    BatchPaxosAcceptorRpcClient.class,
                    "batch-paxos-acceptor-rpc-client");
        }

        @Value.Derived
        default List<BatchPaxosLearnerRpcClient> batchLearner() {
            return factories().createInstrumentedRemoteProxies(
                    BatchPaxosLearnerRpcClient.class,
                    "batch-paxos-learner-rpc-client");
        }
    }

}
