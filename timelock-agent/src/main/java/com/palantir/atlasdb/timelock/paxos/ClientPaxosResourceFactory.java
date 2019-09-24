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
import java.util.concurrent.ExecutorService;
import java.util.function.Supplier;

import org.immutables.value.Value;

import com.google.common.base.Suppliers;
import com.palantir.atlasdb.factory.DynamicDecoratingProxy;
import com.palantir.atlasdb.util.MetricsManager;
import com.palantir.paxos.PaxosAcceptorNetworkClient;
import com.palantir.paxos.PaxosLearnerNetworkClient;
import com.palantir.timelock.config.PaxosRuntimeConfiguration;
import com.palantir.timelock.config.TimeLockInstallConfiguration;
import com.palantir.timelock.paxos.PaxosRemotingUtils;

public final class ClientPaxosResourceFactory {

    private ClientPaxosResourceFactory() { }

    public static ClientResources create(
            MetricsManager metrics,
            Path logDirectory,
            TimeLockInstallConfiguration install,
            Supplier<PaxosRuntimeConfiguration> paxosRuntime,
            ExecutorService sharedExecutor) {
        TimelockPaxosMetrics timelockMetrics =
                TimelockPaxosMetrics.of(PaxosUseCase.TIMESTAMP, metrics.getTaggedRegistry());
        PaxosComponents paxosComponents = new PaxosComponents(timelockMetrics, logDirectory);

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

        SingleLeaderNetworkClientFactories singleClientFactories = ImmutableSingleLeaderNetworkClientFactories.builder()
                .proxyFactories(proxyFactories)
                .components(paxosComponents)
                .quorumSize(quorumSize)
                .sharedExecutor(sharedExecutor)
                .build();

        BatchingNetworkClientFactories batchClientFactories = ImmutableBatchingNetworkClientFactories.builder()
                .proxyFactories(proxyFactories)
                .quorumSize(quorumSize)
                .resource(batchPaxosResource)
                .sharedExecutor(sharedExecutor)
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
                .acceptor(client -> DynamicDecoratingProxy.newProxyInstance(
                        batchNetworkClientFactories.acceptor().create(client),
                        singleLeaderNetworkClientFactories.acceptor().create(client),
                        useBatchPaxos,
                        PaxosAcceptorNetworkClient.class))
                .learner(client -> DynamicDecoratingProxy.newProxyInstance(
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

}
