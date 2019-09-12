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

import org.immutables.value.Value;

import com.palantir.atlasdb.util.MetricsManager;
import com.palantir.timelock.config.TimeLockInstallConfiguration;
import com.palantir.timelock.paxos.PaxosRemotingUtils;

public final class ClientPaxosResourceFactory {

    private ClientPaxosResourceFactory() { }

    public static ClientResources create(
            MetricsManager metrics,
            Path logDirectory,
            TimeLockInstallConfiguration install,
            ExecutorService sharedExecutor) {
        PaxosComponents paxosComponents = new PaxosComponents(metrics.getTaggedRegistry(), "bound-store", logDirectory);
        PaxosResource paxosResource = new PaxosResource(paxosComponents);
        BatchPaxosAcceptorResource clientAcceptorResource =
                new BatchPaxosAcceptorResource(new LocalBatchPaxosAcceptor(paxosComponents, new AcceptorCacheImpl()));
        BatchPaxosLearnerResource clientLearnerResource = new BatchPaxosLearnerResource(paxosComponents);
        UseCaseAwareBatchPaxosResource batchPaxosResource =
                new UseCaseAwareBatchPaxosResource(clientAcceptorResource, clientLearnerResource);

        int quorumSize = PaxosRemotingUtils.getQuorumSize(PaxosRemotingUtils.getClusterAddresses(install));

        TimelockProxyFactories proxyFactories = ImmutableTimelockProxyFactories.builder()
                .install(install)
                .metrics(metrics.getRegistry())
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
                .nonBatchedResource(paxosResource)
                .batchedResource(batchPaxosResource)
                .networkClientFactories(networkClientFactories(install, singleClientFactories, batchClientFactories))
                .build();
    }

    private static NetworkClientFactories networkClientFactories(
            TimeLockInstallConfiguration install,
            SingleLeaderNetworkClientFactories singleClientFactories,
            BatchingNetworkClientFactories batchClientFactories) {
        if (install.paxos().clientPaxos().useBatchPaxos()) {
            return batchClientFactories.factories();
        } else {
            return singleClientFactories.factories();
        }
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
