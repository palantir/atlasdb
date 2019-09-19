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

import java.util.List;
import java.util.concurrent.ExecutorService;
import org.immutables.value.Value;

@Value.Immutable
abstract class BatchingNetworkClientFactories {

    abstract TimelockProxyFactories proxyFactories();
    abstract UseCaseAwareBatchPaxosResource resource();
    abstract ExecutorService sharedExecutor();
    abstract int quorumSize();

    public NetworkClientFactories factories() {
        // TODO(fdesouza): pass these in, as they become use case aware
        List<BatchPaxosAcceptorRpcClient> remoteBatchAcceptors = proxyFactories()
                .createRemoteProxies(BatchPaxosAcceptorRpcClient.class, "timestamp-bound-store.batch-acceptor");

        List<BatchPaxosAcceptor> allBatchAcceptors = proxyFactories().instrumentLocalAndRemotesFor(
                BatchPaxosAcceptor.class,
                resource().acceptor(PaxosUseCase.TIMESTAMP).asLocalBatchPaxosAcceptor(),
                UseCaseAwareBatchPaxosAcceptorAdapter.wrap(PaxosUseCase.TIMESTAMP, remoteBatchAcceptors)).all();

        AutobatchingPaxosAcceptorNetworkClientFactory acceptorFactory =
                AutobatchingPaxosAcceptorNetworkClientFactory.create(allBatchAcceptors, sharedExecutor(), quorumSize());

        // TODO(fdesouza): pass these in as they become use case aware
        List<BatchPaxosLearnerRpcClient> remoteBatchLearners = proxyFactories()
                .createRemoteProxies(BatchPaxosLearnerRpcClient.class, "timestamp-bound-store.batch-learner");

        List<BatchPaxosLearner> allBatchLearners = proxyFactories()
                .instrumentLocalAndRemotesFor(
                        BatchPaxosLearner.class,
                        resource().learner(PaxosUseCase.TIMESTAMP),
                        UseCaseAwareBatchPaxosLearnerAdapter.wrap(PaxosUseCase.TIMESTAMP, remoteBatchLearners))
                .all();

        AutobatchingPaxosLearnerNetworkClientFactory learnerFactory =
                AutobatchingPaxosLearnerNetworkClientFactory.create(allBatchLearners, sharedExecutor(), quorumSize());

        return ImmutableNetworkClientFactories.builder()
                .acceptor(acceptorFactory::paxosAcceptorForClient)
                .learner(learnerFactory::paxosLearnerForClient)
                .addCloseables(acceptorFactory, learnerFactory)
                .build();
    }
}
