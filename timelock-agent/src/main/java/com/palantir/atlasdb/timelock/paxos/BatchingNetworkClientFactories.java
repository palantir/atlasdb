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

    abstract PaxosUseCase useCase();
    abstract TimelockPaxosMetrics metrics();
    abstract ClientPaxosResourceFactory.PaxosRemoteClients remoteClients();
    abstract BatchPaxosResources resource();
    abstract int quorumSize();
    abstract ExecutorService sharedExecutor();

    public NetworkClientFactories factories() {
        List<BatchPaxosAcceptor> allBatchAcceptors = metrics()
                .instrumentLocalAndRemotesFor(
                        BatchPaxosAcceptor.class,
                        resource().batchAcceptor().asLocalBatchPaxosAcceptor(),
                        UseCaseAwareBatchPaxosAcceptorAdapter.wrap(useCase(), remoteClients().batchAcceptor()),
                        "batch-paxos-acceptor")
                .all();

        AutobatchingPaxosAcceptorNetworkClientFactory acceptorFactory =
                AutobatchingPaxosAcceptorNetworkClientFactory.create(allBatchAcceptors, sharedExecutor(), quorumSize());

        List<BatchPaxosLearner> allBatchLearners = metrics()
                .instrumentLocalAndRemotesFor(
                        BatchPaxosLearner.class,
                        resource().batchLearner().asLocalBatchPaxosLearner(),
                        UseCaseAwareBatchPaxosLearnerAdapter.wrap(useCase(), remoteClients().batchLearner()),
                        "batch-paxos-learner")
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
