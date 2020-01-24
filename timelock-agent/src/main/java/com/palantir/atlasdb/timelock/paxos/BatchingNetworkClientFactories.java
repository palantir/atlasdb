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

import org.immutables.value.Value;

import com.google.common.collect.ImmutableList;
import com.palantir.paxos.PaxosAcceptorNetworkClient;
import com.palantir.paxos.PaxosLearnerNetworkClient;

@Value.Immutable
abstract class BatchingNetworkClientFactories implements
        NetworkClientFactories, Dependencies.NetworkClientFactories {

    @Value.Auxiliary
    @Value.Derived
    AutobatchingPaxosAcceptorNetworkClientFactory acceptorNetworkClientFactory() {
        BatchPaxosAcceptor local = components().batchAcceptor();
        List<BatchPaxosAcceptor> remotes =
                UseCaseAwareBatchPaxosAcceptorAdapter.wrap(useCase(), remoteClients().batchAcceptor());
        List<BatchPaxosAcceptor> allBatchAcceptors = LocalAndRemotes.of(local, remotes)
                .enhanceRemotes(remote -> metrics().instrument(BatchPaxosAcceptor.class, remote))
                .all();

        return AutobatchingPaxosAcceptorNetworkClientFactory.create(allBatchAcceptors, sharedExecutor(), quorumSize());
    }

    @Value.Auxiliary
    @Value.Derived
    AutobatchingPaxosLearnerNetworkClientFactory learnerNetworkClientFactory() {
        BatchPaxosLearner local = components().batchLearner();
        List<BatchPaxosLearner> remotes =
                UseCaseAwareBatchPaxosLearnerAdapter.wrap(useCase(), remoteClients().batchLearner());

        LocalAndRemotes<BatchPaxosLearner> allBatchLearners = LocalAndRemotes.of(local, remotes)
                .enhanceRemotes(remote -> metrics().instrument(BatchPaxosLearner.class, remote));

        return AutobatchingPaxosLearnerNetworkClientFactory.create(allBatchLearners, sharedExecutor(), quorumSize());
    }

    @Value.Auxiliary
    @Value.Derived
    @Override
    public List<Closeable> closeables() {
        return ImmutableList.of(acceptorNetworkClientFactory(), learnerNetworkClientFactory());
    }

    @Override
    public Factory<PaxosAcceptorNetworkClient> acceptor() {
        return acceptorNetworkClientFactory()::paxosAcceptorForClient;
    }

    @Override
    public Factory<PaxosLearnerNetworkClient> learner() {
        return learnerNetworkClientFactory()::paxosLearnerForClient;
    }

    public abstract static class Builder implements NetworkClientFactories.Builder {}

}
