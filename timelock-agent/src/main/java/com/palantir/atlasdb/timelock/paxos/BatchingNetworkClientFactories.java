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

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.MoreExecutors;
import com.palantir.common.streams.KeyedStream;
import com.palantir.paxos.PaxosAcceptorNetworkClient;
import com.palantir.paxos.PaxosLearnerNetworkClient;
import java.io.Closeable;
import java.util.List;
import java.util.stream.Collectors;
import org.immutables.value.Value;

@SuppressWarnings("immutables:subtype")
@Value.Immutable
abstract class BatchingNetworkClientFactories implements NetworkClientFactories, Dependencies.NetworkClientFactories {

    @Value.Auxiliary
    @Value.Derived
    AutobatchingPaxosAcceptorNetworkClientFactory acceptorNetworkClientFactory() {
        BatchPaxosAcceptor local = components().batchAcceptor();
        List<WithDedicatedExecutor<BatchPaxosAcceptor>> remotesAndExecutors =
                UseCaseAwareBatchPaxosAcceptorAdapter.wrap(
                        useCase(), remoteClients().batchAcceptorsWithExecutors());
        LocalAndRemotes<WithDedicatedExecutor<BatchPaxosAcceptor>> batchAcceptors = LocalAndRemotes.of(
                        WithDedicatedExecutor.of(local, MoreExecutors.newDirectExecutorService()), remotesAndExecutors)
                .enhanceRemotes(remote ->
                        remote.transformService(service -> metrics().instrument(BatchPaxosAcceptor.class, service)));

        return AutobatchingPaxosAcceptorNetworkClientFactory.create(
                batchAcceptors.all().stream()
                        .map(WithDedicatedExecutor::service)
                        .collect(Collectors.toList()),
                KeyedStream.of(batchAcceptors.all())
                        .mapKeys(WithDedicatedExecutor::service)
                        .map(WithDedicatedExecutor::executor)
                        .collectToMap(),
                quorumSize());
    }

    @Value.Auxiliary
    @Value.Derived
    AutobatchingPaxosLearnerNetworkClientFactory learnerNetworkClientFactory() {
        BatchPaxosLearner local = components().batchLearner();
        List<WithDedicatedExecutor<BatchPaxosLearnerRpcClient>> batchLearners =
                remoteClients().batchLearner();
        List<WithDedicatedExecutor<BatchPaxosLearner>> remoteLearners = batchLearners.stream()
                .map(withDedicatedExecutor -> withDedicatedExecutor
                        .transformService(rpcClient -> UseCaseAwareBatchPaxosLearnerAdapter.wrap(useCase(), rpcClient))
                        .transformService(rpcClient -> metrics().instrument(BatchPaxosLearner.class, rpcClient)))
                .collect(Collectors.toList());

        LocalAndRemotes<WithDedicatedExecutor<BatchPaxosLearner>> allBatchLearners = LocalAndRemotes.of(
                WithDedicatedExecutor.of(local, MoreExecutors.newDirectExecutorService()), remoteLearners);

        return AutobatchingPaxosLearnerNetworkClientFactory.create(allBatchLearners, quorumSize());
    }

    @Value.Auxiliary
    @Value.Derived
    @Override
    public List<Closeable> closeables() {
        return ImmutableList.of(acceptorNetworkClientFactory(), learnerNetworkClientFactory());
    }

    @Override
    public Factory<PaxosAcceptorNetworkClient> acceptor() {
        return client -> metrics()
                .instrument(
                        PaxosAcceptorNetworkClient.class,
                        acceptorNetworkClientFactory().paxosAcceptorForClient(client),
                        client);
    }

    @Override
    public Factory<PaxosLearnerNetworkClient> learner() {
        return client -> metrics()
                .instrument(
                        PaxosLearnerNetworkClient.class,
                        learnerNetworkClientFactory().paxosLearnerForClient(client),
                        client);
    }

    public abstract static class Builder implements NetworkClientFactories.Builder {}
}
