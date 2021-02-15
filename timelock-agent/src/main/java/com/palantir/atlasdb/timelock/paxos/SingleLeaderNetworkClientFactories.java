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

import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.MoreExecutors;
import com.palantir.common.concurrent.CheckedRejectionExecutorService;
import com.palantir.common.streams.KeyedStream;
import com.palantir.paxos.PaxosAcceptor;
import com.palantir.paxos.PaxosAcceptorNetworkClient;
import com.palantir.paxos.PaxosConstants;
import com.palantir.paxos.PaxosLearner;
import com.palantir.paxos.PaxosLearnerNetworkClient;
import com.palantir.paxos.SingleLeaderAcceptorNetworkClient;
import com.palantir.paxos.SingleLeaderLearnerNetworkClient;
import com.palantir.timelock.paxos.TimelockPaxosAcceptorAdapters;
import com.palantir.timelock.paxos.TimelockPaxosLearnerAdapters;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.immutables.value.Value;

@SuppressWarnings("immutables:subtype")
@Value.Immutable
abstract class SingleLeaderNetworkClientFactories
        implements NetworkClientFactories, Dependencies.NetworkClientFactories {

    @Value.Default
    Supplier<Boolean> useBatchedEndpoints() {
        return () -> false;
    }

    @Value.Auxiliary
    @Value.Derived
    @Override
    public Factory<PaxosAcceptorNetworkClient> acceptor() {
        return client -> {
            List<WithDedicatedExecutor<PaxosAcceptor>> remoteAcceptors =
                    TimelockPaxosAcceptorAdapters.create(useCase(), remoteClients(), useBatchedEndpoints(), client);

            PaxosAcceptor localAcceptor = components().acceptor(client);
            LocalAndRemotes<WithDedicatedExecutor<PaxosAcceptor>> paxosAcceptors = LocalAndRemotes.of(
                            WithDedicatedExecutor.of(localAcceptor, MoreExecutors.newDirectExecutorService()),
                            remoteAcceptors)
                    .enhanceRemotes(remote ->
                            remote.transformService(service -> metrics().instrument(PaxosAcceptor.class, service)));

            SingleLeaderAcceptorNetworkClient uninstrumentedAcceptor = new SingleLeaderAcceptorNetworkClient(
                    paxosAcceptors.all().stream()
                            .map(WithDedicatedExecutor::service)
                            .collect(Collectors.toList()),
                    quorumSize(),
                    KeyedStream.of(paxosAcceptors.all())
                            .mapKeys(WithDedicatedExecutor::service)
                            .map(WithDedicatedExecutor::executor)
                            .collectToMap(),
                    PaxosConstants.CANCEL_REMAINING_CALLS);
            return metrics().instrument(PaxosAcceptorNetworkClient.class, uninstrumentedAcceptor);
        };
    }

    @Value.Auxiliary
    @Value.Derived
    @Override
    public Factory<PaxosLearnerNetworkClient> learner() {
        return client -> {
            List<WithDedicatedExecutor<PaxosLearner>> remoteLearners =
                    TimelockPaxosLearnerAdapters.create(useCase(), remoteClients(), useBatchedEndpoints(), client)
                            .stream()
                            .map(withExecutor -> withExecutor.transformService(
                                    remote -> metrics().instrument(PaxosLearner.class, remote, client)))
                            .collect(Collectors.toList());
            PaxosLearner localLearner = components().learner(client);

            Map<PaxosLearner, CheckedRejectionExecutorService> executorMap =
                    ImmutableMap.<PaxosLearner, CheckedRejectionExecutorService>builder()
                            .putAll(KeyedStream.of(remoteLearners)
                                    .mapKeys(WithDedicatedExecutor::service)
                                    .map(WithDedicatedExecutor::executor)
                                    .collectToMap())
                            .put(
                                    localLearner,
                                    new CheckedRejectionExecutorService(MoreExecutors.newDirectExecutorService()))
                            .build();

            SingleLeaderLearnerNetworkClient uninstrumentedLearner = new SingleLeaderLearnerNetworkClient(
                    localLearner,
                    remoteLearners.stream().map(WithDedicatedExecutor::service).collect(Collectors.toList()),
                    quorumSize(),
                    executorMap,
                    PaxosConstants.CANCEL_REMAINING_CALLS);
            return metrics().instrument(PaxosLearnerNetworkClient.class, uninstrumentedLearner);
        };
    }

    public abstract static class Builder implements NetworkClientFactories.Builder {}
}
