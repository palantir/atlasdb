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
import java.util.stream.Collectors;

import org.immutables.value.Value;

import com.google.common.util.concurrent.MoreExecutors;
import com.palantir.common.streams.KeyedStream;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;
import com.palantir.paxos.PaxosAcceptor;
import com.palantir.paxos.PaxosAcceptorNetworkClient;
import com.palantir.paxos.PaxosLearner;
import com.palantir.paxos.PaxosLearnerNetworkClient;
import com.palantir.paxos.SingleLeaderAcceptorNetworkClient;
import com.palantir.paxos.SingleLeaderLearnerNetworkClient;
import com.palantir.timelock.paxos.TimelockPaxosAcceptorAdapter;
import com.palantir.timelock.paxos.TimelockPaxosLearnerAdapter;

@Value.Immutable
abstract class SingleLeaderNetworkClientFactories implements
        NetworkClientFactories, Dependencies.NetworkClientFactories {

    @Value.Auxiliary
    @Value.Derived
    @Override
    public Factory<PaxosAcceptorNetworkClient> acceptor() {
        return client -> {
            List<WithDedicatedExecutor<PaxosAcceptor>> remoteAcceptors = assignExecutors(client);
            PaxosAcceptor localAcceptor = components().acceptor(client);
            LocalAndRemotes<WithDedicatedExecutor<PaxosAcceptor>> paxosAcceptors = LocalAndRemotes.of(
                    WithDedicatedExecutor.of(localAcceptor, MoreExecutors.newDirectExecutorService()),
                    remoteAcceptors)
                    .enhanceRemotes(remote -> remote.transformService(
                            service -> metrics().instrument(PaxosAcceptor.class, service)));

            SingleLeaderAcceptorNetworkClient uninstrumentedAcceptor = new SingleLeaderAcceptorNetworkClient(
                    paxosAcceptors.all().stream().map(WithDedicatedExecutor::service).collect(Collectors.toList()),
                    quorumSize(),
                    KeyedStream.of(paxosAcceptors.all())
                            .mapKeys(WithDedicatedExecutor::service)
                            .map(WithDedicatedExecutor::executor)
                            .collectToMap(),
                    PaxosTimeLockConstants.CANCEL_REMAINING_CALLS);
            return metrics().instrument(PaxosAcceptorNetworkClient.class, uninstrumentedAcceptor);
        };
    }

    private List<WithDedicatedExecutor<PaxosAcceptor>> assignExecutors(Client client) {
        if (useCase() == PaxosUseCase.LEADER_FOR_ALL_CLIENTS || useCase() == PaxosUseCase.TIMESTAMP) {
            return TimelockPaxosAcceptorAdapter
                    .wrapWithDedicatedExecutors(useCase(), remoteClients())
                    .apply(client);
        }
        throw new SafeIllegalStateException("This use case is unsupported for single leader paxos.",
                SafeArg.of("paxosUseCase", useCase()));
    }

    @Value.Auxiliary
    @Value.Derived
    @Override
    public Factory<PaxosLearnerNetworkClient> learner() {
        return client -> {
            List<PaxosLearner> remoteLearners = TimelockPaxosLearnerAdapter
                    .wrap(useCase(), remoteClients())
                    .apply(client);
            PaxosLearner localLearner = components().learner(client);

            LocalAndRemotes<PaxosLearner> allLearners = LocalAndRemotes.of(localLearner, remoteLearners)
                    .enhanceRemotes(remote -> metrics().instrument(PaxosLearner.class, remote, client));
            SingleLeaderLearnerNetworkClient uninstrumentedLearner = new SingleLeaderLearnerNetworkClient(
                    allLearners.local(),
                    allLearners.remotes(),
                    quorumSize(),
                    allLearners.withSharedExecutor(sharedExecutor()),
                    PaxosTimeLockConstants.CANCEL_REMAINING_CALLS);
            return metrics().instrument(PaxosLearnerNetworkClient.class, uninstrumentedLearner);
        };
    }

    public abstract static class Builder implements NetworkClientFactories.Builder {}

}
