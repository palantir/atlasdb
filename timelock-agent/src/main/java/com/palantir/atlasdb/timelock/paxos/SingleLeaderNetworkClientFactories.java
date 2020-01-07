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

import org.immutables.value.Value;

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
            List<PaxosAcceptor> remoteAcceptors = TimelockPaxosAcceptorAdapter
                    .wrap(useCase(), remoteClients())
                    .apply(client);
            PaxosAcceptor localAcceptor = components().acceptor(client);
            LocalAndRemotes<PaxosAcceptor> allAcceptors = metrics().instrumentLocalAndRemotesFor(
                    PaxosAcceptor.class,
                    localAcceptor,
                    remoteAcceptors,
                    client);
            return new SingleLeaderAcceptorNetworkClient(
                    allAcceptors.all(),
                    quorumSize(),
                    allAcceptors.withSharedExecutor(sharedExecutor()));
        };
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
            LocalAndRemotes<PaxosLearner> allLearners = metrics().instrumentLocalAndRemotesFor(
                    PaxosLearner.class,
                    localLearner,
                    remoteLearners,
                    client);
            return new SingleLeaderLearnerNetworkClient(
                    allLearners.local(),
                    allLearners.remotes(),
                    quorumSize(),
                    allLearners.withSharedExecutor(sharedExecutor()));
        };
    }

    public abstract static class Builder implements NetworkClientFactories.Builder {}

}
