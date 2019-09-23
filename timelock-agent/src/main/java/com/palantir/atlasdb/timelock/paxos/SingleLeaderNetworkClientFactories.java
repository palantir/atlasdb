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

import com.palantir.atlasdb.timelock.paxos.NetworkClientFactories.Factory;
import com.palantir.paxos.PaxosAcceptor;
import com.palantir.paxos.PaxosAcceptorNetworkClient;
import com.palantir.paxos.PaxosLearner;
import com.palantir.paxos.PaxosLearnerNetworkClient;
import com.palantir.paxos.SingleLeaderAcceptorNetworkClient;
import com.palantir.paxos.SingleLeaderLearnerNetworkClient;
import com.palantir.timelock.paxos.TimelockPaxosAcceptorAdapter;
import com.palantir.timelock.paxos.TimelockPaxosAcceptorRpcClient;
import com.palantir.timelock.paxos.TimelockPaxosLearnerAdapter;
import com.palantir.timelock.paxos.TimelockPaxosLearnerRpcClient;

@Value.Immutable
abstract class SingleLeaderNetworkClientFactories {

    abstract TimelockProxyFactories proxyFactories();
    abstract int quorumSize();
    abstract ExecutorService sharedExecutor();
    abstract PaxosComponents components();

    NetworkClientFactories factories() {
        List<TimelockPaxosAcceptorRpcClient> remoteClientAwareAcceptors = proxyFactories()
                .createRemoteProxies(TimelockPaxosAcceptorRpcClient.class, "timestamp-bound-store.acceptor");

        Factory<PaxosAcceptorNetworkClient> acceptorClientFactory = client -> {
            List<PaxosAcceptor> remoteAcceptors = TimelockPaxosAcceptorAdapter
                    .wrap(PaxosUseCase.TIMESTAMP, remoteClientAwareAcceptors)
                    .apply(client);
            PaxosAcceptor localAcceptor = components().acceptor(client);
            LocalAndRemotes<PaxosAcceptor> allAcceptors =
                    proxyFactories().instrumentLocalAndRemotesFor(PaxosAcceptor.class, localAcceptor, remoteAcceptors);
            return new SingleLeaderAcceptorNetworkClient(
                    allAcceptors.all(),
                    quorumSize(),
                    allAcceptors.withSharedExecutor(sharedExecutor()));
        };

        List<TimelockPaxosLearnerRpcClient> remoteClientAwareLearners = proxyFactories()
                .createRemoteProxies(TimelockPaxosLearnerRpcClient.class, "timestamp-bound-store.learner");

        Factory<PaxosLearnerNetworkClient> learnerClientFactory = client -> {
            List<PaxosLearner> remoteLearners = TimelockPaxosLearnerAdapter
                    .wrap(PaxosUseCase.TIMESTAMP, remoteClientAwareLearners)
                    .apply(client);
            PaxosLearner localLearner = components().learner(client);
            LocalAndRemotes<PaxosLearner> allLearners =
                    proxyFactories().instrumentLocalAndRemotesFor(PaxosLearner.class, localLearner, remoteLearners);
            return new SingleLeaderLearnerNetworkClient(
                    allLearners.local(),
                    allLearners.remotes(),
                    quorumSize(),
                    allLearners.withSharedExecutor(sharedExecutor()));
        };

        return ImmutableNetworkClientFactories.builder()
                .acceptor(acceptorClientFactory)
                .learner(learnerClientFactory)
                .build();
    }

}
