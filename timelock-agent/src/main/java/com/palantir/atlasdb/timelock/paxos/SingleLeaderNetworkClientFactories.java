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

import com.palantir.atlasdb.timelock.paxos.NetworkClientFactories.Factory;
import com.palantir.paxos.PaxosAcceptor;
import com.palantir.paxos.PaxosAcceptorNetworkClient;
import com.palantir.paxos.PaxosLearner;
import com.palantir.paxos.PaxosLearnerNetworkClient;
import com.palantir.paxos.SingleLeaderAcceptorNetworkClient;
import com.palantir.paxos.SingleLeaderLearnerNetworkClient;
import com.palantir.timelock.paxos.ClientAwarePaxosAcceptor;
import com.palantir.timelock.paxos.ClientAwarePaxosAcceptorAdapter;
import com.palantir.timelock.paxos.ClientAwarePaxosLearner;
import com.palantir.timelock.paxos.ClientAwarePaxosLearnerAdapter;
import java.util.List;
import java.util.concurrent.ExecutorService;
import org.immutables.value.Value;

@Value.Immutable
abstract class SingleLeaderNetworkClientFactories {

    abstract TimelockProxyFactories proxyFactories();
    abstract int quorumSize();
    abstract ExecutorService sharedExecutor();
    abstract PaxosComponents components();

    NetworkClientFactories factories() {
        List<ClientAwarePaxosAcceptor> remoteClientAwareAcceptors = proxyFactories()
                .createRemoteProxies(ClientAwarePaxosAcceptor.class, "timestamp-bound-store.acceptor");

        Factory<PaxosAcceptorNetworkClient> acceptorClientFactory = client -> {
            List<PaxosAcceptor> remoteAcceptors =
                    ClientAwarePaxosAcceptorAdapter.wrap(remoteClientAwareAcceptors).apply(client);
            PaxosAcceptor localAcceptor = components().acceptor(client);
            LocalAndRemotes<PaxosAcceptor> allAcceptors =
                    proxyFactories().instrumentLocalAndRemotesFor(PaxosAcceptor.class, localAcceptor, remoteAcceptors);
            return new SingleLeaderAcceptorNetworkClient(
                    allAcceptors.all(),
                    quorumSize(),
                    allAcceptors.withSharedExecutor(sharedExecutor()));
        };

        List<ClientAwarePaxosLearner> remoteClientAwareLearners = proxyFactories()
                .createRemoteProxies(ClientAwarePaxosLearner.class, "timestamp-bound-store.learner");

        Factory<PaxosLearnerNetworkClient> learnerClientFactory = client -> {
            List<PaxosLearner> remoteLearners =
                    ClientAwarePaxosLearnerAdapter.wrap(remoteClientAwareLearners).apply(client);
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
