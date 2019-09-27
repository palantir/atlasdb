/*
 * (c) Copyright 2018 Palantir Technologies Inc. All rights reserved.
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
package com.palantir.timelock.paxos;

import java.util.function.Supplier;

import com.palantir.atlasdb.config.LeaderConfig;
import com.palantir.atlasdb.timelock.paxos.Client;
import com.palantir.atlasdb.timelock.paxos.NetworkClientFactories;
import com.palantir.atlasdb.timelock.paxos.PaxosResourcesFactory.PaxosUseCaseContext;
import com.palantir.atlasdb.timelock.paxos.PaxosTimestampBoundStore;
import com.palantir.paxos.PaxosAcceptorNetworkClient;
import com.palantir.paxos.PaxosLearner;
import com.palantir.paxos.PaxosLearnerNetworkClient;
import com.palantir.paxos.PaxosProposer;
import com.palantir.timelock.config.PaxosRuntimeConfiguration;
import com.palantir.timestamp.ManagedTimestampService;
import com.palantir.timestamp.PersistentTimestampServiceImpl;
import com.palantir.timestamp.TimestampBoundStore;

public class PaxosTimestampCreator implements TimestampCreator {

    private final PaxosUseCaseContext context;
    private final Supplier<PaxosRuntimeConfiguration> paxosRuntimeConfig;

    PaxosTimestampCreator(PaxosUseCaseContext context, Supplier<PaxosRuntimeConfiguration> paxosRuntimeConfig) {
        this.context = context;
        this.paxosRuntimeConfig = paxosRuntimeConfig;
    }

    @Override
    public Supplier<ManagedTimestampService> createTimestampService(Client client, LeaderConfig unused) {
        NetworkClientFactories clientFactories = context.networkClientFactories();
        PaxosAcceptorNetworkClient acceptorNetworkClient = clientFactories.acceptor().create(client);
        PaxosLearnerNetworkClient learnerNetworkClient = clientFactories.learner().create(client);
        PaxosProposer proposer = context.proposerFactory().create(client);
        PaxosLearner learner = context.components().learner(client);

        return () -> createManagedPaxosTimestampService(
                proposer,
                client,
                learner,
                acceptorNetworkClient,
                learnerNetworkClient);
    }

    private ManagedTimestampService createManagedPaxosTimestampService(
            PaxosProposer proposer,
            Client client,
            PaxosLearner knowledge,
            PaxosAcceptorNetworkClient acceptorNetworkClient,
            PaxosLearnerNetworkClient learnerNetworkClient) {
        // TODO (jkong): live reload ping
        TimestampBoundStore boundStore = context.metrics().instrument(
                TimestampBoundStore.class,
                new PaxosTimestampBoundStore(
                        proposer,
                        knowledge,
                        acceptorNetworkClient,
                        learnerNetworkClient,
                        paxosRuntimeConfig.get().maximumWaitBeforeProposalMs()),
                "paxos-timestamp-bound-store",
                client);
        return PersistentTimestampServiceImpl.create(boundStore);
    }

}
