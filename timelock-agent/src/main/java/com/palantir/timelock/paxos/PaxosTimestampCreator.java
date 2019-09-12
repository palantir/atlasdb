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

import java.util.UUID;
import java.util.function.Supplier;

import com.codahale.metrics.MetricRegistry;
import com.palantir.atlasdb.config.LeaderConfig;
import com.palantir.atlasdb.timelock.paxos.Client;
import com.palantir.atlasdb.timelock.paxos.ClientPaxosResourceFactory.ClientResources;
import com.palantir.atlasdb.timelock.paxos.NetworkClientFactories;
import com.palantir.atlasdb.timelock.paxos.PaxosTimestampBoundStore;
import com.palantir.atlasdb.util.AtlasDbMetrics;
import com.palantir.paxos.PaxosAcceptorNetworkClient;
import com.palantir.paxos.PaxosLearnerNetworkClient;
import com.palantir.paxos.PaxosProposer;
import com.palantir.paxos.PaxosProposerImpl;
import com.palantir.timelock.config.PaxosRuntimeConfiguration;
import com.palantir.timestamp.ManagedTimestampService;
import com.palantir.timestamp.PersistentTimestampServiceImpl;
import com.palantir.timestamp.TimestampBoundStore;

public class PaxosTimestampCreator implements TimestampCreator {
    private final MetricRegistry metricRegistry;
    private final ClientResources clientResources;
    private final Supplier<PaxosRuntimeConfiguration> paxosRuntimeConfig;

    PaxosTimestampCreator(
            MetricRegistry metricRegistry,
            ClientResources clientResources,
            Supplier<PaxosRuntimeConfiguration> paxosRuntimeConfig) {
        this.metricRegistry = metricRegistry;
        this.clientResources = clientResources;
        this.paxosRuntimeConfig = paxosRuntimeConfig;
    }

    @Override
    public Supplier<ManagedTimestampService> createTimestampService(Client client, LeaderConfig unused) {
        NetworkClientFactories clientFactories = clientResources.networkClientFactories();
        PaxosAcceptorNetworkClient acceptorNetworkClient = clientFactories.acceptor().create(client);
        PaxosLearnerNetworkClient learnerNetworkClient = clientFactories.learner().create(client);

        PaxosProposer proposer = instrument(PaxosProposer.class,
                PaxosProposerImpl.newProposer(
                        acceptorNetworkClient,
                        learnerNetworkClient,
                        clientResources.quorumSize(),
                        UUID.randomUUID()),
                client);

        return () -> createManagedPaxosTimestampService(proposer, client, acceptorNetworkClient, learnerNetworkClient);
    }

    private ManagedTimestampService createManagedPaxosTimestampService(
            PaxosProposer proposer,
            Client client,
            PaxosAcceptorNetworkClient acceptorNetworkClient,
            PaxosLearnerNetworkClient learnerNetworkClient) {
        // TODO (jkong): live reload ping
        TimestampBoundStore boundStore = instrument(
                TimestampBoundStore.class,
                new PaxosTimestampBoundStore(
                        proposer,
                        clientResources.components().learner(client),
                        acceptorNetworkClient,
                        learnerNetworkClient,
                        paxosRuntimeConfig.get().maximumWaitBeforeProposalMs()),
                client);
        return PersistentTimestampServiceImpl.create(boundStore);
    }

    private <T> T instrument(Class<T> serviceClass, T service, Client client) {
        // TODO(nziebart): tag with the client name, when tritium supports it
        return AtlasDbMetrics.instrument(metricRegistry, serviceClass, service, MetricRegistry.name(serviceClass));
    }
}
