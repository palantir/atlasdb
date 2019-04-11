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

import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.ImmutableList;
import com.palantir.atlasdb.config.LeaderConfig;
import com.palantir.atlasdb.timelock.paxos.DelegatingManagedTimestampService;
import com.palantir.atlasdb.timelock.paxos.ManagedTimestampService;
import com.palantir.atlasdb.timelock.paxos.PaxosResource;
import com.palantir.atlasdb.timelock.paxos.PaxosTimestampBoundStore;
import com.palantir.atlasdb.util.AtlasDbMetrics;
import com.palantir.paxos.PaxosAcceptor;
import com.palantir.paxos.PaxosLearner;
import com.palantir.paxos.PaxosProposer;
import com.palantir.paxos.PaxosProposerImpl;
import com.palantir.timelock.config.PaxosRuntimeConfiguration;
import com.palantir.timestamp.PersistentTimestampService;
import com.palantir.timestamp.PersistentTimestampServiceImpl;
import com.palantir.timestamp.TimestampBoundStore;

public class PaxosTimestampCreator implements TimestampCreator {
    private final MetricRegistry metricRegistry;
    private final PaxosResource paxosResource;
    private final Supplier<PaxosRuntimeConfiguration> paxosRuntime;
    private final Function<String, List<PaxosAcceptor>> paxosAcceptorsForClient;
    private final Function<String, List<PaxosLearner>> paxosLearnersForClient;
    private final ExecutorService executor;

    public PaxosTimestampCreator(
            MetricRegistry metricRegistry,
            PaxosResource paxosResource,
            Supplier<PaxosRuntimeConfiguration> paxosRuntime,
            Function<String, List<PaxosAcceptor>> paxosAcceptorsForClient,
            Function<String, List<PaxosLearner>> paxosLearnersForClient,
            ExecutorService executor) {
        this.metricRegistry = metricRegistry;
        this.paxosResource = paxosResource;
        this.paxosRuntime = paxosRuntime;
        this.paxosAcceptorsForClient = paxosAcceptorsForClient;
        this.paxosLearnersForClient = paxosLearnersForClient;
        this.executor = executor;
    }

    @Override
    public Supplier<ManagedTimestampService> createTimestampService(String client, LeaderConfig unused) {
        PaxosAcceptor ourAcceptor = paxosResource.getPaxosAcceptor(client);
        List<PaxosAcceptor> acceptors = Stream.concat(
                paxosAcceptorsForClient.apply(client).stream(),
                Stream.of(ourAcceptor))
                .collect(Collectors.toList());

        PaxosLearner ourLearner = paxosResource.getPaxosLearner(client);
        List<PaxosLearner> learners = Stream.concat(
                paxosLearnersForClient.apply(client).stream(),
                Stream.of(ourLearner))
                .collect(Collectors.toList());

        PaxosProposer proposer = instrument(PaxosProposer.class,
                PaxosProposerImpl.newProposer(
                        ourLearner,
                        acceptors,
                        learners,
                        PaxosRemotingUtils.getQuorumSize(acceptors),
                        UUID.randomUUID(),
                        executor),
                client);

        return () -> createManagedPaxosTimestampService(proposer, client, acceptors, learners);
    }

    private ManagedTimestampService createManagedPaxosTimestampService(
            PaxosProposer proposer,
            String client,
            List<PaxosAcceptor> acceptors,
            List<PaxosLearner> learners) {
        // TODO (jkong): live reload ping
        TimestampBoundStore boundStore = instrument(TimestampBoundStore.class,
                new PaxosTimestampBoundStore(
                        proposer,
                        paxosResource.getPaxosLearner(client),
                        ImmutableList.copyOf(acceptors),
                        ImmutableList.copyOf(learners),
                        paxosRuntime.get().maximumWaitBeforeProposalMs(),
                        executor),
                client);
        PersistentTimestampService persistentTimestampService = PersistentTimestampServiceImpl.create(boundStore);
        return new DelegatingManagedTimestampService(persistentTimestampService, persistentTimestampService);
    }

    private <T> T instrument(Class<T> serviceClass, T service, String client) {
        // TODO(nziebart): tag with the client name, when tritium supports it
        return AtlasDbMetrics.instrument(metricRegistry, serviceClass, service, MetricRegistry.name(serviceClass));
    }
}
