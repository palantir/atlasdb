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

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;
import java.util.function.Supplier;

import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.ImmutableMap;
import com.palantir.atlasdb.config.LeaderConfig;
import com.palantir.atlasdb.timelock.paxos.DelegatingManagedTimestampService;
import com.palantir.atlasdb.timelock.paxos.ManagedTimestampService;
import com.palantir.atlasdb.timelock.paxos.PaxosResource;
import com.palantir.atlasdb.timelock.paxos.PaxosTimestampBoundStore;
import com.palantir.atlasdb.tracing.CloseableTrace;
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
    private final Function<String, Map<PaxosAcceptor, ExecutorService>> paxosAcceptorsForClient;
    private final Function<String, Map<PaxosLearner, ExecutorService>> paxosLearnersForClient;
    private final UUID uuid = UUID.randomUUID();
    private final ExecutorService executorService;

    public PaxosTimestampCreator(
            MetricRegistry metricRegistry,
            PaxosResource paxosResource,
            Supplier<PaxosRuntimeConfiguration> paxosRuntime,
            Function<String, Map<PaxosAcceptor, ExecutorService>> paxosAcceptorsForClient,
            Function<String, Map<PaxosLearner, ExecutorService>> paxosLearnersForClient,
            ExecutorService executorService) {
        this.metricRegistry = metricRegistry;
        this.paxosResource = paxosResource;
        this.paxosRuntime = paxosRuntime;
        this.paxosAcceptorsForClient = paxosAcceptorsForClient;
        this.paxosLearnersForClient = paxosLearnersForClient;
        this.executorService = executorService;
    }

    @Override
    public Supplier<ManagedTimestampService> createTimestampService(String client, LeaderConfig unused) {
        PaxosAcceptor ourAcceptor = paxosResource.getPaxosAcceptor(client);
        Map<PaxosAcceptor, ExecutorService> acceptors = ImmutableMap.<PaxosAcceptor, ExecutorService>builder()
                .put(ourAcceptor, executorService)
                .putAll(paxosAcceptorsForClient.apply(client))
                .build();

        PaxosLearner ourLearner = paxosResource.getPaxosLearner(client);
        Map<PaxosLearner, ExecutorService> learners = ImmutableMap.<PaxosLearner, ExecutorService>builder()
                .put(ourLearner, executorService)
                .putAll(paxosLearnersForClient.apply(client))
                .build();

        PaxosProposer proposer = instrument(PaxosProposer.class,
                PaxosProposerImpl.newProposer(
                        ourLearner,
                        acceptors,
                        learners,
                        PaxosRemotingUtils.getQuorumSize(acceptors.keySet()),
                        uuid),
                client);

        // make this async, remove later
//        executor.submit(() -> PaxosSynchronizer.synchronizeLearner(ourLearner, learners));

        return () -> createManagedPaxosTimestampService(proposer, client, acceptors, learners);
    }

    private static CloseableTrace startLocalTrace(String operation) {
        return CloseableTrace.startLocalTrace("AtlasDB:PaxosTimestampCreator", operation);
    }

    private ManagedTimestampService createManagedPaxosTimestampService(
            PaxosProposer proposer,
            String client,
            Map<PaxosAcceptor, ExecutorService> acceptors,
            Map<PaxosLearner, ExecutorService> learners) {
        // TODO (jkong): live reload ping
        TimestampBoundStore boundStore = instrument(TimestampBoundStore.class,
                new PaxosTimestampBoundStore(
                        proposer,
                        paxosResource.getPaxosLearner(client),
                        ImmutableMap.copyOf(acceptors),
                        ImmutableMap.copyOf(learners),
                        paxosRuntime.get().maximumWaitBeforeProposalMs()),
                client);

        try (CloseableTrace ignored = startLocalTrace("createManagedPaxosTimestampService")) {
            PersistentTimestampService persistentTimestampService = PersistentTimestampServiceImpl.create(boundStore);
            return new DelegatingManagedTimestampService(persistentTimestampService, persistentTimestampService);
        }
    }

    private <T> T instrument(Class<T> serviceClass, T service, String client) {
        // TODO(nziebart): tag with the client name, when tritium supports it
        return AtlasDbMetrics.instrument(metricRegistry, serviceClass, service, MetricRegistry.name(serviceClass));
    }
}
