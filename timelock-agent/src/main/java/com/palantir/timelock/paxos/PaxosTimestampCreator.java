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
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.ImmutableList;
import com.palantir.atlasdb.config.LeaderConfig;
import com.palantir.atlasdb.http.AtlasDbHttpClients;
import com.palantir.atlasdb.timelock.paxos.ClientAwarePaxosAcceptor;
import com.palantir.atlasdb.timelock.paxos.ClientAwarePaxosAcceptorAdapter;
import com.palantir.atlasdb.timelock.paxos.ClientAwarePaxosLearner;
import com.palantir.atlasdb.timelock.paxos.ClientAwarePaxosLearnerAdapter;
import com.palantir.atlasdb.timelock.paxos.DelegatingManagedTimestampService;
import com.palantir.atlasdb.timelock.paxos.ManagedTimestampService;
import com.palantir.atlasdb.timelock.paxos.PaxosResource;
import com.palantir.atlasdb.timelock.paxos.PaxosSynchronizer;
import com.palantir.atlasdb.timelock.paxos.PaxosTimeLockUriUtils;
import com.palantir.atlasdb.timelock.paxos.PaxosTimestampBoundStore;
import com.palantir.atlasdb.util.AtlasDbMetrics;
import com.palantir.conjure.java.config.ssl.TrustContext;
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
    private final Set<String> remoteServers;
    private final Optional<TrustContext> optionalSecurity;
    private final Supplier<PaxosRuntimeConfiguration> paxosRuntime;
    private final ExecutorService executor;

    public PaxosTimestampCreator(MetricRegistry metricRegistry, PaxosResource paxosResource,
            Set<String> remoteServers,
            Optional<TrustContext> optionalSecurity,
            Supplier<PaxosRuntimeConfiguration> paxosRuntime,
            ExecutorService executor) {
        this.metricRegistry = metricRegistry;
        this.paxosResource = paxosResource;
        this.remoteServers = remoteServers;
        this.optionalSecurity = optionalSecurity;
        this.paxosRuntime = paxosRuntime;
        this.executor = executor;
    }

    @Override
    public Supplier<ManagedTimestampService> createTimestampService(String client, LeaderConfig unused) {
        PaxosAcceptor ourAcceptor = paxosResource.getPaxosAcceptor(client);
        Set<String> namespacedUris = PaxosTimeLockUriUtils.getClientPaxosUris(remoteServers, client);
        List<PaxosAcceptor> acceptors = Stream.concat(
                createProxies(
                        metricRegistry,
                        namespacedUris,
                        optionalSecurity,
                        ClientAwarePaxosAcceptor.class,
                        "timestamp-bound-store." + client)
                        .map(acceptor -> new ClientAwarePaxosAcceptorAdapter(client, acceptor)),
                Stream.of(ourAcceptor))
                .collect(Collectors.toList());

        PaxosLearner ourLearner = paxosResource.getPaxosLearner(client);
        List<PaxosLearner> learners = Stream.concat(
                createProxies(
                        metricRegistry,
                        namespacedUris,
                        optionalSecurity,
                        ClientAwarePaxosLearner.class,
                        "timestamp-bound-store." + client)
                        .map(learner -> new ClientAwarePaxosLearnerAdapter(client, learner)),
                Stream.of(ourLearner))
                .collect(Collectors.toList());

        PaxosProposer proposer = instrument(PaxosProposer.class,
                PaxosProposerImpl.newProposer(
                        ourLearner,
                        ImmutableList.copyOf(acceptors),
                        ImmutableList.copyOf(learners),
                        PaxosRemotingUtils.getQuorumSize(acceptors),
                        UUID.randomUUID(),
                        executor),
                client);

        PaxosSynchronizer.synchronizeLearner(ourLearner, learners);

        return () -> createManagedPaxosTimestampService(proposer, client, acceptors, learners);
    }

    private static <T> Stream<T> createProxies(
            MetricRegistry metrics,
            Set<String> remoteUris,
            Optional<TrustContext> trustContext,
            Class<T> clazz,
            String userAgent) {
        return remoteUris.stream()
                .map(uri -> AtlasDbHttpClients.createProxy(metrics, trustContext, uri, clazz, userAgent, false));
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
