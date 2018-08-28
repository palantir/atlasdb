/*
 * Copyright 2017 Palantir Technologies, Inc. All rights reserved.
 *
 * Licensed under the BSD-3 License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://opensource.org/licenses/BSD-3-Clause
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

import javax.net.ssl.SSLSocketFactory;

import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.palantir.atlasdb.config.LeaderConfig;
import com.palantir.atlasdb.factory.Leaders;
import com.palantir.atlasdb.timelock.paxos.DelegatingManagedTimestampService;
import com.palantir.atlasdb.timelock.paxos.ManagedTimestampService;
import com.palantir.atlasdb.timelock.paxos.PaxosResource;
import com.palantir.atlasdb.timelock.paxos.PaxosSynchronizer;
import com.palantir.atlasdb.timelock.paxos.PaxosTimeLockUriUtils;
import com.palantir.atlasdb.timelock.paxos.PaxosTimestampBoundStore;
import com.palantir.atlasdb.util.AtlasDbMetrics;
import com.palantir.common.concurrent.PTExecutors;
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
    private final Optional<SSLSocketFactory> optionalSecurity;
    private final Supplier<PaxosRuntimeConfiguration> paxosRuntime;

    public PaxosTimestampCreator(MetricRegistry metricRegistry, PaxosResource paxosResource,
            Set<String> remoteServers,
            Optional<SSLSocketFactory> optionalSecurity,
            Supplier<PaxosRuntimeConfiguration> paxosRuntime) {
        this.metricRegistry = metricRegistry;
        this.paxosResource = paxosResource;
        this.remoteServers = remoteServers;
        this.optionalSecurity = optionalSecurity;
        this.paxosRuntime = paxosRuntime;
    }

    @Override
    public Supplier<ManagedTimestampService> createTimestampService(String client, LeaderConfig unused) {
        ExecutorService executor = PTExecutors.newCachedThreadPool(new ThreadFactoryBuilder()
                .setNameFormat("atlas-consensus-" + client + "-%d")
                .setDaemon(true)
                .build());

        Set<String> namespacedUris = PaxosTimeLockUriUtils.getClientPaxosUris(remoteServers, client);
        List<PaxosAcceptor> acceptors = Leaders.createProxyAndLocalList(
                metricRegistry,
                paxosResource.getPaxosAcceptor(client),
                namespacedUris,
                optionalSecurity,
                PaxosAcceptor.class,
                "timestamp-bound-store." + client);

        PaxosLearner ourLearner = paxosResource.getPaxosLearner(client);
        List<PaxosLearner> learners = Leaders.createProxyAndLocalList(
                metricRegistry,
                ourLearner,
                namespacedUris,
                optionalSecurity,
                PaxosLearner.class,
                "timestamp-bound-store." + client);

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
                        paxosRuntime.get().maximumWaitBeforeProposalMs()),
                client);
        PersistentTimestampService persistentTimestampService = PersistentTimestampServiceImpl.create(boundStore);
        return new DelegatingManagedTimestampService(persistentTimestampService, persistentTimestampService);
    }

    private <T> T instrument(Class<T> serviceClass, T service, String client) {
        // TODO(nziebart): tag with the client name, when tritium supports it
        return AtlasDbMetrics.instrument(metricRegistry, serviceClass, service, MetricRegistry.name(serviceClass));
    }
}
