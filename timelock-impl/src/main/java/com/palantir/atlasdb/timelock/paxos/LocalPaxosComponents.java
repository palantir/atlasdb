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

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.function.Supplier;

import org.immutables.value.Value;

import com.google.common.base.Suppliers;
import com.google.common.collect.Maps;
import com.palantir.paxos.PaxosAcceptor;
import com.palantir.paxos.PaxosAcceptorImpl;
import com.palantir.paxos.PaxosLearner;
import com.palantir.paxos.PaxosLearnerImpl;

public class LocalPaxosComponents {

    private final TimelockPaxosMetrics metrics;
    private final Path logDirectory;
    private final Map<Client, Components> componentsByClient = Maps.newConcurrentMap();
    private final Supplier<BatchPaxosAcceptor> memoizedBatchAcceptor;
    private final Supplier<BatchPaxosLearner> memoizedBatchLearner;

    LocalPaxosComponents(TimelockPaxosMetrics metrics, Path logDirectory) {
        this.metrics = metrics;
        this.logDirectory = logDirectory;
        this.memoizedBatchAcceptor = Suppliers.memoize(this::createBatchAcceptor);
        this.memoizedBatchLearner = Suppliers.memoize(this::createBatchLearner);
    }

    public PaxosAcceptor acceptor(Client client) {
        return getOrCreateComponents(client).acceptor();
    }

    public PaxosLearner learner(Client client) {
        return getOrCreateComponents(client).learner();
    }

    public BatchPaxosAcceptor batchAcceptor() {
        return memoizedBatchAcceptor.get();
    }

    public BatchPaxosLearner batchLearner() {
        return memoizedBatchLearner.get();
    }

    private Components getOrCreateComponents(Client client) {
        return componentsByClient.computeIfAbsent(client, this::createComponents);
    }

    private Components createComponents(Client client) {
        Path clientDirectory = logDirectory.resolve(client.value());
        Path learnerLogDir = Paths.get(clientDirectory.toString(), PaxosTimeLockConstants.LEARNER_SUBDIRECTORY_PATH);

        PaxosLearner learner = metrics.instrument(
                PaxosLearner.class,
                PaxosLearnerImpl.newLearner(learnerLogDir.toString()),
                "paxos-learner",
                client);

        Path acceptorLogDir = Paths.get(clientDirectory.toString(), PaxosTimeLockConstants.ACCEPTOR_SUBDIRECTORY_PATH);
        PaxosAcceptor acceptor = metrics.instrument(
                PaxosAcceptor.class,
                PaxosAcceptorImpl.newAcceptor(acceptorLogDir.toString()),
                "paxos-acceptor",
                client);

        return ImmutableComponents.builder()
                .acceptor(acceptor)
                .learner(learner)
                .build();
    }

    private BatchPaxosAcceptor createBatchAcceptor() {
        AcceptorCache acceptorCache = metrics
                .instrument(AcceptorCache.class, new AcceptorCacheImpl(), "acceptor-cache");
        return metrics.instrument(
                BatchPaxosAcceptor.class,
                new LocalBatchPaxosAcceptor(this, acceptorCache),
                "local-batch-paxos-acceptor");
    }

    private BatchPaxosLearner createBatchLearner() {
        return metrics.instrument(
                BatchPaxosLearner.class,
                new LocalBatchPaxosLearner(this),
                "local-batch-paxos-learner");
    }

    @Value.Immutable
    interface Components {
        PaxosAcceptor acceptor();
        PaxosLearner learner();
    }

}
