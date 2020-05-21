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
import java.util.UUID;
import java.util.function.Supplier;

import org.immutables.value.Value;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Suppliers;
import com.google.common.collect.Maps;
import com.palantir.atlasdb.timelock.management.DiskNamespaceLoader;
import com.palantir.atlasdb.timelock.management.PersistentNamespaceLoader;
import com.palantir.common.remoting.ServiceNotAvailableException;
import com.palantir.leader.LocalPingableLeader;
import com.palantir.leader.PaxosKnowledgeEventRecorder;
import com.palantir.leader.PingableLeader;
import com.palantir.paxos.Client;
import com.palantir.paxos.ImmutableNamespaceAndUseCase;
import com.palantir.paxos.ImmutablePaxosStorageParameters;
import com.palantir.paxos.PaxosAcceptor;
import com.palantir.paxos.PaxosAcceptorImpl;
import com.palantir.paxos.PaxosLearner;
import com.palantir.paxos.PaxosLearnerImpl;
import com.palantir.paxos.PaxosStorageParameters;
import com.palantir.paxos.SqlitePaxosStateLogFactory;

public class LocalPaxosComponents {

    private final TimelockPaxosMetrics metrics;
    private final PaxosUseCase paxosUseCase;
    private final Path baseLogDirectory;
    private final Path sqliteLogDirectory;
    private final UUID leaderUuid;
    private final Map<Client, Components> componentsByClient = Maps.newConcurrentMap();
    private final Supplier<BatchPaxosAcceptor> memoizedBatchAcceptor;
    private final Supplier<BatchPaxosLearner> memoizedBatchLearner;
    private final Supplier<BatchPingableLeader> memoizedBatchPingableLeader;
    private final boolean canCreateNewClients;
    private final SqlitePaxosStateLogFactory sqliteFactory = new SqlitePaxosStateLogFactory();

    LocalPaxosComponents(TimelockPaxosMetrics metrics,
            PaxosUseCase paxosUseCase,
            Path legacyLogDirectory,
            Path sqliteLogDirectory,
            UUID leaderUuid,
            boolean canCreateNewClients) {
        this.metrics = metrics;
        this.paxosUseCase = paxosUseCase;
        this.baseLogDirectory = legacyLogDirectory;
        this.sqliteLogDirectory = sqliteLogDirectory;
        this.leaderUuid = leaderUuid;
        this.memoizedBatchAcceptor = Suppliers.memoize(this::createBatchAcceptor);
        this.memoizedBatchLearner = Suppliers.memoize(this::createBatchLearner);
        this.memoizedBatchPingableLeader = Suppliers.memoize(this::createBatchPingableLeader);
        this.canCreateNewClients = canCreateNewClients;
    }

    public static LocalPaxosComponents createWithBlockingMigration(TimelockPaxosMetrics metrics,
            PaxosUseCase paxosUseCase,
            Path legacyLogDirectory,
            Path sqliteLogDirectory,
            UUID leaderUuid,
            boolean canCreateNewClients) {
        LocalPaxosComponents components = new LocalPaxosComponents(metrics, paxosUseCase, legacyLogDirectory,
                sqliteLogDirectory, leaderUuid, canCreateNewClients);

        Path legacyClientDir = paxosUseCase.logDirectoryRelativeToDataDirectory(legacyLogDirectory);
        PersistentNamespaceLoader namespaceLoader = new DiskNamespaceLoader(legacyClientDir);
        namespaceLoader.getAllPersistedNamespaces().forEach(components::getOrCreateComponents);
        components.getOrCreateComponents(PaxosUseCase.PSEUDO_LEADERSHIP_CLIENT);
        return components;
    }

    public PaxosAcceptor acceptor(Client client) {
        return getOrCreateComponents(client).acceptor();
    }

    public PaxosLearner learner(Client client) {
        return getOrCreateComponents(client).learner();
    }

    public PingableLeader pingableLeader(Client client) {
        return getOrCreateComponents(client).pingableLeader();
    }

    public BatchPaxosAcceptor batchAcceptor() {
        return memoizedBatchAcceptor.get();
    }

    public BatchPaxosLearner batchLearner() {
        return memoizedBatchLearner.get();
    }

    public BatchPingableLeader batchPingableLeader() {
        return memoizedBatchPingableLeader.get();
    }

    private Components getOrCreateComponents(Client client) {
        return componentsByClient.computeIfAbsent(client, this::createComponents);
    }

    private Components createComponents(Client client) {
        Path legacyClientDir = paxosUseCase.logDirectoryRelativeToDataDirectory(baseLogDirectory)
                .resolve(client.value());

        // TODO (jkong): This test is no longer valid with the new implementation, it needs to be fixed before
        // the migration.
        if (!canCreateNewClients && clientDirectoryDoesNotExist(legacyClientDir)) {
            throw new ServiceNotAvailableException("This TimeLock server is not allowed to create new clients at this"
                    + " time, and the client " + client + " provided is novel for this TimeLock server.");
        }

        PaxosLearner learner = PaxosLearnerImpl.newVerifyingLearner(getLearnerParameters(client), sqliteFactory,
                PaxosKnowledgeEventRecorder.NO_OP);
        PaxosAcceptor acceptor = PaxosAcceptorImpl.newVerifyingAcceptor(getAcceptorParameters(client), sqliteFactory);
        PingableLeader localPingableLeader = new LocalPingableLeader(learner, leaderUuid);

        return ImmutableComponents.builder()
                .acceptor(acceptor)
                .learner(learner)
                .pingableLeader(localPingableLeader)
                .build();
    }

    @VisibleForTesting
    PaxosStorageParameters getLearnerParameters(Client client) {
        Path legacyDir = paxosUseCase.logDirectoryRelativeToDataDirectory(baseLogDirectory).resolve(client.value());
        Path learnerLogDir = Paths.get(legacyDir.toString(), PaxosTimeLockConstants.LEARNER_SUBDIRECTORY_PATH);
        String learnerUseCase = String.format("%s!learner", paxosUseCase.toString());
        return ImmutablePaxosStorageParameters.builder()
                .fileBasedLogDirectory(learnerLogDir.toString())
                .sqliteBasedLogDirectory(sqliteLogDirectory)
                .namespaceAndUseCase(ImmutableNamespaceAndUseCase.of(client, learnerUseCase))
                .build();
    }

    private PaxosStorageParameters getAcceptorParameters(Client client) {
        Path legacyDir = paxosUseCase.logDirectoryRelativeToDataDirectory(baseLogDirectory).resolve(client.value());
        Path acceptorLogDir = Paths.get(legacyDir.toString(), PaxosTimeLockConstants.ACCEPTOR_SUBDIRECTORY_PATH);
        String acceptorUseCase = String.format("%s!acceptor", paxosUseCase.toString());
        return ImmutablePaxosStorageParameters.builder()
                .fileBasedLogDirectory(acceptorLogDir.toString())
                .sqliteBasedLogDirectory(sqliteLogDirectory)
                .namespaceAndUseCase(ImmutableNamespaceAndUseCase.of(client, acceptorUseCase))
                .build();
    }

    private boolean clientDirectoryDoesNotExist(Path clientDirectory) {
        return !clientDirectory.toFile().exists();
    }

    private BatchPaxosAcceptor createBatchAcceptor() {
        AcceptorCache acceptorCache = metrics
                .instrument(AcceptorCache.class, new AcceptorCacheImpl());
        return metrics.instrument(
                BatchPaxosAcceptor.class,
                new LocalBatchPaxosAcceptor(this, acceptorCache));
    }

    private BatchPaxosLearner createBatchLearner() {
        return metrics.instrument(
                BatchPaxosLearner.class,
                new LocalBatchPaxosLearner(this));
    }

    private BatchPingableLeader createBatchPingableLeader() {
        return metrics.instrument(
                BatchPingableLeader.class,
                new BatchPingableLeaderResource(leaderUuid, this));
    }

    @Value.Immutable
    interface Components {
        PaxosAcceptor acceptor();
        PaxosLearner learner();
        PingableLeader pingableLeader();
    }

}
