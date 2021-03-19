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

import com.codahale.metrics.Counter;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Suppliers;
import com.palantir.atlasdb.AtlasDbMetricNames;
import com.palantir.atlasdb.timelock.management.DiskNamespaceLoader;
import com.palantir.atlasdb.timelock.management.PersistentNamespaceLoader;
import com.palantir.common.concurrent.PTExecutors;
import com.palantir.common.remoting.ServiceNotAvailableException;
import com.palantir.leader.LocalPingableLeader;
import com.palantir.leader.PaxosKnowledgeEventRecorder;
import com.palantir.leader.PingableLeader;
import com.palantir.logsafe.SafeArg;
import com.palantir.paxos.Client;
import com.palantir.paxos.ImmutableLegacyOperationMarkers;
import com.palantir.paxos.ImmutableNamespaceAndUseCase;
import com.palantir.paxos.ImmutablePaxosStorageParameters;
import com.palantir.paxos.PaxosAcceptor;
import com.palantir.paxos.PaxosAcceptorImpl;
import com.palantir.paxos.PaxosLearner;
import com.palantir.paxos.PaxosLearnerImpl;
import com.palantir.paxos.PaxosStorageParameters;
import com.palantir.paxos.PaxosValue;
import com.palantir.paxos.SplittingPaxosStateLog;
import com.palantir.sls.versions.OrderableSlsVersion;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.function.Supplier;
import javax.sql.DataSource;
import org.immutables.value.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("FinalClass") // mocks
public class LocalPaxosComponents {
    private static final Logger log = LoggerFactory.getLogger(LocalPaxosComponents.class);

    private final TimelockPaxosMetrics metrics;
    private final PaxosUseCase paxosUseCase;
    private final Path baseLogDirectory;
    private final DataSource sqliteDataSource;
    private final UUID leaderUuid;
    private final Map<Client, Components> componentsByClient = new ConcurrentHashMap<>();
    private final Supplier<BatchPaxosAcceptor> memoizedBatchAcceptor;
    private final Supplier<BatchPaxosLearner> memoizedBatchLearner;
    private final Supplier<BatchPingableLeader> memoizedBatchPingableLeader;
    private final boolean canCreateNewClients;
    private final OrderableSlsVersion timeLockVersion;
    private final boolean skipConsistencyCheckAndTruncateOldPaxosLog;

    private LocalPaxosComponents(
            TimelockPaxosMetrics metrics,
            PaxosUseCase paxosUseCase,
            Path legacyLogDirectory,
            DataSource sqliteDataSource,
            UUID leaderUuid,
            boolean canCreateNewClients,
            OrderableSlsVersion timeLockVersion,
            boolean skipConsistencyCheckAndTruncateOldPaxosLog) {
        this.metrics = metrics;
        this.paxosUseCase = paxosUseCase;
        this.baseLogDirectory = legacyLogDirectory;
        this.sqliteDataSource = sqliteDataSource;
        this.leaderUuid = leaderUuid;
        this.memoizedBatchAcceptor = Suppliers.memoize(this::createBatchAcceptor);
        this.memoizedBatchLearner = Suppliers.memoize(this::createBatchLearner);
        this.memoizedBatchPingableLeader = Suppliers.memoize(this::createBatchPingableLeader);
        this.canCreateNewClients = canCreateNewClients;
        this.timeLockVersion = timeLockVersion;
        this.skipConsistencyCheckAndTruncateOldPaxosLog = skipConsistencyCheckAndTruncateOldPaxosLog;
    }

    public static LocalPaxosComponents createWithAsyncMigration(
            TimelockPaxosMetrics metrics,
            PaxosUseCase paxosUseCase,
            Path legacyLogDirectory,
            DataSource sqliteDataSource,
            UUID leaderUuid,
            boolean canCreateNewClients,
            OrderableSlsVersion timeLockVersion,
            boolean skipConsistencyCheckAndTruncateOldPaxosLog) {
        ExecutorService sqliteAsyncExecutor = PTExecutors.newSingleThreadExecutor(true);
        LocalPaxosComponents components = createWithAsyncMigration(
                metrics,
                paxosUseCase,
                legacyLogDirectory,
                sqliteDataSource,
                leaderUuid,
                canCreateNewClients,
                timeLockVersion,
                skipConsistencyCheckAndTruncateOldPaxosLog,
                sqliteAsyncExecutor);
        sqliteAsyncExecutor.shutdown();
        return components;
    }

    public static LocalPaxosComponents createWithAsyncMigration(
            TimelockPaxosMetrics metrics,
            PaxosUseCase paxosUseCase,
            Path legacyLogDirectory,
            DataSource sqliteDataSource,
            UUID leaderUuid,
            boolean canCreateNewClients,
            OrderableSlsVersion timeLockVersion,
            boolean skipConsistencyCheckAndTruncateOldPaxosLog,
            ExecutorService sqliteAsyncExecutor) {
        LocalPaxosComponents components = new LocalPaxosComponents(
                metrics,
                paxosUseCase,
                legacyLogDirectory,
                sqliteDataSource,
                leaderUuid,
                canCreateNewClients,
                timeLockVersion,
                skipConsistencyCheckAndTruncateOldPaxosLog);

        Path legacyClientDir = paxosUseCase.logDirectoryRelativeToDataDirectory(legacyLogDirectory);
        PersistentNamespaceLoader namespaceLoader = new DiskNamespaceLoader(legacyClientDir);
        Set<Client> namespaces = namespaceLoader.getAllPersistedNamespaces();

        sqliteAsyncExecutor.execute(() -> migrate(components, namespaces));

        return components;
    }

    private static void migrate(LocalPaxosComponents components, Set<Client> namespaces) {
        log.info("Performing asynchronous migration of {} namespaces", SafeArg.of("numNamespaces", namespaces.size()));
        Instant startInstant = Instant.now();
        namespaces.forEach(components::getOrCreateComponents);
        log.info(
                "Successfully migrated a total of {} namespaces in {}",
                SafeArg.of("numNamespaces", namespaces.size()),
                SafeArg.of("duration", Duration.between(startInstant, Instant.now())));
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
        Path legacyClientDir = paxosUseCase
                .logDirectoryRelativeToDataDirectory(baseLogDirectory)
                .resolve(client.value());

        // TODO (jkong): This test is no longer valid with the new implementation, it needs to be fixed before
        // the migration.
        if (!canCreateNewClients && clientDirectoryDoesNotExist(legacyClientDir)) {
            throw new ServiceNotAvailableException("This TimeLock server is not allowed to create new clients at this"
                    + " time, and the client " + client + " provided is novel for this TimeLock server.");
        }

        PaxosLearner learner = PaxosLearnerImpl.newSplittingLearner(
                getLearnerParameters(client), createMetrics(PaxosLearner.class), PaxosKnowledgeEventRecorder.NO_OP);

        PaxosAcceptor acceptor = PaxosAcceptorImpl.newSplittingAcceptor(
                getAcceptorParameters(client),
                createMetrics(PaxosAcceptor.class),
                learner.getGreatestLearnedValue().map(PaxosValue::getRound));
        PingableLeader localPingableLeader = new LocalPingableLeader(learner, leaderUuid, timeLockVersion);

        return ImmutableComponents.builder()
                .acceptor(acceptor)
                .learner(learner)
                .pingableLeader(localPingableLeader)
                .build();
    }

    private SplittingPaxosStateLog.LegacyOperationMarkers createMetrics(Class<?> forClass) {
        return ImmutableLegacyOperationMarkers.builder()
                .markLegacyRead(getReadCounter(forClass)::inc)
                .markLegacyWrite(getWriteCounter(forClass)::inc)
                .build();
    }

    @VisibleForTesting
    Counter getReadCounter(Class<?> forClass) {
        return metrics.asMetricsManager().registerOrGetCounter(forClass, AtlasDbMetricNames.LEGACY_READ);
    }

    @VisibleForTesting
    Counter getWriteCounter(Class<?> forClass) {
        return metrics.asMetricsManager().registerOrGetCounter(forClass, AtlasDbMetricNames.LEGACY_WRITE);
    }

    @VisibleForTesting
    PaxosStorageParameters getLearnerParameters(Client client) {
        Path legacyDir = paxosUseCase
                .logDirectoryRelativeToDataDirectory(baseLogDirectory)
                .resolve(client.value());
        Path learnerLogDir = Paths.get(legacyDir.toString(), PaxosTimeLockConstants.LEARNER_SUBDIRECTORY_PATH);
        String learnerUseCase = String.format("%s!learner", paxosUseCase.toString());
        return ImmutablePaxosStorageParameters.builder()
                .fileBasedLogDirectory(learnerLogDir.toString())
                .sqliteDataSource(sqliteDataSource)
                .namespaceAndUseCase(ImmutableNamespaceAndUseCase.of(client, learnerUseCase))
                .skipConsistencyCheckAndTruncateOldPaxosLog(skipConsistencyCheckAndTruncateOldPaxosLog)
                .build();
    }

    @VisibleForTesting
    PaxosStorageParameters getAcceptorParameters(Client client) {
        Path legacyDir = paxosUseCase
                .logDirectoryRelativeToDataDirectory(baseLogDirectory)
                .resolve(client.value());
        Path acceptorLogDir = Paths.get(legacyDir.toString(), PaxosTimeLockConstants.ACCEPTOR_SUBDIRECTORY_PATH);
        String acceptorUseCase = String.format("%s!acceptor", paxosUseCase.toString());
        return ImmutablePaxosStorageParameters.builder()
                .fileBasedLogDirectory(acceptorLogDir.toString())
                .sqliteDataSource(sqliteDataSource)
                .namespaceAndUseCase(ImmutableNamespaceAndUseCase.of(client, acceptorUseCase))
                .skipConsistencyCheckAndTruncateOldPaxosLog(skipConsistencyCheckAndTruncateOldPaxosLog)
                .build();
    }

    private boolean clientDirectoryDoesNotExist(Path clientDirectory) {
        return !clientDirectory.toFile().exists();
    }

    private BatchPaxosAcceptor createBatchAcceptor() {
        AcceptorCache acceptorCache = metrics.instrument(AcceptorCache.class, new AcceptorCacheImpl());
        return metrics.instrument(BatchPaxosAcceptor.class, new LocalBatchPaxosAcceptor(this, acceptorCache));
    }

    private BatchPaxosLearner createBatchLearner() {
        return metrics.instrument(BatchPaxosLearner.class, new LocalBatchPaxosLearner(this));
    }

    private BatchPingableLeader createBatchPingableLeader() {
        return metrics.instrument(BatchPingableLeader.class, new BatchPingableLeaderResource(leaderUuid, this));
    }

    @Value.Immutable
    interface Components {
        PaxosAcceptor acceptor();

        PaxosLearner learner();

        PingableLeader pingableLeader();
    }
}
