/*
 * (c) Copyright 2020 Palantir Technologies Inc. All rights reserved.
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

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.util.UUID;
import java.util.function.Supplier;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.palantir.atlasdb.util.MetricsManagers;
import com.palantir.paxos.Client;
import com.palantir.paxos.ImmutableNamespaceAndUseCase;
import com.palantir.paxos.ImmutablePaxosStorageParameters;
import com.palantir.paxos.PaxosLearner;
import com.palantir.paxos.PaxosStateLog;
import com.palantir.paxos.PaxosStateLogImpl;
import com.palantir.paxos.PaxosStorageParameters;
import com.palantir.paxos.PaxosValue;
import com.palantir.paxos.SqliteConnections;
import com.palantir.paxos.SqlitePaxosStateLog;

public class PaxosStateLogMigrationIntegrationTest {
    private static final Client CLIENT = Client.of("test");

    @Rule
    public final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

    private LocalPaxosComponents paxosComponents;
    private PaxosUseCase useCase = PaxosUseCase.LEADER_FOR_ALL_CLIENTS;
    private Path legacyDirectory;
    private Path sqliteDirectory;

    @Before
    public void setUp() throws IOException {
        legacyDirectory = TEMPORARY_FOLDER.newFolder("legacy").toPath();
        sqliteDirectory = TEMPORARY_FOLDER.newFolder("sqlite").toPath();
        resetPaxosComponents();
    }

    @Test
    public void learnerMigratesLogStateCorrectly() throws IOException {
        int round = 100;
        PaxosStateLog<PaxosValue> fileBasedLog = createFileSystemLog(CLIENT);
        fileBasedLog.writeRound(round, valueForRound(round));

        PaxosStorageParameters parameters = getParametersForClient(CLIENT);
        PaxosLearner learner = paxosComponents.learner(CLIENT);
        PaxosStateLog<PaxosValue> sqliteLog = createSqliteLog(parameters);

        assertValuePresent(round, sqliteLog);
        assertValueLearned(round, learner);
    }

    @Test
    public void legacyLogIsTheSourceOfTruthWhenValueOnlyInLegacy() throws IOException {
        int migratedRound = 100;
        int nonMigratedRound = 200;
        PaxosStateLog<PaxosValue> fileBasedLog = createFileSystemLog(CLIENT);
        fileBasedLog.writeRound(migratedRound, valueForRound(migratedRound));

        PaxosStorageParameters parameters = getParametersForClient(CLIENT);
        PaxosLearner learner = paxosComponents.learner(CLIENT);
        PaxosStateLog<PaxosValue> sqliteLog = createSqliteLog(parameters);

        fileBasedLog.writeRound(nonMigratedRound, valueForRound(nonMigratedRound));

        assertValueAbsent(nonMigratedRound, sqliteLog);
        assertValuePresent(nonMigratedRound, fileBasedLog);
        assertValueLearned(nonMigratedRound, learner);
    }

    @Test
    public void legacyLogIsTheSourceOfTruthWhenValueAbsentFromLegacy() throws IOException {
        int migratedRound = 100;
        int rogueValue = 200;
        PaxosStateLog<PaxosValue> fileBasedLog = createFileSystemLog(CLIENT);
        fileBasedLog.writeRound(migratedRound, valueForRound(migratedRound));

        PaxosStorageParameters parameters = getParametersForClient(CLIENT);
        PaxosLearner learner = paxosComponents.learner(CLIENT);
        PaxosStateLog<PaxosValue> sqliteLog = createSqliteLog(parameters);

        sqliteLog.writeRound(rogueValue, valueForRound(rogueValue));

        assertValuePresent(rogueValue, sqliteLog);
        assertValueAbsent(rogueValue, fileBasedLog);
        assertValueNotLearned(rogueValue, learner);
    }

    @Test
    public void doesNotMigrateAgainIfGreatestSequencesMatch() throws IOException {
        int firstRound = 100;
        int secondRound = 200;
        int nonMigratedRound = 150;
        PaxosStateLog<PaxosValue> fileBasedLog = createFileSystemLog(CLIENT);
        fileBasedLog.writeRound(firstRound, valueForRound(firstRound));
        fileBasedLog.writeRound(secondRound, valueForRound(secondRound));

        PaxosStorageParameters parameters = getParametersForClient(CLIENT);
        paxosComponents.learner(CLIENT);
        PaxosStateLog<PaxosValue> sqliteLog = createSqliteLog(parameters);

        fileBasedLog.writeRound(nonMigratedRound, valueForRound(nonMigratedRound));

        resetPaxosComponents();
        PaxosLearner learner = paxosComponents.learner(CLIENT);

        assertValuePresent(firstRound, sqliteLog);
        assertValuePresent(secondRound, sqliteLog);
        assertValueAbsent(nonMigratedRound, sqliteLog);
        assertValueLearned(nonMigratedRound, learner);
    }

    @Test
    public void migratesAgainIfOutOfSyncDetected() throws IOException {
        int firstRound = 100;
        int secondRound = 200;
        PaxosStateLog<PaxosValue> fileBasedLog = createFileSystemLog(CLIENT);
        fileBasedLog.writeRound(firstRound, valueForRound(firstRound));

        PaxosStorageParameters parameters = getParametersForClient(CLIENT);
        paxosComponents.learner(CLIENT);
        PaxosStateLog<PaxosValue> sqliteLog = createSqliteLog(parameters);

        fileBasedLog.writeRound(secondRound, valueForRound(secondRound));

        resetPaxosComponents();
        PaxosLearner learner = paxosComponents.learner(CLIENT);

        assertValuePresent(secondRound, sqliteLog);
        assertValueLearned(secondRound, learner);
    }

    @Test
    public void noCrossClientPollution() throws IOException {
        int round = 200;
        int otherRound = 100;
        PaxosStateLog<PaxosValue> fileBasedLog = createFileSystemLog(CLIENT);
        fileBasedLog.writeRound(round, valueForRound(round));

        PaxosLearner learner = paxosComponents.learner(CLIENT);

        Client otherClient = Client.of("other");
        PaxosStateLog<PaxosValue> otherFileBasedLog = createFileSystemLog(otherClient);
        otherFileBasedLog.writeRound(otherRound, valueForRound(otherRound));
        fileBasedLog.writeRound(round, valueForRound(round));

        PaxosLearner otherLearner = paxosComponents.learner(otherClient);
        PaxosStorageParameters otherParameters = getParametersForClient(otherClient);
        PaxosStateLog<PaxosValue> otherSqliteLog = createSqliteLog(otherParameters);

        assertValueAbsent(round, otherSqliteLog);
        assertValuePresent(otherRound, otherSqliteLog);
        assertValueLearned(round, learner);
        assertValueNotLearned(otherRound, learner);
        assertValueNotLearned(round, otherLearner);
        assertValueLearned(otherRound, otherLearner);
    }

    private void assertValueLearned(int secondRound, PaxosLearner learner) {
        assertThat(learner.getLearnedValue(secondRound)).hasValue(valueForRound(secondRound));
    }

    private void assertValueNotLearned(int rogueValue, PaxosLearner learner) {
        assertThat(learner.getLearnedValue(rogueValue)).isEmpty();
    }

    private void assertValuePresent(int rogueValue, PaxosStateLog<PaxosValue> sqliteLog) throws IOException {
        assertThat(PaxosValue.BYTES_HYDRATOR.hydrateFromBytes(sqliteLog.readRound(rogueValue)))
                .isEqualTo(valueForRound(rogueValue));
    }

    private void assertValueAbsent(int nonMigratedRound, PaxosStateLog<PaxosValue> sqliteLog)
            throws IOException {
        assertThat(sqliteLog.readRound(nonMigratedRound)).isNull();
    }

    private void resetPaxosComponents() {
        paxosComponents = new LocalPaxosComponents(
                TimelockPaxosMetrics.of(useCase, MetricsManagers.createForTests()),
                useCase,
                legacyDirectory,
                sqliteDirectory, UUID.randomUUID(), true);
    }

    private PaxosValue valueForRound(int i) {
        return new PaxosValue("value", i, new byte[] {1});
    }

    private PaxosStateLog<PaxosValue> createFileSystemLog(Client client) {
        Path dir = useCase.logDirectoryRelativeToDataDirectory(legacyDirectory).resolve(client.value());
        String learnerLogDir = Paths.get(dir.toString(), PaxosTimeLockConstants.LEARNER_SUBDIRECTORY_PATH).toString();
        return new PaxosStateLogImpl<>(learnerLogDir);
    }

    private PaxosStateLog<PaxosValue> createSqliteLog(PaxosStorageParameters parameters) {
        Supplier<Connection> conn = SqliteConnections
                .createDefaultNamedSqliteDatabaseAtPath(parameters.sqliteBasedLogDirectory());
        return SqlitePaxosStateLog.create(parameters.namespaceAndUseCase(), conn);
    }

    private PaxosStorageParameters getParametersForClient(Client client) {
        Path dir = useCase.logDirectoryRelativeToDataDirectory(legacyDirectory).resolve(client.value());
        String learnerLogDir = Paths.get(dir.toString(), PaxosTimeLockConstants.LEARNER_SUBDIRECTORY_PATH).toString();
        Path sqlite = useCase.logDirectoryRelativeToDataDirectory(sqliteDirectory).toAbsolutePath();
        return ImmutablePaxosStorageParameters.builder()
                .fileBasedLogDirectory(learnerLogDir)
                .sqliteBasedLogDirectory(sqlite)
                .namespaceAndUseCase(ImmutableNamespaceAndUseCase
                        .of(client, String.format("%s!learner", useCase.toString())))
                .build();
    }
}
