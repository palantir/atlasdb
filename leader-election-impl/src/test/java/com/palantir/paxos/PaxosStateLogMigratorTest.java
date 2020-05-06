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

package com.palantir.paxos;

import static org.assertj.core.api.Assertions.assertThat;

import static com.palantir.paxos.PaxosStateLogMigrator.BATCH_SIZE;

import java.io.IOException;
import java.sql.Connection;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class PaxosStateLogMigratorTest {
    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    private static final String LOG_NAMESPACE_1 = "tom";

    private Supplier<Connection> sourceConnSupplier;
    private Supplier<Connection> targetConnSupplier;

    private PaxosStateLog<PaxosValue> source;
    private SqlitePaxosStateLog<PaxosValue> target;

    private SqlitePaxosStateLogMigrationState migrationState;

    @Before
    public void setup() throws IOException {
        sourceConnSupplier = SqliteConnections
                .createSqliteDatabase(tempFolder.getRoot().toPath().resolve("test.db").toString());
        targetConnSupplier = SqliteConnections
                .createSqliteDatabase(tempFolder.newFolder("subfolder").toPath().resolve("test.db").toString());
        source = SqlitePaxosStateLog.create(LOG_NAMESPACE_1, sourceConnSupplier);
        target = SqlitePaxosStateLog.create(LOG_NAMESPACE_1, targetConnSupplier);
        migrationState = SqlitePaxosStateLogMigrationState.create(LOG_NAMESPACE_1, targetConnSupplier);
    }

    @Test
    public void emptyLogMigrationSuccessfullyMarksAsMigrated() {
        PaxosStateLogMigrator.migrate(ImmutableMigrationContext.<PaxosValue>builder()
                .sourceLog(source)
                .destinationLog(target)
                .hydrator(PaxosValue.BYTES_HYDRATOR)
                .migrationState(migrationState)
                .build());
        assertThat(migrationState.hasAlreadyMigrated()).isTrue();
    }

    @Test
    public void logMigrationSuccessfullyMigratesEntries() {
        long lowerBound = 10;
        long upperBound = 25;
        List<PaxosValue> valuesWritten = LongStream.rangeClosed(lowerBound, upperBound)
                .mapToObj(PaxosStateLogMigratorTest::valueForRound)
                .collect(Collectors.toList());
        valuesWritten.forEach(value -> source.writeRound(value.seq, value));

        PaxosStateLogMigrator.migrate(ImmutableMigrationContext.<PaxosValue>builder()
                .sourceLog(source)
                .destinationLog(target)
                .hydrator(PaxosValue.BYTES_HYDRATOR)
                .migrationState(migrationState)
                .build());
        assertThat(migrationState.hasAlreadyMigrated()).isTrue();
        assertThat(target.getLeastLogEntry()).isEqualTo(lowerBound);
        assertThat(target.getGreatestLogEntry()).isEqualTo(upperBound);

        valuesWritten.forEach(value ->
                assertThat(PaxosValue.BYTES_HYDRATOR.hydrateFromBytes(target.readRound(value.seq))).isEqualTo(value));
    }

    @Test
    public void migrationDeletesExistingState() {
        long lowerBound = 10;
        long upperBound = 25;
        List<PaxosValue> valuesWritten = LongStream.rangeClosed(lowerBound, upperBound)
                .mapToObj(PaxosStateLogMigratorTest::valueForRound)
                .collect(Collectors.toList());
        valuesWritten.forEach(value -> target.writeRound(value.seq, value));

        PaxosStateLogMigrator.migrate(ImmutableMigrationContext.<PaxosValue>builder()
                .sourceLog(source)
                .destinationLog(target)
                .hydrator(PaxosValue.BYTES_HYDRATOR)
                .migrationState(migrationState)
                .build());
        assertThat(migrationState.hasAlreadyMigrated()).isTrue();
        assertThat(target.getLeastLogEntry()).isEqualTo(PaxosAcceptor.NO_LOG_ENTRY);
        assertThat(target.getGreatestLogEntry()).isEqualTo(PaxosAcceptor.NO_LOG_ENTRY);
        valuesWritten.forEach(value -> assertThat(target.readRound(value.seq)).isNull());
    }

    @Test
    public void doNotMigrateIfAlreadyMigrated() {
        migrationState.finishMigration();

        long lowerBound = 10;
        long upperBound = 25;
        List<PaxosValue> valuesWritten = LongStream.rangeClosed(lowerBound, upperBound)
                .mapToObj(PaxosStateLogMigratorTest::valueForRound)
                .collect(Collectors.toList());
        valuesWritten.forEach(value -> source.writeRound(value.seq, value));

        PaxosStateLogMigrator.migrate(ImmutableMigrationContext.<PaxosValue>builder()
                .sourceLog(source)
                .destinationLog(target)
                .hydrator(PaxosValue.BYTES_HYDRATOR)
                .migrationState(migrationState)
                .build());

        assertThat(migrationState.hasAlreadyMigrated()).isTrue();
        assertThat(target.getLeastLogEntry()).isEqualTo(PaxosAcceptor.NO_LOG_ENTRY);
        assertThat(target.getGreatestLogEntry()).isEqualTo(PaxosAcceptor.NO_LOG_ENTRY);
        valuesWritten.forEach(value -> assertThat(target.readRound(value.seq)).isNull());
    }

    @Test
    public void logMigrationSuccessfullyMigratesManyEntries() {
        long lowerBound = 10;
        long upperBound = lowerBound + BATCH_SIZE * 10;
        List<PaxosValue> valuesWritten = LongStream.rangeClosed(lowerBound, upperBound)
                .mapToObj(PaxosStateLogMigratorTest::valueForRound)
                .collect(Collectors.toList());
        List<PaxosRound<PaxosValue>> asBatch = valuesWritten.stream()
                .map(value -> ImmutablePaxosRound.<PaxosValue>builder().value(value).sequence(value.seq).build())
                .collect(Collectors.toList());
        source.writeBatchOfRounds(asBatch);

        PaxosStateLogMigrator.migrate(ImmutableMigrationContext.<PaxosValue>builder()
                .sourceLog(source)
                .destinationLog(target)
                .hydrator(PaxosValue.BYTES_HYDRATOR)
                .migrationState(migrationState)
                .build());
        assertThat(migrationState.hasAlreadyMigrated()).isTrue();
        assertThat(target.getLeastLogEntry()).isEqualTo(lowerBound);
        assertThat(target.getGreatestLogEntry()).isEqualTo(upperBound);

        valuesWritten.forEach(value ->
                assertThat(PaxosValue.BYTES_HYDRATOR.hydrateFromBytes(target.readRound(value.seq))).isEqualTo(value));
    }

    private static PaxosValue valueForRound(long round) {
        byte[] bytes = new byte[16];
        ThreadLocalRandom.current().nextBytes(bytes);
        return new PaxosValue("someLeader", round, bytes);
    }
}
