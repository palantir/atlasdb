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
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import static com.palantir.paxos.PaxosStateLogMigrator.BATCH_SIZE;

import java.io.IOException;
import java.sql.Connection;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.palantir.common.base.Throwables;

public class PaxosStateLogMigratorTest {
    private static final NamespaceAndUseCase NAMESPACE_AND_USE_CASE = ImmutableNamespaceAndUseCase
            .of(Client.of("client"), "tom");

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    private PaxosStateLog<PaxosValue> source;
    private PaxosStateLog<PaxosValue> target;

    private SqlitePaxosStateLogMigrationState migrationState;

    @Before
    public void setup() throws IOException {
        Supplier<Connection> sourceConnSupplier = SqliteConnections
                .createDefaultNamedSqliteDatabaseAtPath(tempFolder.newFolder("source").toPath());
        Supplier<Connection> targetConnSupplier = SqliteConnections
                .createDefaultNamedSqliteDatabaseAtPath(tempFolder.newFolder("target").toPath());
        source = SqlitePaxosStateLog.create(NAMESPACE_AND_USE_CASE, sourceConnSupplier);
        target = SqlitePaxosStateLog.create(NAMESPACE_AND_USE_CASE, targetConnSupplier);
        migrationState = SqlitePaxosStateLogMigrationState.create(NAMESPACE_AND_USE_CASE, targetConnSupplier);
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
                assertThat(PaxosValue.BYTES_HYDRATOR.hydrateFromBytes(readRoundUnchecked(value.seq))).isEqualTo(value));
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
        valuesWritten.forEach(value -> assertThat(readRoundUnchecked(value.seq)).isNull());
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
        valuesWritten.forEach(value -> assertThat(readRoundUnchecked(value.seq)).isNull());
    }

    @Test
    public void logMigrationSuccessfullyMigratesManyEntries() throws IOException {
        long lowerBound = 10;
        long upperBound = lowerBound + BATCH_SIZE * 10;

        PaxosStateLog<PaxosValue> mockLog = mock(PaxosStateLog.class);

        when(mockLog.getLeastLogEntry()).thenReturn(lowerBound);
        when(mockLog.getGreatestLogEntry()).thenReturn(upperBound);
        when(mockLog.readRound(anyLong())).thenAnswer(invocation -> {
            long sequence = (long) invocation.getArguments()[0];
            if (sequence > upperBound || sequence < lowerBound) {
                return null;
            }
            return valueForRound(sequence).persistToBytes();
        });

        PaxosStateLogMigrator.migrate(ImmutableMigrationContext.<PaxosValue>builder()
                .sourceLog(mockLog)
                .destinationLog(target)
                .hydrator(PaxosValue.BYTES_HYDRATOR)
                .migrationState(migrationState)
                .build());
        assertThat(migrationState.hasAlreadyMigrated()).isTrue();
        assertThat(target.getLeastLogEntry()).isEqualTo(lowerBound);
        assertThat(target.getGreatestLogEntry()).isEqualTo(upperBound);

        for (long counter = lowerBound; counter <= upperBound; counter += BATCH_SIZE) {
            assertThat(readRoundUnchecked(counter)).containsExactly(valueForRound(counter).persistToBytes());
        }
    }

    private byte[] readRoundUnchecked(long seq) {
        try {
            return target.readRound(seq);
        } catch (IOException e) {
            throw Throwables.rewrapAndThrowUncheckedException(e);
        }
    }

    private static PaxosValue valueForRound(long round) {
        byte[] bytes = new byte[] { 1 };
        return new PaxosValue("someLeader", round, bytes);
    }
}
