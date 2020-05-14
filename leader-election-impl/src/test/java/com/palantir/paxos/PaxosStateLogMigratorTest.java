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
import static com.palantir.paxos.PaxosStateLogTestUtils.NAMESPACE;
import static com.palantir.paxos.PaxosStateLogTestUtils.getPaxosValue;
import static com.palantir.paxos.PaxosStateLogTestUtils.readRoundUnchecked;
import static com.palantir.paxos.PaxosStateLogTestUtils.valueForRound;

import java.io.IOException;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
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

    private PaxosStateLog<PaxosValue> source;
    private PaxosStateLog<PaxosValue> target;
    private SqlitePaxosStateLogMigrationState migrationState;

    @Before
    public void setup() throws IOException {
        Supplier<Connection> sourceConnSupplier = SqliteConnections
                .createDefaultNamedSqliteDatabaseAtPath(tempFolder.newFolder("source").toPath());
        Supplier<Connection> targetConnSupplier = SqliteConnections
                .createDefaultNamedSqliteDatabaseAtPath(tempFolder.newFolder("target").toPath());
        source = SqlitePaxosStateLog.create(NAMESPACE, sourceConnSupplier);
        target = SqlitePaxosStateLog.create(NAMESPACE, targetConnSupplier);
        migrationState = SqlitePaxosStateLogMigrationState.create(NAMESPACE, targetConnSupplier);
    }

    @Test
    public void emptyLogMigrationSuccessfullyMarksAsMigrated() {
        migrateFrom(source);
        assertThat(migrationState.hasMigratedFromInitialState()).isTrue();
    }

    @Test
    public void logMigrationSuccessfullyMigratesEntries() {
        long lowerBound = 10;
        long upperBound = 25;
        List<PaxosValue> valuesWritten = insertValuesWithinBounds(lowerBound, upperBound, source);

        migrateFrom(source);
        assertThat(migrationState.hasMigratedFromInitialState()).isTrue();
        assertThat(target.getLeastLogEntry()).isEqualTo(lowerBound);
        assertThat(target.getGreatestLogEntry()).isEqualTo(upperBound);

        valuesWritten.forEach(value -> assertThat(getPaxosValue(target, value.seq)).isEqualTo(value));
    }

    @Test
    public void migrationDeletesExistingState() {
        long lowerBound = 13;
        long upperBound = 35;
        List<PaxosValue> valuesWritten = insertValuesWithinBounds(lowerBound, upperBound, target);

        migrateFrom(source);
        assertThat(migrationState.hasMigratedFromInitialState()).isTrue();
        assertThat(target.getLeastLogEntry()).isEqualTo(PaxosAcceptor.NO_LOG_ENTRY);
        assertThat(target.getGreatestLogEntry()).isEqualTo(PaxosAcceptor.NO_LOG_ENTRY);
        valuesWritten.forEach(value -> assertThat(readRoundUnchecked(target, value.seq)).isNull());
    }

    @Test
    public void doNotMigrateIfAlreadyMigratedAndNoMismatchDetected() throws IOException {
        long lowerBound = 10;
        long upperBound = 25;
        List<PaxosValue> expectedValues = insertValuesWithinBounds(lowerBound, upperBound, source);

        migrateFrom(source);
        assertThat(migrationState.hasMigratedFromInitialState()).isTrue();
        assertThat(target.getLeastLogEntry()).isEqualTo(lowerBound);
        assertThat(target.getGreatestLogEntry()).isEqualTo(upperBound);

        List<PaxosValue> unExpectedValues = insertValuesWithinBounds(1, 2, source);
        migrateFrom(source);

        assertThat(target.getLeastLogEntry()).isNotEqualTo(source.getLeastLogEntry());
        expectedValues.forEach(value -> assertThat(getPaxosValue(target, value.seq)).isEqualTo(value));
        unExpectedValues.forEach(value -> assertThat(readRoundUnchecked(target, value.seq)).isNull());
    }

    @Test
    public void migirateAgainIfMismatchDetected() throws IOException {
        long lowerBound = 10;
        long upperBound = 25;
        List<PaxosValue> valuesWritten = new ArrayList<>();
        valuesWritten.addAll(insertValuesWithinBounds(lowerBound, upperBound, source));

        migrateFrom(source);
        assertThat(migrationState.hasMigratedFromInitialState()).isTrue();
        assertThat(target.getLeastLogEntry()).isEqualTo(lowerBound);
        assertThat(target.getGreatestLogEntry()).isEqualTo(upperBound);

        valuesWritten.addAll(insertValuesWithinBounds(1, 2, source));
        valuesWritten.addAll(insertValuesWithinBounds(28, 30, source));

        migrateFrom(source);

        assertThat(target.getLeastLogEntry()).isEqualTo(source.getLeastLogEntry());
        assertThat(target.getGreatestLogEntry()).isEqualTo(source.getGreatestLogEntry());
        valuesWritten.forEach(value -> assertThat(getPaxosValue(target, value.seq)).isEqualTo(value));
    }

    @Test
    public void logMigrationSuccessfullyMigratesManyEntriesIncludingSingleEntryInLastBatch() throws IOException {
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

        migrateFrom(mockLog);
        assertThat(migrationState.hasMigratedFromInitialState()).isTrue();
        assertThat(target.getLeastLogEntry()).isEqualTo(lowerBound);
        assertThat(target.getGreatestLogEntry()).isEqualTo(upperBound);

        for (long counter = lowerBound; counter <= upperBound; counter += BATCH_SIZE) {
            assertThat(readRoundUnchecked(target, counter)).containsExactly(valueForRound(counter).persistToBytes());
        }
    }

    private void migrateFrom(PaxosStateLog<PaxosValue> sourceLog) {
        PaxosStateLogMigrator.migrateToValidation(ImmutableMigrationContext.<PaxosValue>builder()
                .sourceLog(sourceLog)
                .destinationLog(target)
                .hydrator(PaxosValue.BYTES_HYDRATOR)
                .migrationState(migrationState)
                .build());
    }

    private List<PaxosValue> insertValuesWithinBounds(long from, long to, PaxosStateLog<PaxosValue> targetLog) {
        List<PaxosValue> valuesWritten = LongStream.rangeClosed(from, to)
                .mapToObj(PaxosStateLogTestUtils::valueForRound)
                .collect(Collectors.toList());
        valuesWritten.forEach(value -> targetLog.writeRound(value.seq, value));
        return valuesWritten;
    }
}
