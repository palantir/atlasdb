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
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
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
import java.util.OptionalLong;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import org.assertj.core.api.AbstractAssert;
import org.assertj.core.api.Assertions;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.palantir.common.streams.KeyedStream;

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
        SqlitePaxosStateLogFactory factory = new SqlitePaxosStateLogFactory();
        source = factory.create(NAMESPACE, sourceConnSupplier);
        target = spy(factory.create(NAMESPACE, targetConnSupplier));
        migrationState = factory.createMigrationState(NAMESPACE, targetConnSupplier);
    }

    @Test
    public void emptyLogMigrationSuccessfullyMarksAsMigrated() {
        migrateFrom(source);
        assertThat(migrationState.hasMigratedFromInitialState()).isTrue();
    }

    @Test
    public void logMigrationWithNoLowerBoundMigratesOnlyGreatest() {
        long lowerBound = 10;
        long upperBound = 25;
        insertValuesWithinBounds(lowerBound, upperBound, source);

        long cutoff = migrateFrom(source);

        assertThat(cutoff).isEqualTo(upperBound);
        assertThat(migrationState.hasMigratedFromInitialState()).isTrue();
        assertThat(migrationState.isInMigratedState()).isTrue();
        assertThat(target.getLeastLogEntry()).isEqualTo(upperBound);
        assertThat(target.getGreatestLogEntry()).isEqualTo(upperBound);

        LongStream.rangeClosed(lowerBound, upperBound - 1)
                .mapToObj(sequence -> readRoundUnchecked(target, sequence))
                .map(Assertions::assertThat)
                .forEach(AbstractAssert::isNull);
        assertThat(getPaxosValue(target, upperBound)).isEqualTo(valueForRound(upperBound));
    }

    @Test
    public void logMigrationWithLowerBoundMigratesFromBound() {
        long lowerBound = 10;
        long upperBound = 25;
        insertValuesWithinBounds(lowerBound, upperBound, source);

        long expectedCutoff = 17;
        long cutoff = migrateFrom(source, OptionalLong.of(expectedCutoff));

        assertThat(cutoff).isEqualTo(expectedCutoff);
        assertThat(migrationState.hasMigratedFromInitialState()).isTrue();
        assertThat(migrationState.isInMigratedState()).isTrue();
        assertThat(target.getLeastLogEntry()).isEqualTo(expectedCutoff);
        assertThat(target.getGreatestLogEntry()).isEqualTo(upperBound);

        LongStream.rangeClosed(lowerBound, expectedCutoff - 1)
                .mapToObj(sequence -> readRoundUnchecked(target, sequence))
                .map(Assertions::assertThat)
                .forEach(AbstractAssert::isNull);
        KeyedStream.of(LongStream.rangeClosed(expectedCutoff, upperBound).boxed())
                .map(sequence -> getPaxosValue(target, sequence))
                .mapKeys(PaxosStateLogTestUtils::valueForRound)
                .entries()
                .forEach(entry -> assertThat(entry.getKey()).isEqualTo(entry.getValue()));
    }

    @Test
    public void migrationDeletesExistingState() {
        long lowerBound = 13;
        long upperBound = 35;
        insertValuesWithinBounds(lowerBound, upperBound, target);

        long cutoff = migrateFrom(source);
        assertThat(cutoff).isEqualTo(PaxosAcceptor.NO_LOG_ENTRY);
        assertThat(migrationState.hasMigratedFromInitialState()).isTrue();
        assertThat(target.getLeastLogEntry()).isEqualTo(PaxosAcceptor.NO_LOG_ENTRY);
        assertThat(target.getGreatestLogEntry()).isEqualTo(PaxosAcceptor.NO_LOG_ENTRY);
        verify(target, times(1)).truncate(upperBound);
    }

    @Test
    public void migrateIfInValidationState() {
        long lowerBound = 10;
        long upperBound = 25;
        insertValuesWithinBounds(lowerBound, upperBound, source);
        migrationState.migrateToValidationState();

        long cutoff = migrateFrom(source);
        assertThat(cutoff).isEqualTo(upperBound);
        assertThat(migrationState.isInMigratedState()).isTrue();
        assertThat(target.getLeastLogEntry()).isEqualTo(upperBound);
        assertThat(target.getGreatestLogEntry()).isEqualTo(upperBound);
        verify(target, times(1)).truncate(anyLong());
    }

    @Test
    public void doNotMigrateIfAlreadyMigratedAndReturnOldCutoff() {
        long lowerBound = 10;
        long upperBound = 25;
        insertValuesWithinBounds(lowerBound, upperBound, source);

        int expectedCutoff = 17;
        migrateFrom(source, OptionalLong.of(expectedCutoff));

        List<PaxosValue> newValuesWritten = new ArrayList<>();
        newValuesWritten.addAll(insertValuesWithinBounds(1, 5, source));
        newValuesWritten.addAll(insertValuesWithinBounds(30, 35, source));
        long cutoff = migrateFrom(source, OptionalLong.of(0));

        assertThat(cutoff).isEqualTo(expectedCutoff);
        assertThat(target.getLeastLogEntry()).isEqualTo(expectedCutoff);
        assertThat(target.getGreatestLogEntry()).isEqualTo(upperBound);

        newValuesWritten.forEach(value -> assertThat(readRoundUnchecked(target, value.seq)).isNull());
    }

    @Test
    public void logMigrationSuccessfullyMigratesManyEntriesInBatches() throws IOException {
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

        migrateFrom(mockLog, OptionalLong.of(lowerBound));
        assertThat(migrationState.hasMigratedFromInitialState()).isTrue();
        assertThat(target.getLeastLogEntry()).isEqualTo(lowerBound);
        assertThat(target.getGreatestLogEntry()).isEqualTo(upperBound);
        verify(target, times(11)).writeBatchOfRounds(anyList());

        for (long counter = lowerBound; counter <= upperBound; counter += BATCH_SIZE) {
            assertThat(readRoundUnchecked(target, counter)).containsExactly(valueForRound(counter).persistToBytes());
        }
    }

    private long migrateFrom(PaxosStateLog<PaxosValue> sourceLog) {
        return migrateFrom(sourceLog, OptionalLong.empty());
    }

    private long migrateFrom(PaxosStateLog<PaxosValue> sourceLog, OptionalLong lowerBound) {
        return PaxosStateLogMigrator.migrateAndReturnCutoff(ImmutableMigrationContext.<PaxosValue>builder()
                .sourceLog(sourceLog)
                .destinationLog(target)
                .hydrator(PaxosValue.BYTES_HYDRATOR)
                .migrationState(migrationState)
                .migrateFrom(lowerBound)
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
