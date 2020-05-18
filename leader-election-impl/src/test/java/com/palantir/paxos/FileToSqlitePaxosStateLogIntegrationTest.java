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

import static com.palantir.paxos.PaxosStateLogTestUtils.NAMESPACE;
import static com.palantir.paxos.PaxosStateLogTestUtils.generateRounds;
import static com.palantir.paxos.PaxosStateLogTestUtils.readRoundUnchecked;

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

public class FileToSqlitePaxosStateLogIntegrationTest {
    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    private PaxosStateLog<PaxosValue> source;
    private PaxosStateLog<PaxosValue> target;
    private SqlitePaxosStateLogMigrationState migrationState;

    @Before
    public void setup() throws IOException {
        source = new PaxosStateLogImpl<>(tempFolder.newFolder("source").getPath());
        Supplier<Connection> targetConnSupplier = SqliteConnections
                .createDefaultNamedSqliteDatabaseAtPath(tempFolder.newFolder("target").toPath());
        target = SqlitePaxosStateLog.createFactory().create(NAMESPACE, targetConnSupplier);
        migrationState = SqlitePaxosStateLogMigrationState.create(NAMESPACE, targetConnSupplier);
    }

    @Test
    public void emptyMigrationSucceeds() {
        migrate();
        assertThat(migrationState.hasMigratedFromInitialState()).isTrue();
    }

    @Test
    public void contiguousMigrationFromZeroSucceeds() {
        migrateAndVerifyValuesForSequences(LongStream.rangeClosed(0, 100));
    }

    @Test
    public void contiguousMigrationFromGreaterThanZeroSucceeds() {
        migrateAndVerifyValuesForSequences(LongStream.rangeClosed(50, 130));
    }

    @Test
    public void nonContiguousMigrationSucceeds() {
        migrateAndVerifyValuesForSequences(LongStream.iterate(0, x -> x + 1_301).limit(100));
    }

    private void migrateAndVerifyValuesForSequences(LongStream sequences) {
        List<PaxosRound<PaxosValue>> rounds = generateRounds(sequences);
        List<PaxosValue> expectedValues = roundsToValues(rounds);

        source.writeBatchOfRounds(rounds);

        migrate();
        List<PaxosValue> migratedValues = readMigratedValuesFor(expectedValues);

        assertThat(migratedValues).isEqualTo(roundsToValues(rounds));
        assertThat(migrationState.hasMigratedFromInitialState()).isTrue();
    }

    private List<PaxosValue> roundsToValues(List<PaxosRound<PaxosValue>> rounds) {
        return rounds.stream().map(PaxosRound::value).collect(Collectors.toList());
    }

    private void migrate() {
        PaxosStateLogMigrator.migrate(ImmutableMigrationContext.<PaxosValue>builder()
                .sourceLog(source)
                .destinationLog(target)
                .hydrator(PaxosValue.BYTES_HYDRATOR)
                .migrationState(migrationState)
                .build());
    }

    private List<PaxosValue> readMigratedValuesFor(List<PaxosValue> values) {
        return values.stream()
                .map(value -> PaxosValue.BYTES_HYDRATOR.hydrateFromBytes(readRoundUnchecked(target, value.seq)))
                .collect(Collectors.toList());
    }
}
