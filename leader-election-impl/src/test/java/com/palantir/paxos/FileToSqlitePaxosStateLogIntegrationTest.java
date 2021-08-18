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

import static com.palantir.paxos.PaxosStateLogTestUtils.NAMESPACE;
import static com.palantir.paxos.PaxosStateLogTestUtils.generateRounds;
import static com.palantir.paxos.PaxosStateLogTestUtils.readRoundUnchecked;
import static org.assertj.core.api.Assertions.assertThat;

import com.palantir.common.streams.KeyedStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import javax.sql.DataSource;
import org.assertj.core.api.AbstractAssert;
import org.assertj.core.api.Assertions;
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
        DataSource targetSource = SqliteConnections.getDefaultConfiguredPooledDataSource(
                tempFolder.newFolder("target").toPath());
        target = SqlitePaxosStateLog.create(NAMESPACE, targetSource);
        migrationState = SqlitePaxosStateLogMigrationState.create(NAMESPACE, targetSource);
    }

    @Test
    public void emptyMigrationSucceeds() {
        migrate();
        assertThat(migrationState.hasMigratedFromInitialState()).isTrue();
    }

    @Test
    public void migrationSucceedsWhenGreatestEntrySmallerThanSafetyBuffer() {
        migrateAndVerifyValuesForSequences(LongStream.rangeClosed(0, PaxosStateLogMigrator.SAFETY_BUFFER - 10));
    }

    @Test
    public void migrationSucceedsWhenGreatestEntrySmallerThanSafetyBufferAndNotFromZero() {
        migrateAndVerifyValuesForSequences(LongStream.rangeClosed(10, PaxosStateLogMigrator.SAFETY_BUFFER - 10));
    }

    @Test
    public void migrationForContiguousEntriesFromZeroSucceeds() {
        migrateAndVerifyValuesForSequences(LongStream.rangeClosed(0, 100));
    }

    @Test
    public void migrationForContiguousEntriesFromGreaterThanZeroSucceeds() {
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
        long cutoff = Math.max(
                PaxosAcceptor.NO_LOG_ENTRY, source.getGreatestLogEntry() - PaxosStateLogMigrator.SAFETY_BUFFER);
        Map<Long, byte[]> targetEntries = readMigratedValuesFor(expectedValues);

        targetEntries.entrySet().stream()
                .filter(entry -> entry.getKey() < cutoff)
                .map(Map.Entry::getValue)
                .map(Assertions::assertThat)
                .forEach(AbstractAssert::isNull);
        KeyedStream.stream(targetEntries)
                .filterKeys(sequence -> sequence >= cutoff)
                .mapKeys(PaxosStateLogTestUtils::valueForRound)
                .mapKeys(PaxosValue::persistToBytes)
                .forEach((fst, snd) -> assertThat(fst).containsExactly(snd));
        assertThat(migrationState.hasMigratedFromInitialState()).isTrue();
    }

    private List<PaxosValue> roundsToValues(List<PaxosRound<PaxosValue>> rounds) {
        return rounds.stream().map(PaxosRound::value).collect(Collectors.toList());
    }

    private void migrate() {
        PaxosStateLogMigrator.migrateAndReturnCutoff(ImmutableMigrationContext.<PaxosValue>builder()
                .sourceLog(source)
                .destinationLog(target)
                .hydrator(PaxosValue.BYTES_HYDRATOR)
                .migrationState(migrationState)
                .namespaceAndUseCase(ImmutableNamespaceAndUseCase.of(Client.of("client"), "UseCase"))
                .skipValidationAndTruncateSourceIfMigrated(false)
                .build());
    }

    private Map<Long, byte[]> readMigratedValuesFor(List<PaxosValue> values) {
        return KeyedStream.of(values)
                .mapKeys(value -> value.seq)
                .map(value -> readRoundUnchecked(target, value.seq))
                .collectToMap();
    }
}
