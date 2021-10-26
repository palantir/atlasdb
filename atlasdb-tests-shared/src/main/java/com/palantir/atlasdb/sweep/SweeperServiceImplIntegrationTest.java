/*
 * (c) Copyright 2018 Palantir Technologies Inc. All rights reserved.
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
package com.palantir.atlasdb.sweep;

import static org.assertj.core.api.Assertions.assertThat;

import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.keyvalue.impl.InMemoryKeyValueService;
import com.palantir.atlasdb.protos.generated.TableMetadataPersistence.SweepStrategy;
import com.palantir.common.streams.KeyedStream;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.Before;
import org.junit.Test;

public class SweeperServiceImplIntegrationTest extends AbstractBackgroundSweeperIntegrationTest {
    private SweeperService sweeperService;

    @Before
    @Override
    public void setup() {
        super.setup();
        sweeperService = new SweeperServiceImpl(specificTableSweeper, sweepBatchConfigSource);
    }

    @Override
    @Test
    public void smokeTest() {
        createTable(TABLE_1, SweepStrategy.CONSERVATIVE);
        putManyCells(TABLE_1, 100, 110);
        putManyCells(TABLE_1, 103, 113);
        putManyCells(TABLE_1, 105, 115);
        sweeperService.sweepTableFully(TABLE_1.getQualifiedName());
        verifyTableSwept(TABLE_1, 75, true);
    }

    @Test
    public void previouslyConservativeErasesMostExistingSentinelsInThoroughTable() {
        skipCellVersion.setPeriod(Integer.MAX_VALUE);
        createTable(TABLE_1, SweepStrategy.THOROUGH);

        Map<Cell, byte[]> sentinelWrites = IntStream.range(0, 100)
                .mapToObj(PtBytes::toBytes)
                .map(bytes -> Cell.create(bytes, bytes))
                .collect(Collectors.toMap(cell -> cell, _ignore -> PtBytes.EMPTY_BYTE_ARRAY));
        kvs.put(TABLE_1, sentinelWrites, Value.INVALID_VALUE_TIMESTAMP);
        Map<Cell, Long> readMap = KeyedStream.stream(sentinelWrites)
                .map(_ignore -> Long.MAX_VALUE)
                .collectToMap();
        assertThat(kvs.get(TABLE_1, readMap)).hasSize(100);

        sweeperService.sweepPreviouslyConservativeNowThoroughTable(
                TABLE_1.getQualifiedName(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty());

        assertThat(kvs.get(TABLE_1, readMap).size()).isEqualTo(1);
    }

    @Test
    public void previouslyConservativeNoOpIfTableIsStillConservativelySwept() {
        createTable(TABLE_1, SweepStrategy.CONSERVATIVE);

        Map<Cell, byte[]> writes = KeyedStream.of(IntStream.range(0, 100).boxed())
                .mapKeys(PtBytes::toBytes)
                .mapKeys(bytes -> Cell.create(bytes, bytes))
                .map(PtBytes::toBytes)
                .collectToMap();
        kvs.put(TABLE_1, writes, 100L);
        txService.putUnlessExists(100L, 101L);
        kvs.put(TABLE_1, writes, 103L);
        txService.putUnlessExists(103L, 104L);
        Map<Cell, Long> readMap =
                KeyedStream.stream(writes).map(_ignore -> 105L).collectToMap();

        assertThat(kvs.get(TABLE_1, readMap)).hasSize(100);

        sweeperService.sweepPreviouslyConservativeNowThoroughTable(
                TABLE_1.getQualifiedName(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty());

        assertThat(kvs.get(TABLE_1, readMap).size()).isEqualTo(100);
    }

    /**
     * To help understand the test below, refer to the tables.
     * Before sweep
     * +----------+--------+-----------+
     * | START_TS | VALUES | DELETIONS |
     * +----------+--------+-----------+
     * |      100 |    500 |       500 |
     * |      103 |    500 |       500 |
     * +----------+--------+-----------+
     *
     * After sweep
     * +----------+--------+-----------+
     * | START_TS | VALUES | DELETIONS |
     * +----------+--------+-----------+
     * |      100 |      6 |         5 |
     * |      103 |    500 |     5 + 5 |
     * +----------+--------+-----------+
     */
    @Test
    public void previouslyConservativeRespectsSkipPeriodWhenSweepingNormally() {
        skipCellVersion.setPeriod(9);
        createTable(TABLE_1, SweepStrategy.THOROUGH);

        Map<Cell, byte[]> writes = KeyedStream.of(IntStream.range(0, 1000).boxed())
                .mapKeys(PtBytes::toBytes)
                .mapKeys(bytes -> Cell.create(bytes, bytes))
                .map(count -> count < 500 ? PtBytes.toBytes(count) : PtBytes.EMPTY_BYTE_ARRAY)
                .collectToMap();
        kvs.put(TABLE_1, writes, 100L);
        txService.putUnlessExists(100L, 101L);
        kvs.put(TABLE_1, writes, 103L);
        txService.putUnlessExists(103L, 104L);

        Map<Cell, Long> readMap =
                KeyedStream.stream(writes).map(_ignore -> 102L).collectToMap();
        assertThat(kvs.get(TABLE_1, readMap)).hasSize(1000);

        sweeperService.sweepPreviouslyConservativeNowThoroughTable(
                TABLE_1.getQualifiedName(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty());

        assertThat(kvs.get(TABLE_1, readMap))
                .as("deletes all but 11 entries at lower timestamp ~ 1%")
                .hasSize(11);
        Map<Cell, Long> readsAtMaxTs =
                KeyedStream.stream(readMap).map(_ignore -> Long.MAX_VALUE).collectToMap();
        Map<Cell, Value> latestVisibleVersions = kvs.get(TABLE_1, readsAtMaxTs);
        assertThat(latestVisibleVersions.values().stream()
                        .map(Value::getTimestamp)
                        .noneMatch(timestamp -> timestamp == 100L))
                .as("none of the entries at lower ts are naked")
                .isTrue();
        assertThat(latestVisibleVersions)
                .as("500 non-deletes, 5 deletes skipped normally, and 5 skipped to prevent revealing at lower ts")
                .hasSize(510);
    }

    @Test
    public void sweepPreviouslyConservativeNowThoroughTableFuzzTest() {
        skipCellVersion.makeNonDeterministic();
        createTable(TABLE_1, SweepStrategy.THOROUGH);

        Set<Cell> cells = IntStream.range(0, 10_000)
                .boxed()
                .map(PtBytes::toBytes)
                .map(bytes -> Cell.create(bytes, bytes))
                .collect(Collectors.toSet());

        for (int i = 0; i < 10; i++) {
            Map<Cell, byte[]> writes = KeyedStream.of(cells)
                    .map(cell ->
                            ThreadLocalRandom.current().nextBoolean() ? cell.getRowName() : PtBytes.EMPTY_BYTE_ARRAY)
                    .collectToMap();
            kvs.put(TABLE_1, writes, 100L + 2 * i);
            txService.putUnlessExists(100L + 2 * i, 101L + 2 * i);
        }

        Map<Cell, Long> readMap = KeyedStream.of(cells).map(_ignore -> 102L).collectToMap();
        assertThat(kvs.get(TABLE_1, readMap)).hasSize(10_000);

        sweeperService.sweepPreviouslyConservativeNowThoroughTable(
                TABLE_1.getQualifiedName(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty());

        assertThat(kvs.get(TABLE_1, readMap).size())
                .as("expected number of remaining entries is 1%, ~100")
                .isBetween(50, 200);

        Map<Cell, Long> readsAtMaxTs =
                KeyedStream.of(cells).map(_ignore -> Long.MAX_VALUE).collectToMap();
        Map<Cell, Value> latestVisibleVersions = kvs.get(TABLE_1, readsAtMaxTs);
        assertThat(latestVisibleVersions.values().stream()
                        .map(Value::getTimestamp)
                        .allMatch(ts -> ts == 100 + 2 * 9))
                .as("every visible cell still has the version at greatest timestamp")
                .isTrue();
        assertThat(latestVisibleVersions.size())
                .as("expected number of cells that are fully deleted is ~45.22%, ~5500 remaining")
                .isBetween(4500, 6500);
    }

    @Override
    protected KeyValueService getKeyValueService() {
        return new InMemoryKeyValueService(true);
    }
}
