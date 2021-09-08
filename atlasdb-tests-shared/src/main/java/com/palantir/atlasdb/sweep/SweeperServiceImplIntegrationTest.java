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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.keyvalue.impl.InMemoryKeyValueService;
import com.palantir.atlasdb.protos.generated.TableMetadataPersistence.SweepStrategy;
import com.palantir.common.streams.KeyedStream;
import java.util.Map;
import java.util.Optional;
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
    public void previouslyConservativeSweepsEverythingWhenNothingIsSkipped() {
        skipCellVersion.setPeriod(Integer.MAX_VALUE);
        createTable(TABLE_1, SweepStrategy.THOROUGH);
        putManyCells(TABLE_1, 100, 101);
        putManyCells(TABLE_1, 103, 104);
        putManyCells(TABLE_1, 107, 109);

        sweeperService.sweepPreviouslyConservativeNowThoroughTable(
                TABLE_1.getQualifiedName(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty());

        verifyTableSwept(TABLE_1, 58, false);
    }

    @Test
    public void previouslyConservativeErasesExistingSentinelsInThoroughTable() {
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

        assertThat(kvs.get(TABLE_1, readMap)).isEmpty();
    }

    @Test
    public void previouslyConservativeThrowsIfTableIsStillConservativelySwept() {
        createTable(TABLE_1, SweepStrategy.CONSERVATIVE);

        assertThatThrownBy(() -> sweeperService.sweepPreviouslyConservativeNowThoroughTable(
                        TABLE_1.getQualifiedName(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("it is not safe to run this type of sweep on conservatively swept tables");
    }

    @Test
    public void previouslyConservativeRespectsSkipPeriodWhenErasingSentinels() {
        skipCellVersion.setPeriod(4);
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

        // Impl specific, documenting here -- exact last row will be swept twice, so the sentinel will be erased on
        // second pass-through; this is fine, not worth the risk of modifying behaviour
        assertThat(kvs.get(TABLE_1, readMap)).hasSize(24);
    }

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

        // deletes all but a ninth of the entries at the lower timestamp
        assertThat(kvs.get(TABLE_1, readMap)).hasSize(111);
        Map<Cell, Long> readsAtMaxTs =
                KeyedStream.stream(readMap).map(_ignore -> Long.MAX_VALUE).collectToMap();
        // none of the cells at lower timestamp became visible
        assertThat(kvs.get(TABLE_1, readsAtMaxTs).values().stream()
                        .map(Value::getTimestamp)
                        .filter(timestamp -> timestamp == 100L)
                        .collect(Collectors.toList()))
                .hasSize(0);
        // 500 cannot be completely removed because they are not deletes, 111 of the remaining 500 have either of the
        // two versions skipped from sweep, which results in a version here
        assertThat(kvs.get(TABLE_1, readsAtMaxTs)).hasSize(500 + 111);
    }

    @Override
    protected KeyValueService getKeyValueService() {
        return new InMemoryKeyValueService(true);
    }
}
