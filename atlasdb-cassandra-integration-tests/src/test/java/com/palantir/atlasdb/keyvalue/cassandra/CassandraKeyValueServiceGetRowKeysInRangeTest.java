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

package com.palantir.atlasdb.keyvalue.cassandra;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.ByteString;
import com.palantir.atlasdb.containers.CassandraResource;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.TimestampRangeDelete;
import com.palantir.atlasdb.protos.generated.TableMetadataPersistence;
import com.palantir.atlasdb.table.description.TableMetadata;
import com.palantir.common.random.RandomBytes;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

public class CassandraKeyValueServiceGetRowKeysInRangeTest {
    private static final TableReference GET_ROW_KEYS_TABLE = TableReference.createFromFullyQualifiedName("test.rows");
    private static final byte[] NO_SWEEP_METADATA = TableMetadata.builder()
            .sweepStrategy(TableMetadataPersistence.SweepStrategy.NOTHING)
            .build()
            .persistToBytes();

    private CassandraKeyValueService kvs;

    @ClassRule
    public static final CassandraResource CASSANDRA = new CassandraResource();

    @Before
    public void initialize() {
        kvs = CASSANDRA.getDefaultKvs();
        kvs.createTable(GET_ROW_KEYS_TABLE, NO_SWEEP_METADATA);
    }

    @After
    public void cleanup() {
        kvs.truncateTable(GET_ROW_KEYS_TABLE);
    }

    @Test
    public void getRowKeysInRangeReturnsEmptyOnEmptyTable() {
        List<byte[]> result = kvs.getRowKeysInRange(GET_ROW_KEYS_TABLE, new byte[0], new byte[0], 100);
        assertListsMatch(result, ImmutableList.of());
    }

    @Test
    public void getRowKeysInRangeRespectsInclusiveLowerBound() {
        List<Cell> cells = createAndWriteCells(10, 1);
        List<byte[]> sortedRows = sortedUniqueRowKeys(cells);

        List<byte[]> result = kvs.getRowKeysInRange(GET_ROW_KEYS_TABLE, sortedRows.get(3), new byte[0], 100);
        assertListsMatch(result, sortedRows.subList(3, 10));
    }

    @Test
    public void getRowKeysInRangeRespectsExclusiveUpperBound() {
        List<Cell> cells = createAndWriteCells(10, 1);
        List<byte[]> sortedRows = sortedUniqueRowKeys(cells);

        List<byte[]> result = kvs.getRowKeysInRange(GET_ROW_KEYS_TABLE, new byte[0], sortedRows.get(7), 100);
        assertListsMatch(result, sortedRows.subList(0, 8));
    }

    @Test
    public void getRowKeysInRangeRespectsMaxSize() {
        List<Cell> cells = createAndWriteCells(10, 1);
        List<byte[]> sortedRows = sortedUniqueRowKeys(cells);

        List<byte[]> result = kvs.getRowKeysInRange(GET_ROW_KEYS_TABLE, sortedRows.get(1), sortedRows.get(7), 3);
        assertListsMatch(result, sortedRows.subList(1, 4));
    }

    @Test
    public void getRowKeysInRangeDoesNotReturnDuplicateKeysInPresenceOfMultipleColumns() {
        List<Cell> cells = createAndWriteCells(30, 5);
        List<byte[]> sortedRows = sortedUniqueRowKeys(cells);

        List<byte[]> result = kvs.getRowKeysInRange(GET_ROW_KEYS_TABLE, new byte[0], new byte[0], 10);
        assertListsMatch(result, sortedRows.subList(0, 10));
    }

    @Test
    public void getRowKeysInRangeReturnsTombstonedRows() {
        List<Cell> cells = createAndWriteCells(10, 2);
        List<byte[]> sortedRows = sortedUniqueRowKeys(cells);

        kvs.deleteAllTimestamps(
                GET_ROW_KEYS_TABLE,
                ImmutableMap.of(
                        cells.get(1),
                        new TimestampRangeDelete.Builder()
                                .timestamp(Long.MAX_VALUE)
                                .endInclusive(false)
                                .deleteSentinels(true)
                                .build()));
        List<byte[]> result = kvs.getRowKeysInRange(GET_ROW_KEYS_TABLE, new byte[0], new byte[0], 10);
        assertListsMatch(result, sortedRows);
    }

    private List<Cell> createAndWriteCells(int numRows, int colsPerRow) {
        List<Cell> cells = createCells(numRows, colsPerRow);

        cells.forEach(cell -> kvs.put(GET_ROW_KEYS_TABLE, ImmutableMap.of(cell, cell.getColumnName()), getTimestamp()));

        return cells;
    }

    private static long getTimestamp() {
        return ThreadLocalRandom.current().nextLong(0, Long.MAX_VALUE);
    }

    private static List<Cell> createCells(int numRows, int colsPerRow) {
        return IntStream.generate(() -> colsPerRow)
                .limit(numRows)
                .boxed()
                .flatMap(CassandraKeyValueServiceGetRowKeysInRangeTest::createRandomCellsInSameRow)
                .collect(Collectors.toList());
    }

    private static Stream<Cell> createRandomCellsInSameRow(int number) {
        byte[] row = RandomBytes.ofLength(10);
        return IntStream.range(0, number)
                .mapToObj(ignore -> RandomBytes.ofLength(10))
                .map(col -> Cell.create(row, col));
    }

    private static List<byte[]> sortedUniqueRowKeys(List<Cell> cells) {
        return cells.stream()
                .map(Cell::getRowName)
                .map(ByteString::copyFrom)
                .distinct()
                .sorted(ByteString.unsignedLexicographicalComparator())
                .map(ByteString::toByteArray)
                .collect(Collectors.toList());
    }

    private static void assertListsMatch(List<byte[]> result, List<byte[]> expected) {
        assertThat(result).hasSameSizeAs(expected);
        for (int i = 0; i < result.size(); i++) {
            assertThat(expected.get(i)).isEqualTo(result.get(i));
        }
    }
}
