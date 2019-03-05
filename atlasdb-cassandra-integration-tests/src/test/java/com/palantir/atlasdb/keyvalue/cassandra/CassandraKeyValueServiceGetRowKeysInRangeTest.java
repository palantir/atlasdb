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

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertThat;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.containers.CassandraResource;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.protos.generated.TableMetadataPersistence;
import com.palantir.atlasdb.table.description.TableMetadata;
import com.palantir.common.random.RandomBytes;

import okio.ByteString;

public class CassandraKeyValueServiceGetRowKeysInRangeTest {
    private static final TableReference GET_ROW_KEYS_TABLE = TableReference.createFromFullyQualifiedName("test.rows");
    private static final byte[] NO_SWEEP_METADATA = TableMetadata.builder()
            .sweepStrategy(TableMetadataPersistence.SweepStrategy.NOTHING).build().persistToBytes();

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
    public void getRowKeysInRangeTest() {
        List<Cell> cells = createCells(50, 3);

        kvs.multiPut(ImmutableMap.of(GET_ROW_KEYS_TABLE,
                cells.stream().collect(Collectors.toMap(cell -> cell, Cell::getRowName))), 100L);

        List<byte[]> sortedRows = sortedUniqueRowKeys(cells);

        List<byte[]> result = kvs.getRowKeysInRange(GET_ROW_KEYS_TABLE, sortedRows.get(20), 10);
        List<byte[]> expected = sortedRows.subList(20, 30);

        assertThat(result.size(), is(expected.size()));
        for (int i = 0; i < result.size(); i++) {
            assertArrayEquals(result.get(i), expected.get(i));
        }
    }

    @Test
    public void getRowKeysInRangeReturnsTombstonedRows() {
        List<Cell> cells = createCells(10, 1);

        kvs.multiPut(ImmutableMap.of(GET_ROW_KEYS_TABLE,
                cells.stream().collect(Collectors.toMap(cell -> cell, Cell::getRowName))), 100L);

        kvs.delete(GET_ROW_KEYS_TABLE, ImmutableListMultimap.of(cells.get(0), 100L));
        kvs.deleteAllTimestamps(GET_ROW_KEYS_TABLE, ImmutableMap.of(cells.get(1), Long.MAX_VALUE), true);

        List<byte[]> sortedRows = sortedUniqueRowKeys(cells);

        List<byte[]> result = kvs.getRowKeysInRange(GET_ROW_KEYS_TABLE, sortedRows.get(0), 10);

        assertThat(result.size(), is(sortedRows.size()));
        for (int i = 0; i < result.size(); i++) {
            assertArrayEquals(result.get(i), sortedRows.get(i));
        }
    }

    private static List<Cell> createCells(int numRows, int colsPerRow) {
        return IntStream.generate(() -> colsPerRow).limit(numRows).boxed()
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
                .map(ByteString::of)
                .distinct()
                .sorted()
                .map(ByteString::toByteArray)
                .collect(Collectors.toList());
    }
}
