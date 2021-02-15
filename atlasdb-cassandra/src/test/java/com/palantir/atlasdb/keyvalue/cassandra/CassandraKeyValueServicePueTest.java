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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.palantir.atlasdb.keyvalue.api.Cell;
import java.util.Map;
import java.util.stream.Collectors;
import okio.ByteString;
import org.junit.Test;

public class CassandraKeyValueServicePueTest {
    @Test
    public void multipleEntriesFromSameRowAreBatchedTogether() {
        Map<Cell, byte[]> entries = ImmutableMap.of(
                Cell.create(getBytes(0), getBytes(0)), getBytes(0),
                Cell.create(getBytes(0), getBytes(1)), getBytes(1),
                Cell.create(getBytes(0), getBytes(2)), getBytes(1));
        Map<ByteString, Map<Cell, byte[]>> result = CassandraKeyValueServiceImpl.partitionPerRow(entries);

        assertThat(result).hasSize(1);
        assertThat(Iterables.getOnlyElement(result.values())).containsAllEntriesOf(entries);
        assertThat(entries).containsAllEntriesOf(Iterables.getOnlyElement(result.values()));
    }

    @Test
    public void entriesFromDifferentRowsAreNotBatchedTogether() {
        Map<Cell, byte[]> entries = ImmutableMap.of(
                Cell.create(getBytes(0), getBytes(0)), getBytes(0),
                Cell.create(getBytes(1), getBytes(1)), getBytes(1),
                Cell.create(getBytes(2), getBytes(2)), getBytes(1));
        Map<ByteString, Map<Cell, byte[]>> result = CassandraKeyValueServiceImpl.partitionPerRow(entries);

        assertThat(result).hasSize(3);
        Map<Cell, byte[]> resultEntries = result.values().stream()
                .flatMap(x -> x.entrySet().stream())
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        assertThat(resultEntries).containsAllEntriesOf(entries);
        assertThat(entries).containsAllEntriesOf(resultEntries);
    }

    @Test
    public void entriesFromSameRowAreBatchedTogetherButNotWithElementsFromDifferentRows() {
        Map<Cell, byte[]> entries = ImmutableMap.of(
                Cell.create(getBytes(0), getBytes(0)), getBytes(0),
                Cell.create(getBytes(1), getBytes(1)), getBytes(1),
                Cell.create(getBytes(0), getBytes(2)), getBytes(2),
                Cell.create(getBytes(1), getBytes(3)), getBytes(3),
                Cell.create(getBytes(2), getBytes(2)), getBytes(1));
        Map<ByteString, Map<Cell, byte[]>> result = CassandraKeyValueServiceImpl.partitionPerRow(entries);

        assertThat(result).hasSize(3);

        assertThat(getTransformedMapFor(result, 0))
                .containsExactly(
                        Maps.immutableEntry(Cell.create(getBytes(0), getBytes(0)), ByteString.of(getBytes(0))),
                        Maps.immutableEntry(Cell.create(getBytes(0), getBytes(2)), ByteString.of(getBytes(2))));

        assertThat(getTransformedMapFor(result, 1))
                .containsExactly(
                        Maps.immutableEntry(Cell.create(getBytes(1), getBytes(1)), ByteString.of(getBytes(1))),
                        Maps.immutableEntry(Cell.create(getBytes(1), getBytes(3)), ByteString.of(getBytes(3))));

        assertThat(getTransformedMapFor(result, 2))
                .containsExactly(
                        Maps.immutableEntry(Cell.create(getBytes(2), getBytes(2)), ByteString.of(getBytes(1))));
    }

    private Map<Cell, ByteString> getTransformedMapFor(Map<ByteString, Map<Cell, byte[]>> result, int row) {
        return Maps.transformValues(result.get(ByteString.of(getBytes(row))), ByteString::of);
    }

    private static byte[] getBytes(int num) {
        return new byte[] {(byte) num};
    }
}
