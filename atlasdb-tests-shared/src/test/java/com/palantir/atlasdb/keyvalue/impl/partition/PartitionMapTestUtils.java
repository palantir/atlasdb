/**
 * Copyright 2015 Palantir Technologies
 *
 * Licensed under the BSD-3 License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://opensource.org/licenses/BSD-3-Clause
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.palantir.atlasdb.keyvalue.impl.partition;

import static org.junit.Assert.assertEquals;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.collect.Maps;
import com.google.common.primitives.UnsignedBytes;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.partition.api.PartitionMap;
import com.palantir.common.collect.Maps2;
import com.palantir.util.Pair;

public class PartitionMapTestUtils {

    private final PartitionMap partitionMap;
    private final String tableName;

    public PartitionMapTestUtils(PartitionMap partitionMap, String tableName) {
        this.partitionMap = partitionMap;
        this.tableName = tableName;
    }

    public void testRows(Map<KeyValueService, Set<byte[]>> expected, Collection<byte[]> rows) {
        final Map<KeyValueService, Set<byte[]>> result = Maps.newHashMap();
        partitionMap.runForRowsRead(tableName, rows, new Function<Pair<KeyValueService,Iterable<byte[]>>, Void>() {
            @Override
            public Void apply(Pair<KeyValueService, Iterable<byte[]>> input) {
                result.put(input.lhSide, ImmutableSortedSet
                                .<byte[]> orderedBy(UnsignedBytes.lexicographicalComparator())
                                .addAll(input.rhSide).build());
                return null;
            }
        });
        assertEquals(expected, result);
    }

    public void testCellsRead(Map<KeyValueService, Set<Cell>> expected, Set<Cell> cells) {
        final Map<KeyValueService, Set<Cell>> result = Maps.newHashMap();
        partitionMap.runForCellsRead(tableName, cells, new Function<Pair<KeyValueService, Set<Cell>>, Void>() {
            @Override
            public Void apply(Pair<KeyValueService, Set<Cell>> input) {
                result.put(input.lhSide, input.rhSide);
                return null;
            }
        });
        assertEquals(expected, result);
    }

    protected void testCellsRead(Set<KeyValueService> expected, Cell cell) {
        Map<KeyValueService, Set<Cell>> expectedMap = Maps2
                .<KeyValueService, Set<Cell>> createConstantValueMap(expected, ImmutableSet.of(cell));
        testCellsRead(expectedMap, ImmutableSet.of(cell));
    }

    protected void testCellsWrite(Map<KeyValueService, Set<Cell>> expected, Set<Cell> cells) {
        final Map<KeyValueService, Set<Cell>> result = Maps.newHashMap();
        partitionMap.runForCellsWrite(tableName, cells, new Function<Pair<KeyValueService, Set<Cell>>, Void>() {
            @Override
            public Void apply(Pair<KeyValueService, Set<Cell>> input) {
                result.put(input.lhSide, input.rhSide);
                return null;
            }
        });
        assertEquals(expected, result);
    }

    protected void testCellsWrite(Set<KeyValueService> expected, Cell cell) {
        Map<KeyValueService, Set<Cell>> expectedMap = Maps2
                .<KeyValueService, Set<Cell>> createConstantValueMap(expected, ImmutableSet.of(cell));
        testCellsWrite(expectedMap, ImmutableSet.of(cell));
    }

}
