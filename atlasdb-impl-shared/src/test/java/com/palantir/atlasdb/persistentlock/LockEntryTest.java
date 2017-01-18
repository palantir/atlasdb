/**
 * Copyright 2017 Palantir Technologies
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
package com.palantir.atlasdb.persistentlock;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.junit.Test;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.keyvalue.impl.InMemoryKeyValueService;

public class LockEntryTest {
    public static final String LOCK_ID = "12345";
    public static final String REASON = "test";
    private static final LockEntry LOCK_ENTRY = ImmutableLockEntry.builder()
            .rowName("row")
            .lockId(LOCK_ID)
            .reason(REASON)
            .build();
    public static final TableReference TEST_TABLE = TableReference.createWithEmptyNamespace("lockEntryTestTable");

    @Test
    public void insertionMapUsesRowName() {
        Map<Cell, byte[]> insertionMap = LOCK_ENTRY.insertionMap();

        Set<Cell> cellsWithWrongRowName = insertionMap.keySet().stream()
                .filter(cell -> !Arrays.equals(cell.getRowName(), "row".getBytes(StandardCharsets.UTF_8)))
                .collect(Collectors.toSet());

        assertThat(cellsWithWrongRowName, empty());
    }

    @Test
    public void insertionMapContainsSingleEntry() {
        assertEquals(1, LOCK_ENTRY.insertionMap().size());
    }

    @Test
    public void insertionMapContainsLockIdAndReason() {
        Map<Cell, byte[]> insertionMap = LOCK_ENTRY.insertionMap();

        Set<String> reasonsInMap = insertionMap.entrySet().stream()
                .filter(entry -> Arrays.equals(
                        entry.getKey().getColumnName(),
                        "lock".getBytes(StandardCharsets.UTF_8)))
                .map(entry -> new String(entry.getValue(), StandardCharsets.UTF_8))
                .collect(Collectors.toSet());

        assertThat(reasonsInMap, contains(LOCK_ID + "_" + REASON));
    }

    @Test
    public void fromRowResultProducesLockEntry() {
        KeyValueService kvs = new InMemoryKeyValueService(false);
        kvs.createTable(TEST_TABLE, AtlasDbConstants.GENERIC_TABLE_METADATA);
        kvs.putUnlessExists(TEST_TABLE, LOCK_ENTRY.insertionMap());

        Iterator<RowResult<Value>> range = kvs.getRange(TEST_TABLE,
                RangeRequest.all(),
                AtlasDbConstants.TRANSACTION_TS + 1);
        RowResult<Value> onlyEntry = Iterables.getOnlyElement(ImmutableSet.copyOf(range));

        LockEntry lockEntry = LockEntry.fromRowResult(onlyEntry);
        assertEquals(LOCK_ENTRY, lockEntry);
    }

    @Test
    public void deletionMapUsesRowName() {
        Multimap<Cell, Long> insertionMap = LOCK_ENTRY.deletionMap();

        Set<Cell> cellsWithWrongRowName = insertionMap.keySet().stream()
                .filter(cell -> !Arrays.equals(cell.getRowName(), "row".getBytes(StandardCharsets.UTF_8)))
                .collect(Collectors.toSet());

        assertThat(cellsWithWrongRowName, empty());
    }

    @Test
    public void deletionMapContainsLock() {
        Multimap<Cell, Long> deletionMap = LOCK_ENTRY.deletionMap();

        List<Map.Entry<Cell, Long>> matchingEntries = deletionMap.entries().stream()
                .filter(entry -> Arrays.equals(
                        entry.getKey().getColumnName(),
                        "lock".getBytes(StandardCharsets.UTF_8)))
                .collect(Collectors.toList());

        Map.Entry<Cell, Long> entry = Iterables.getOnlyElement(matchingEntries);

        assertArrayEquals("lock".getBytes(StandardCharsets.UTF_8), entry.getKey().getColumnName());
        assertEquals(AtlasDbConstants.TRANSACTION_TS, (long) entry.getValue());
    }

}
