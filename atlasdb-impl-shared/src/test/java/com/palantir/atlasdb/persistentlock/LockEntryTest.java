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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.junit.Test;

import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.keyvalue.api.Cell;

public class LockEntryTest {
    private static final LockEntry LOCK_ENTRY = ImmutableLockEntry.builder()
            .rowName("row")
            .lockId("12345")
            .reason("test")
            .build();

    @Test
    public void insertionMapUsesRowName() {
        Map<Cell, byte[]> insertionMap = LOCK_ENTRY.insertionMap();

        Set<Cell> cellsWithWrongRowName = insertionMap.keySet().stream()
                .filter(cell -> !Arrays.equals(cell.getRowName(), "row".getBytes(StandardCharsets.UTF_8)))
                .collect(Collectors.toSet());

        assertThat(cellsWithWrongRowName, empty());
    }

    @Test
    public void insertionMapContainsReason() {
        Map<Cell, byte[]> insertionMap = LOCK_ENTRY.insertionMap();

        Set<String> reasonsInMap = insertionMap.entrySet().stream()
                .filter(entry -> Arrays.equals(
                        entry.getKey().getColumnName(),
                        "reasonForLock".getBytes(StandardCharsets.UTF_8)))
                .map(entry -> new String(entry.getValue(), StandardCharsets.UTF_8))
                .collect(Collectors.toSet());

        assertThat(reasonsInMap, contains("test"));
    }

    @Test
    public void insertionMapContainsLockId() {
        Map<Cell, byte[]> insertionMap = LOCK_ENTRY.insertionMap();

        Set<byte[]> lockIdsInMap = insertionMap.entrySet().stream()
                .filter(entry -> Arrays.equals(
                        entry.getKey().getColumnName(),
                        "lockId".getBytes(StandardCharsets.UTF_8)))
                .map(Map.Entry::getValue)
                .collect(Collectors.toSet());

        assertThat(lockIdsInMap, contains("12345".getBytes(StandardCharsets.UTF_8)));
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
    public void deletionMapContainsReason() {
        Multimap<Cell, Long> deletionMap = LOCK_ENTRY.deletionMap();

        List<Map.Entry<Cell, Long>> matchingEntries = deletionMap.entries().stream()
                .filter(entry -> Arrays.equals(
                        entry.getKey().getColumnName(),
                        "reasonForLock".getBytes(StandardCharsets.UTF_8)))
                .collect(Collectors.toList());

        Map.Entry<Cell, Long> entry = Iterables.getOnlyElement(matchingEntries);

        assertArrayEquals("reasonForLock".getBytes(StandardCharsets.UTF_8), entry.getKey().getColumnName());
        assertEquals(AtlasDbConstants.TRANSACTION_TS, (long) entry.getValue());
    }

    @Test
    public void deletionMapContainsLockId() {
        Multimap<Cell, Long> deletionMap = LOCK_ENTRY.deletionMap();

        List<Map.Entry<Cell, Long>> matchingEntries = deletionMap.entries().stream()
                .filter(entry -> Arrays.equals(
                        entry.getKey().getColumnName(),
                        "lockId".getBytes(StandardCharsets.UTF_8)))
                .collect(Collectors.toList());

        Map.Entry<Cell, Long> entry = Iterables.getOnlyElement(matchingEntries);

        assertArrayEquals("lockId".getBytes(StandardCharsets.UTF_8), entry.getKey().getColumnName());
        assertEquals(AtlasDbConstants.TRANSACTION_TS, (long) entry.getValue());
    }

}
