/**
 * Copyright 2016 Palantir Technologies
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
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

import java.nio.charset.StandardCharsets;
import java.util.Set;
import java.util.stream.Collectors;

import org.junit.Test;

import com.google.common.collect.Multimap;
import com.palantir.atlasdb.keyvalue.api.Cell;

public class LockEntryShould {
    private static final long LOCK_ID = 1234;
    private static final String REASON = "exclusiveStr";
    private static final boolean EXCLUSIVE = false;

    private final LockEntry lockEntry = LockEntry.of(PersistentLockName.of("lockName"), LOCK_ID, REASON, EXCLUSIVE);
    private final byte[] rowBytes = "lockName_1234".getBytes(StandardCharsets.UTF_8);

    @Test
    public void insertionMapContainsCorrectRow() {
        Set<String> rowNames = lockEntry.insertionMap().keySet().stream()
                .map(Cell::getRowName)
                .map(bytes -> new String(bytes, StandardCharsets.UTF_8))
                .collect(Collectors.toSet());

        assertThat(rowNames, contains("lockName_1234"));
    }

    @Test
    public void insertionMapContainsReason() {
        byte[] reasonBytes = lockEntry.insertionMap().get(getReasonKey());

        assertThat(new String(reasonBytes, StandardCharsets.UTF_8), equalTo(REASON));
    }

    @Test
    public void insertionMapContainsExclusiveFlag() {
        byte[] exclusiveBytes = lockEntry.insertionMap().get(getExclusiveKey());

        boolean exclusiveFlag = Boolean.parseBoolean(new String(exclusiveBytes, StandardCharsets.UTF_8));
        assertThat(exclusiveFlag, equalTo(EXCLUSIVE));
    }

    @Test
    public void deletionMapContainsTimestamps() {
        long timestamp = 4321;

        Multimap<Cell, Long> deletionMap = lockEntry.deletionMapWithTimestamp(timestamp);

        assertThat(deletionMap.get(getReasonKey()), contains(timestamp));
        assertThat(deletionMap.get(getExclusiveKey()), contains(timestamp));
    }

    private Cell getReasonKey() {
        return Cell.create(
                rowBytes,
                LockEntry.REASON_FOR_LOCK_COLUMN.getBytes(StandardCharsets.UTF_8));
    }

    private Cell getExclusiveKey() {
        return Cell.create(
                    rowBytes,
                    LockEntry.EXCLUSIVE_COLUMN.getBytes(StandardCharsets.UTF_8));
    }
}
