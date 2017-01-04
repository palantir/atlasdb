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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.junit.Test;

import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.keyvalue.api.Cell;

public class LockEntryTest {
    @Test
    public void insertionMapContainsReason() {
        LockEntry lockEntry = ImmutableLockEntry.builder().lockId(1L).reason("test").build();

        Map<Cell, byte[]> insertionMap = lockEntry.insertionMap();

        Set<String> reasonsInMap = insertionMap.entrySet().stream()
                .filter(entry -> Arrays.equals(
                        entry.getKey().getColumnName(),
                        "reasonForLock".getBytes(StandardCharsets.UTF_8)))
                .map(entry -> new String(entry.getValue(), StandardCharsets.UTF_8))
                .collect(Collectors.toSet());

        assertThat(reasonsInMap, contains("test"));
    }

    @Test
    public void deletionMapContainsReason() {
        LockEntry lockEntry = ImmutableLockEntry.builder().lockId(1L).reason("test").build();

        Multimap<Cell, Long> deletionMap = lockEntry.deletionMap();
        Map.Entry<Cell, Long> entry = Iterables.getOnlyElement(deletionMap.entries());

        assertArrayEquals("reasonForLock".getBytes(StandardCharsets.UTF_8), entry.getKey().getColumnName());
        assertEquals(AtlasDbConstants.TRANSACTION_TS, (long) entry.getValue());
    }

}
