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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.nio.charset.StandardCharsets;
import java.util.Iterator;

import org.junit.Test;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.CheckAndSetRequest;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.keyvalue.impl.InMemoryKeyValueService;

public class LockEntryTest {
    private static final String ROW = "row";
    private static final String LOCK_ID = "12345";
    private static final String REASON = "test";

    private static final byte[] ROW_BYTES = asUtf8Bytes(ROW);
    private static final byte[] LOCK_BYTES = asUtf8Bytes(LockEntry.LOCK_COLUMN);

    private static final LockEntry LOCK_ENTRY = ImmutableLockEntry.builder()
            .rowName("row")
            .lockId(LOCK_ID)
            .reason(REASON)
            .build();
    private static final TableReference TEST_TABLE = TableReference.createWithEmptyNamespace("lockEntryTestTable");

    @Test
    public void cellContainsRowAndColumn() {
        Cell cell = LOCK_ENTRY.cell();
        assertArrayEquals(ROW_BYTES, cell.getRowName());
        assertArrayEquals(LOCK_BYTES, cell.getColumnName());
    }

    @Test
    public void valueContainsLockIdAndReason() {
        String lockAndReason = LOCK_ID + "_" + REASON;
        byte[] expectedValue = asUtf8Bytes(lockAndReason);
        byte[] value = LOCK_ENTRY.value();

        assertArrayEquals(expectedValue, value);
    }

    @Test
    public void fromRowResultProducesLockEntry() {
        KeyValueService kvs = new InMemoryKeyValueService(false);
        kvs.createTable(TEST_TABLE, AtlasDbConstants.GENERIC_TABLE_METADATA);
        kvs.checkAndSet(CheckAndSetRequest.newCell(TEST_TABLE, LOCK_ENTRY.cell(), LOCK_ENTRY.value()));

        Iterator<RowResult<Value>> range = kvs.getRange(TEST_TABLE,
                RangeRequest.all(),
                AtlasDbConstants.TRANSACTION_TS + 1);
        RowResult<Value> onlyEntry = Iterables.getOnlyElement(ImmutableSet.copyOf(range));

        LockEntry lockEntry = LockEntry.fromRowResult(onlyEntry);
        assertEquals(LOCK_ENTRY, lockEntry);
    }

    private static byte[] asUtf8Bytes(String lockAndReason) {
        return lockAndReason.getBytes(StandardCharsets.UTF_8);
    }
}
