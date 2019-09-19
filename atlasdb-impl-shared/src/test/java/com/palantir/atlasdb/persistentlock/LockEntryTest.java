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
package com.palantir.atlasdb.persistentlock;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
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
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.UUID;
import org.junit.Test;

public class LockEntryTest {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static final String ROW = "row";
    private static final UUID LOCK_ID = UUID.fromString("1-1-2-3-5");
    private static final String REASON = "test";

    private static final byte[] ROW_BYTES = asUtf8Bytes(ROW);
    private static final byte[] LOCK_BYTES = asUtf8Bytes(LockEntry.LOCK_COLUMN);

    private static final LockEntry LOCK_ENTRY = ImmutableLockEntry.builder()
            .lockName("row")
            .instanceId(LOCK_ID)
            .reason(REASON)
            .build();
    private static final String JSON_LOCK_SERIALIZATION = "{\"lockName\":\"row\","
                    + "\"instanceId\":\"00000001-0001-0002-0003-000000000005\","
                    + "\"reason\":\"test\"}";
    private static final TableReference TEST_TABLE = TableReference.createWithEmptyNamespace("lockEntryTestTable");

    @Test
    public void testSerialisation() throws IOException {
        LockEntry deserialisedLockEntry = MAPPER.readValue(MAPPER.writeValueAsString(LOCK_ENTRY), LockEntry.class);

        assertEquals(LOCK_ENTRY.lockName(), deserialisedLockEntry.lockName());
        assertEquals(LOCK_ENTRY.instanceId(), deserialisedLockEntry.instanceId());
        assertEquals(LOCK_ENTRY.reason(), deserialisedLockEntry.reason());
    }

    @Test
    public void cellContainsRowAndColumn() {
        Cell cell = LOCK_ENTRY.cell();
        assertArrayEquals(ROW_BYTES, cell.getRowName());
        assertArrayEquals(LOCK_BYTES, cell.getColumnName());
    }

    @Test
    public void valueIsSerialisedLockEntry() throws JsonProcessingException {
        String serialisedLockEntry = MAPPER.writeValueAsString(LOCK_ENTRY);
        byte[] expectedValue = asUtf8Bytes(serialisedLockEntry);
        byte[] value = LOCK_ENTRY.value();

        String msg = String.format("Expected: %s%nActual: %s",
                new String(expectedValue, StandardCharsets.UTF_8),
                new String(value, StandardCharsets.UTF_8));
        assertArrayEquals(msg, expectedValue, value);
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

    @Test
    public void fromStoredValueProducesLockEntry() throws JsonProcessingException {
        byte[] value = asUtf8Bytes(MAPPER.writeValueAsString(LOCK_ENTRY));
        LockEntry actual = LockEntry.fromStoredValue(value);
        assertEquals(LOCK_ENTRY, actual);
    }

    @Test
    public void confirmJsonOnDiskBackCompatMaintainedDeserialization() {
        assertEquals(LOCK_ENTRY, LockEntry.fromStoredValue(asUtf8Bytes(JSON_LOCK_SERIALIZATION)));
    }

    @Test
    public void confirmJsonOnDiskBackCompatMaintainedSerialization() {
        assertEquals(JSON_LOCK_SERIALIZATION, new String(LOCK_ENTRY.value(), StandardCharsets.UTF_8));
    }

    private static byte[] asUtf8Bytes(String lockAndReason) {
        return lockAndReason.getBytes(StandardCharsets.UTF_8);
    }
}
