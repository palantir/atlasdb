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

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.SortedMap;

import org.immutables.value.Value;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.annotations.VisibleForTesting;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.common.base.Throwables;

@JsonSerialize(as = ImmutableLockEntry.class)
@JsonDeserialize(as = ImmutableLockEntry.class)
@Value.Immutable
public abstract class LockEntry {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    @VisibleForTesting
    static final String LOCK_COLUMN = "lock";

    public abstract String rowName();
    public abstract String lockId();
    public abstract String reason();

    static LockEntry fromRowResult(RowResult<com.palantir.atlasdb.keyvalue.api.Value> rowResult) {
        return deserialize(valueOfColumnInRow(LOCK_COLUMN, rowResult));
    }

    static LockEntry fromStoredValue(byte[] value) {
        return deserialize(asString(value));
    }

    private static LockEntry deserialize(String serializedEntry) {
        try {
            return MAPPER.readValue(serializedEntry, LockEntry.class);
        } catch (IOException e) {
            String msg = String.format("The stored lock entry could not be read; found %s", serializedEntry);
            throw new IllegalStateException(msg, e);
        }
    }

    public Cell cell() {
        return Cell.create(asUtf8Bytes(rowName()), asUtf8Bytes(LOCK_COLUMN));
    }

    public byte[] value() {
        try {
            return asUtf8Bytes(MAPPER.writeValueAsString(this));
        } catch (JsonProcessingException e) {
            throw Throwables.throwUncheckedException(e);
        }
    }

    private static String valueOfColumnInRow(
            String columnName,
            RowResult<com.palantir.atlasdb.keyvalue.api.Value> rowResult) {
        byte[] columnNameBytes = asUtf8Bytes(columnName);
        SortedMap<byte[], com.palantir.atlasdb.keyvalue.api.Value> columns = rowResult.getColumns();
        if (columns.containsKey(columnNameBytes)) {
            byte[] contents = columns.get(columnNameBytes).getContents();
            return asString(contents);
        } else {
            throw new IllegalStateException(String.format("Couldn't find column %s in the %s table!",
                    LOCK_COLUMN,
                    AtlasDbConstants.PERSISTED_LOCKS_TABLE));
        }
    }

    private static byte[] asUtf8Bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private static String asString(byte[] value) {
        return new String(value, StandardCharsets.UTF_8);
    }
}
