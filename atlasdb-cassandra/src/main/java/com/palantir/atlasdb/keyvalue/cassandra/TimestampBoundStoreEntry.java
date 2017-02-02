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
package com.palantir.atlasdb.keyvalue.cassandra;

import java.util.UUID;

import org.apache.cassandra.thrift.CASResult;
import org.apache.cassandra.thrift.Column;
import org.apache.commons.lang3.ArrayUtils;

import com.google.common.collect.Iterables;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.table.description.ValueType;
import com.palantir.common.annotation.Immutable;

@Immutable
final class TimestampBoundStoreEntry {
    private final UUID id;
    private final long timestamp;

    private static final int sizeOfIdInBytes = ValueType.UUID.sizeOf(null);
    private static final int sizeWithoutIdInBytes = Long.BYTES;
    private static final int sizeWithIdInBytes = sizeOfIdInBytes + sizeWithoutIdInBytes;

    TimestampBoundStoreEntry(UUID uuid, long timestamp) {
        this.id = uuid;
        this.timestamp = timestamp;
    }

    static TimestampBoundStoreEntry createFromBytes(byte[] values) {
        if (values.length == sizeWithIdInBytes) {
            return new TimestampBoundStoreEntry((UUID) ValueType.UUID.convertToJava(values, 0),
                    PtBytes.toLong(values, sizeOfIdInBytes));
        } else if (values.length == sizeWithoutIdInBytes) {
            return new TimestampBoundStoreEntry(null, PtBytes.toLong(values));
        }
        throw new IllegalArgumentException("Unsupported format: required " + sizeWithIdInBytes + " or "
                + sizeWithoutIdInBytes + " bytes, but has " + values.length + "!");
    }

    static TimestampBoundStoreEntry createFromColumn(Column column) {
        return createFromBytes(column.getValue());
    }

    static TimestampBoundStoreEntry createFromCasResult(CASResult result) {
        return createFromColumn(Iterables.getOnlyElement(result.getCurrent_values()));
    }

    static byte[] getByteValueForIdAndBound(UUID id, Long ts) {
        if (ts == null) {
            return null;
        }
        return (new TimestampBoundStoreEntry(id, ts)).getByteValue();
    }

    byte[] getByteValue() {
        if (!hasId()) {
            return PtBytes.toBytes(timestamp);
        }
        return ArrayUtils.addAll(ValueType.UUID.convertFromJava(id), PtBytes.toBytes(timestamp));
    }

    long getTimestamp() {
        return timestamp;
    }

    boolean hasId() {
        return id != null;
    }

    UUID getId() {
        return id;
    }
}

