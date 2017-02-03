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
import org.immutables.value.Value;

import com.google.common.collect.Iterables;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.table.description.ValueType;

@Value.Immutable
abstract class TimestampBoundStoreEntry {
    abstract UUID id();
    abstract long timestamp();

    private static final int SIZE_OF_ID_IN_BYTES = ValueType.UUID.sizeOf(null);
    private static final int SIZE_WITHOUT_ID_IN_BYTES = Long.BYTES;
    private static final int SIZE_WITH_ID_IN_BYTES = SIZE_OF_ID_IN_BYTES + SIZE_WITHOUT_ID_IN_BYTES;

    static TimestampBoundStoreEntry create(UUID id, long timestamp) {
        return ImmutableTimestampBoundStoreEntry.builder()
                .id(id)
                .timestamp(timestamp)
                .build();
    }

    static TimestampBoundStoreEntry createFromBytes(byte[] values) {
        if (values.length == SIZE_WITH_ID_IN_BYTES) {
            return create((UUID) ValueType.UUID.convertToJava(values, SIZE_WITHOUT_ID_IN_BYTES),
                    PtBytes.toLong(values));
        } else if (values.length == SIZE_WITHOUT_ID_IN_BYTES) {
            return create(null, PtBytes.toLong(values));
        }
        throw new IllegalArgumentException("Unsupported format: required " + SIZE_WITH_ID_IN_BYTES + " or "
                + SIZE_WITHOUT_ID_IN_BYTES + " bytes, but has " + values.length + "!");
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
        return (create(id, ts)).getByteValue();
    }

    byte[] getByteValue() {
        if (!hasId()) {
            return PtBytes.toBytes(timestamp());
        }
        return ArrayUtils.addAll(PtBytes.toBytes(timestamp()), ValueType.UUID.convertFromJava(id()));
    }

    long getTimestamp() {
        return timestamp();
    }

    boolean hasId() {
        return id() != null;
    }

    UUID getId() {
        return id();
    }
}

