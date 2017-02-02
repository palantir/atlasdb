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

final class TimestampBoundStoreEntry {
    private final UUID id;
    private final long timestamp;

    TimestampBoundStoreEntry(UUID uuid, long timestamp) {
        this.id = uuid;
        this.timestamp = timestamp;
    }

    static TimestampBoundStoreEntry createFromBytes(byte[] values) {
        if (values.length > 8) {
            return new TimestampBoundStoreEntry((UUID) ValueType.UUID.convertToJava(values, 0),
                    PtBytes.toLong(values, ValueType.UUID.sizeOf(null)));
        } else {
            return new TimestampBoundStoreEntry(null, PtBytes.toLong(values));
        }
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

