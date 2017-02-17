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

import java.util.Optional;
import java.util.UUID;

import org.apache.cassandra.thrift.CASResult;
import org.apache.cassandra.thrift.Column;
import org.apache.commons.lang3.ArrayUtils;

import com.google.common.collect.Iterables;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.table.description.ValueType;
import com.palantir.timestamp.TimestampBoundStoreEntry;

public class CassandraTimestampBoundStoreEntry extends TimestampBoundStoreEntry {
    private static final int SIZE_OF_ID_IN_BYTES = ValueType.UUID.sizeOf(null);
    private static final int SIZE_WITHOUT_ID_IN_BYTES = Long.BYTES;
    private static final int SIZE_WITH_ID_IN_BYTES = SIZE_OF_ID_IN_BYTES + SIZE_WITHOUT_ID_IN_BYTES;

    protected CassandraTimestampBoundStoreEntry(Long timestamp, UUID id) {
        super(timestamp, id);
    }

    public static CassandraTimestampBoundStoreEntry create(Long timestamp, UUID id) {
        return new CassandraTimestampBoundStoreEntry(timestamp, id);
    }

    public static CassandraTimestampBoundStoreEntry createFromBytes(byte[] values) {
        if (values.length == SIZE_WITH_ID_IN_BYTES) {
            return create(PtBytes.toLong(values),
                    (UUID) ValueType.UUID.convertToJava(values, SIZE_WITHOUT_ID_IN_BYTES));
        } else if (values.length == SIZE_WITHOUT_ID_IN_BYTES) {
            return create(PtBytes.toLong(values), null);
        }
        throw new IllegalArgumentException("Unsupported format: required " + SIZE_WITH_ID_IN_BYTES + " or "
                + SIZE_WITHOUT_ID_IN_BYTES + " bytes, but has " + values.length + "!");
    }

    public static CassandraTimestampBoundStoreEntry createFromCasResult(CASResult result) {
        if (result.getCurrent_values() == null) {
            return null;
        }
        return createFromColumn(Optional.ofNullable(Iterables.getOnlyElement(result.getCurrent_values(), null)));
    }

    public static CassandraTimestampBoundStoreEntry createFromColumn(Optional<Column> column) {
        return column.map(Column::getValue).map(CassandraTimestampBoundStoreEntry::createFromBytes)
                .orElse(create(null, null));
    }

    public static CassandraTimestampBoundStoreEntry createFromSuper(TimestampBoundStoreEntry entry) {
        return CassandraTimestampBoundStoreEntry.create(entry.timestamp.orElse(null), entry.id.orElse(null));
    }

    public static byte[] getByteValueForBoundAndId(Long ts, UUID id) {
        return (create(ts, id)).getByteValue();
    }

    public byte[] getByteValue() {
        return ArrayUtils.addAll(timestamp.map(PtBytes::toBytes).orElse(null),
                id.map(ValueType.UUID::convertFromJava).orElse(null));
    }
}
