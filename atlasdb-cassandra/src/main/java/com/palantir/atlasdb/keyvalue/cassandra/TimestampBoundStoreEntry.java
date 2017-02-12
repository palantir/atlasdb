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
import org.immutables.value.Value;

import com.google.common.collect.Iterables;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.table.description.ValueType;

@Value.Immutable
abstract class TimestampBoundStoreEntry {
    abstract Optional<UUID> id();
    abstract Optional<Long> timestamp();

    private static final int SIZE_OF_ID_IN_BYTES = ValueType.UUID.sizeOf(null);
    private static final int SIZE_WITHOUT_ID_IN_BYTES = Long.BYTES;
    private static final int SIZE_WITH_ID_IN_BYTES = SIZE_OF_ID_IN_BYTES + SIZE_WITHOUT_ID_IN_BYTES;
    private static final long INITIAL_VALUE = 10000L;


    static TimestampBoundStoreEntry create(Long timestamp, UUID id) {
        return ImmutableTimestampBoundStoreEntry.builder()
                .id(Optional.ofNullable(id))
                .timestamp(Optional.ofNullable(timestamp))
                .build();
    }

    static TimestampBoundStoreEntry createFromBytes(byte[] values) {
        if (values.length == SIZE_WITH_ID_IN_BYTES) {
            return create(PtBytes.toLong(values),
                    (UUID) ValueType.UUID.convertToJava(values, SIZE_WITHOUT_ID_IN_BYTES));
        } else if (values.length == SIZE_WITHOUT_ID_IN_BYTES) {
            return create(PtBytes.toLong(values), null);
        }
        throw new IllegalArgumentException("Unsupported format: required " + SIZE_WITH_ID_IN_BYTES + " or "
                + SIZE_WITHOUT_ID_IN_BYTES + " bytes, but has " + values.length + "!");
    }

    static TimestampBoundStoreEntry createFromColumn(Optional<Column> column) {
        return column.map(Column::getValue).map(TimestampBoundStoreEntry::createFromBytes).orElse(create(null, null));
    }

    static TimestampBoundStoreEntry createFromCasResult(CASResult result) {
        return createFromColumn(Optional.ofNullable(Iterables.getOnlyElement(result.getCurrent_values(), null)));
    }

    static byte[] getByteValueForIdAndBound(UUID id, Long ts) {
        return (create(ts, id)).getByteValue();
    }

    byte[] getByteValue() {
        return ArrayUtils.addAll(timestamp().map(PtBytes::toBytes).orElse(null),
                id().map(ValueType.UUID::convertFromJava).orElse(null));
    }

    boolean idMatches(UUID otherId) {
        return id().map(otherId::equals).orElse(false);
    }

    long getTimestamp() {
        return timestamp().orElseGet(() -> INITIAL_VALUE);
    }

    String getTimestampAsString() {
        return timestamp().map(ts -> Long.toString(ts)).orElse("none");
    }

    String getIdAsString() {
        return id().map(UUID::toString).orElse("none");
    }
}
