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

import java.util.Arrays;
import java.util.Optional;
import java.util.UUID;

import org.apache.cassandra.thrift.CASResult;
import org.apache.cassandra.thrift.Column;
import org.immutables.value.Value;

import com.palantir.timestamp.MultipleRunningTimestampServiceError;

@Value.Immutable
public abstract class EntryPair {
    abstract TimestampBoundStoreEntry noId();
    abstract TimestampBoundStoreEntry withId();

    public static EntryPair create(TimestampBoundStoreEntry withoutId, TimestampBoundStoreEntry withId) {
        return ImmutableEntryPair.builder()
                .noId(withoutId)
                .withId(withId)
                .build();
    }

    public static EntryPair createForTimestampAndId(long ts, UUID id) {
        return create(TimestampBoundStoreEntry.create(ts, null), TimestampBoundStoreEntry.create(ts, id));
    }

    public static EntryPair createFromCasResult(CASResult result) {
        if (result.getCurrent_values() == null || result.getCurrent_values().size() != 2) {
            throw new MultipleRunningTimestampServiceError("Unexpected number of entries in the timestamp bound DB: "
                    + result.getCurrent_values().size() + ". Expected number of entries is 2."
                    + " This may indicate another timestamp service is running against this cassandra keyspace.");
        }
        Column firstColumn = result.getCurrent_values().get(0);
        Column secondColumn = result.getCurrent_values().get(1);
        EntryPair entries;
        if (noIdColumn(firstColumn) && withIdColumn(secondColumn)) {
            entries = createFromColumns(firstColumn, secondColumn);
        } else {
            entries = createFromColumns(secondColumn, firstColumn);
        }
        if (entries.noId().id().isPresent()) {
            throw new MultipleRunningTimestampServiceError("The legacy format timestamp bound entry in the DB has ID "
                    + entries.noId().getIdAsString() + " when it should have none."
                    + " This may indicate another timestamp service is running against this cassandra keyspace.");
        }
        entries.checkTimestampsMatch();
        return entries;
    }

    public static EntryPair createFromColumns(Column columnNoId, Column columnWithId) {
        return create(TimestampBoundStoreEntry.createFromColumn(Optional.of(columnNoId)),
                TimestampBoundStoreEntry.createFromColumn(Optional.of(columnWithId)));
    }

    public void checkTimestampsMatch() {
        if (noId().getTimestampOrInitialValue() != withId().getTimestampOrInitialValue()) {
            CassandraTimestampUtils.throwGetTimestampMismatchError(this);
        }
    }

    private static boolean noIdColumn(Column column) {
        return Arrays.equals(column.getName(), CassandraTimestampBoundStore.NO_ID_TIMESTAMP_ARRAY);
    }

    private static boolean withIdColumn(Column column) {
        return Arrays.equals(column.getName(), CassandraTimestampBoundStore.WITH_ID_TIMESTAMP_ARRAY);
    }

    public long maxTimestamp() {
        return Long.max(noId().getTimestampOrInitialValue(), withId().getTimestampOrInitialValue());
    }

    public long getTimestamp() {
        checkTimestampsMatch();
        return noId().getTimestampOrInitialValue();
    }
}
