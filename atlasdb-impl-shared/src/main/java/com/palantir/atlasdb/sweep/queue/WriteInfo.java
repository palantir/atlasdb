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
package com.palantir.atlasdb.sweep.queue;

import com.google.common.math.IntMath;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.TimestampRangeDelete;
import com.palantir.atlasdb.keyvalue.api.WriteReference;
import com.palantir.atlasdb.sweep.Sweeper;
import org.immutables.value.Value;

/**
 * Contains information about a committed write, for use by the sweep queue.
 */
@Value.Immutable
public interface WriteInfo {
    long timestamp();
    WriteReference writeRef();

    default TableReference tableRef() {
        return writeRef().tableRef();
    }

    default Cell cell() {
        return writeRef().cell();
    }

    default TimestampRangeDelete toDelete(Sweeper sweeper) {
        return new TimestampRangeDelete.Builder()
                .timestamp(timestamp())
                .endInclusive(writeRef().isTombstone() && sweeper.shouldSweepLastCommitted())
                .deleteSentinels(!sweeper.shouldAddSentinels())
                .build();
    }

    default int toShard(int numShards) {
        return IntMath.mod(writeRef().cellReference().hashCode(), numShards);
    }

    static WriteInfo of(WriteReference writeRef, long timestamp) {
        return ImmutableWriteInfo.builder()
                .writeRef(writeRef)
                .timestamp(timestamp)
                .build();
    }

    static WriteInfo tombstone(TableReference tableRef, Cell cell, long timestamp) {
        return WriteInfo.of(WriteReference.of(tableRef, cell, true), timestamp);
    }

    static WriteInfo write(TableReference tableRef, Cell cell, long timestamp) {
        return WriteInfo.of(WriteReference.of(tableRef, cell, false), timestamp);
    }

    static WriteInfo higherTimestamp(WriteInfo fst, WriteInfo snd) {
        return fst.timestamp() > snd.timestamp() ? fst : snd;
    }
}
