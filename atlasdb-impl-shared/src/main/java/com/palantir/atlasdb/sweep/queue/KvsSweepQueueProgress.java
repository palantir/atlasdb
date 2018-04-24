/*
 * Copyright 2018 Palantir Technologies, Inc. All rights reserved.
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

package com.palantir.atlasdb.sweep.queue;

import java.util.Map;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.CheckAndSetException;
import com.palantir.atlasdb.keyvalue.api.CheckAndSetRequest;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.schema.generated.SweepShardProgressTable;
import com.palantir.atlasdb.schema.generated.TargetedSweepTableFactory;
import com.palantir.util.PersistableBoolean;

public class KvsSweepQueueProgress {
    private static final TableReference TABLE_REF = TargetedSweepTableFactory.of()
            .getSweepShardProgressTable(null).getTableRef();
    private static final int NUMBER_OF_SHARDS_INDEX = 1024;

    private final KeyValueService kvs;

    public KvsSweepQueueProgress(KeyValueService kvs) {
        this.kvs = kvs;
    }

    public long getNumberOfShards() {
        return getOrReturnInitial(NUMBER_OF_SHARDS_INDEX, true, 1L);
    }

    public void updateNumberOfShards(int newNumber) {
        Preconditions.checkArgument(newNumber <= 128);
        increaseValue(NUMBER_OF_SHARDS_INDEX, true, newNumber);
    }

    public long getLastSweptTimestampPartition(int shard, boolean conservative) {
        return getOrReturnInitial(shard, conservative, -1L);
    }

    public void updateLastSweptTimestampPartition(int shard, boolean conservative, long timestamp) {
        increaseValue(shard, conservative, timestamp);
    }

    private long getOrReturnInitial(int shard, boolean conservative, long initialValue) {
        Map<Cell, Value> result = getEntry(shard, conservative);
        if (result.isEmpty()) {
            return initialValue;
        }
        return getValue(result);
    }

    private Map<Cell, Value> getEntry(int shard, boolean conservative) {
        return kvs.get(TABLE_REF, ImmutableMap.of(cellForShard(shard, conservative), SweepQueueUtils.CAS_TS));
    }

    private Cell cellForShard(int shard, boolean conservative) {
        SweepShardProgressTable.SweepShardProgressRow row = SweepShardProgressTable.SweepShardProgressRow.of(
                shard, PersistableBoolean.of(conservative).persistToBytes());
        return Cell.create(row.persistToBytes(),
                SweepShardProgressTable.SweepShardProgressNamedColumn.VALUE.getShortName());
    }

    private long getValue(Map<Cell, Value> entry) {
        SweepShardProgressTable.Value value = SweepShardProgressTable.Value.BYTES_HYDRATOR.hydrateFromBytes(
                Iterables.getOnlyElement(entry.values()).getContents());
        return value.getValue();
    }

    private void increaseValue(int shard, boolean conservative, long newValue) {
        long oldVal = getLastSweptTimestampPartition(shard, conservative);
        byte[] colValNew = SweepShardProgressTable.Value.of(newValue).persistValue();

        // todo(gmaretic): is the potential infinite loop here scary? Should we code defensively and fail if oldVal remains constant?
        while (oldVal < newValue) {
            CheckAndSetRequest casRequest = createRequest(shard, conservative, oldVal, colValNew);
            try {
                kvs.checkAndSet(casRequest);
                return;
            } catch (CheckAndSetException e) {
                oldVal = getValue(getEntry(shard, conservative));
            }
        }
    }

    private CheckAndSetRequest createRequest(int shard, boolean conservative, long oldVal, byte[] colValNew) {
        if (oldVal == -1) {
            return CheckAndSetRequest.newCell(TABLE_REF, cellForShard(shard, conservative), colValNew);
        }
        byte[] colValOld = SweepShardProgressTable.Value.of(oldVal).persistValue();
        return CheckAndSetRequest.singleCell(TABLE_REF, cellForShard(shard, conservative), colValOld, colValNew);
    }
}
