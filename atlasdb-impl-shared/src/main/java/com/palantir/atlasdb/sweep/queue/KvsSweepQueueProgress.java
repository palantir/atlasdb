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
    private static final int SHARD_COUNT_INDEX = -1;
    private static final long CAS_TIMESTAMP = 1L;
    public static final long INITIAL_SHARDS = 1L;
    public static final long INITIAL_TIMESTAMP = 0L;

    private final KeyValueService kvs;

    public KvsSweepQueueProgress(KeyValueService kvs) {
        this.kvs = kvs;
    }

    public long numberOfShards() {
        return getOrInitializeTo(ShardAndStrategy.conservative(SHARD_COUNT_INDEX), INITIAL_SHARDS);
    }

    public void updateNumberOfShards(int newNumber) {
        Preconditions.checkArgument(newNumber <= 128);
        increaseValueToAtLeast(ShardAndStrategy.conservative(SHARD_COUNT_INDEX), newNumber);
    }

    public long lastSweptTimestamp(ShardAndStrategy shardAndStrategy) {
        return getOrInitializeTo(shardAndStrategy, INITIAL_TIMESTAMP);
    }

    public void updateLastSweptTimestamp(ShardAndStrategy shardAndStrategy, long timestamp) {
        increaseValueToAtLeast(shardAndStrategy, timestamp);
    }

    private long getOrInitializeTo(ShardAndStrategy shardAndStrategy, long initialValue) {
        Map<Cell, Value> result = getEntry(shardAndStrategy);
        if (result.isEmpty()) {
            return initializeShard(shardAndStrategy, initialValue);
        }
        return getValue(result);
    }

    private Map<Cell, Value> getEntry(ShardAndStrategy shardAndStrategy) {
        return kvs.get(TABLE_REF, ImmutableMap.of(cellForShard(shardAndStrategy), CAS_TIMESTAMP));
    }

    private Cell cellForShard(ShardAndStrategy shardAndStrategy) {
        SweepShardProgressTable.SweepShardProgressRow row = SweepShardProgressTable.SweepShardProgressRow.of(
                shardAndStrategy.shard(),
                PersistableBoolean.of(shardAndStrategy.isConservative()).persistToBytes());
        return Cell.create(row.persistToBytes(),
                SweepShardProgressTable.SweepShardProgressNamedColumn.VALUE.getShortName());
    }

    private long getValue(Map<Cell, Value> entry) {
        SweepShardProgressTable.Value value = SweepShardProgressTable.Value.BYTES_HYDRATOR.hydrateFromBytes(
                Iterables.getOnlyElement(entry.values()).getContents());
        return value.getValue();
    }

    private long initializeShard(ShardAndStrategy shardAndStrategy, long initialValue) {
        SweepShardProgressTable.Value colVal = SweepShardProgressTable.Value.of(initialValue);
        CheckAndSetRequest initializeRequest = CheckAndSetRequest
                .newCell(TABLE_REF, cellForShard(shardAndStrategy), colVal.persistValue());
        try {
            kvs.checkAndSet(initializeRequest);
            return initialValue;
        } catch (CheckAndSetException e) {
            return getValue(getEntry(shardAndStrategy));
        }
    }

    private long increaseValueToAtLeast(ShardAndStrategy shardAndStrategy, long newValue) {
        long oldVal = getValue(getEntry(shardAndStrategy));
        SweepShardProgressTable.Value colValNew = SweepShardProgressTable.Value.of(newValue);

        while (oldVal < newValue) {
            SweepShardProgressTable.Value colValOld = SweepShardProgressTable.Value.of(oldVal);
            CheckAndSetRequest casRequest = CheckAndSetRequest.singleCell(
                    TABLE_REF, cellForShard(shardAndStrategy), colValOld.persistValue(), colValNew.persistValue());
            try {
                kvs.checkAndSet(casRequest);
                return newValue;
            } catch (CheckAndSetException e) {
                oldVal = getValue(getEntry(shardAndStrategy));
            }
        }
        return  oldVal;
    }
}
