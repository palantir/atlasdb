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

public class KvsSweepQueueProgress {
    private static final TableReference TABLE_REF = TargetedSweepTableFactory.of()
            .getSweepShardProgressTable(null).getTableRef();
    private static final byte[] COL_BYTES = SweepShardProgressTable.SweepShardProgressNamedColumn.VALUE.getShortName();
    private static final int NUMBER_OF_SHARDS_INDEX = 1024;
    private static final long CAS_TIMESTAMP = 1L;

    private final KeyValueService kvs;

    public KvsSweepQueueProgress(KeyValueService kvs) {
        this.kvs = kvs;
    }

    public long numberOfShards() {
        return getOrInitializeTo(NUMBER_OF_SHARDS_INDEX, 1L);
    }

    public void updateNumberOfShards(int newNumber) {
        Preconditions.checkArgument(newNumber <= 128);
        increaseValue(NUMBER_OF_SHARDS_INDEX, newNumber);
    }

    public long lastSweptTimestamp(int shard) {
        return getOrInitializeTo(shard, 0L);
    }

    public void updateLastSweptTimestamp(int shard, long timestamp) {
        increaseValue(shard, timestamp);
    }

    private long getOrInitializeTo(int shard, long initialValue) {
        Map<Cell, Value> result = getEntry(shard);
        if (result.isEmpty()) {
            return initializeShard(shard, initialValue);
        }
        return getValue(result);
    }

    private Map<Cell, Value> getEntry(int shard) {
        return kvs.get(TABLE_REF, ImmutableMap.of(cellForShard(shard), CAS_TIMESTAMP));
    }

    private Cell cellForShard(int shard) {
        SweepShardProgressTable.SweepShardProgressRow row = SweepShardProgressTable.SweepShardProgressRow.of(shard);
        return Cell.create(row.persistToBytes(), COL_BYTES);
    }

    private long getValue(Map<Cell, Value> entry) {
        SweepShardProgressTable.Value value = SweepShardProgressTable.Value.BYTES_HYDRATOR.hydrateFromBytes(
                Iterables.getOnlyElement(entry.values()).getContents());
        return value.getValue();
    }

    private long initializeShard(int shard, long initialVale) {
        SweepShardProgressTable.Value colVal = SweepShardProgressTable.Value.of(initialVale);
        CheckAndSetRequest initializeRequest = CheckAndSetRequest
                .newCell(TABLE_REF, cellForShard(shard), colVal.persistValue());
        try {
            kvs.checkAndSet(initializeRequest);
            return initialVale;
        } catch (CheckAndSetException e) {
            return getValue(getEntry(shard));
        }
    }

    void increaseValue(int shard, long newValue){
        long oldVal = getValue(getEntry(shard));
        SweepShardProgressTable.Value colValNew = SweepShardProgressTable.Value.of(newValue);

        while (oldVal < newValue) {
            SweepShardProgressTable.Value colValOld = SweepShardProgressTable.Value.of(oldVal);
            CheckAndSetRequest casRequest = CheckAndSetRequest
                    .singleCell(TABLE_REF, cellForShard(shard), colValOld.persistValue(), colValNew.persistValue());
            try {
                kvs.checkAndSet(casRequest);
                return;
            } catch (CheckAndSetException e) {
                oldVal = getValue(getEntry(shard));
            }
        }
    }
}
