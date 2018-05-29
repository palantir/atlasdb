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

import java.util.Collection;
import java.util.Map;
import java.util.stream.Collectors;

import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.sweep.Sweeper;

public class SweepQueueDeleter {
    private final KeyValueService kvs;

    SweepQueueDeleter(KeyValueService kvs) {
        this.kvs = kvs;
    }

    /**
     * Executes targeted sweep, by inserting ranged tombstones corresponding to the given writes, using the sweep
     * strategy determined by the sweeper.
     *
     * @param writes individual writes to sweep for. Depending on the strategy, we will insert a ranged tombstone for
     * each write at either the write's timestamp - 1, or at its timestamp.
     * @param sweeper supplies the strategy-specific behaviour: the timestamp for the tombstone and whether we must use
     * sentinels or not.
     */
    public void sweep(Collection<WriteInfo> writes, Sweeper sweeper) {
        Map<TableReference, Map<Cell, Long>> maxTimestampByCell = writesPerTable(writes, sweeper);
        for (Map.Entry<TableReference, Map<Cell, Long>> entry: maxTimestampByCell.entrySet()) {
            if (sweeper.shouldAddSentinels()) {
                kvs.addGarbageCollectionSentinelValues(entry.getKey(), entry.getValue().keySet());
                kvs.deleteAllTimestamps(entry.getKey(), entry.getValue());
            } else {
                kvs.deleteAllTimestampsIncludingSentinels(entry.getKey(), entry.getValue());
            }
        }
    }

    private Map<TableReference, Map<Cell, Long>> writesPerTable(Collection<WriteInfo> writes, Sweeper sweeper) {
        return writes.stream().collect(Collectors.groupingBy(
                WriteInfo::tableRef,
                Collectors.toMap(WriteInfo::cell, write -> write.timestampToDeleteAtExclusive(sweeper))));
    }
}
