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

import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableMap;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.schema.generated.SweepableTimestampsTable;
import com.palantir.atlasdb.schema.generated.TargetedSweepTableFactory;

public class SweepableTimestampsWriter extends KvsSweepQueueWriter {
    private static final long TS_COARSE_GRANULARITY = 10_000_000L;
    private static final long TS_FINE_GRANULARITY = 50_000L;
    private static final byte[] DUMMY = new byte[0];

    private final SweepStrategyCache strategyCache;

    SweepableTimestampsWriter(KeyValueService kvs, TargetedSweepTableFactory tableFactory, SweepStrategyCache cache) {
        super(kvs, tableFactory.getSweepableTimestampsTable(null).getTableRef());
        this.strategyCache = cache;
    }

    @Override
    protected Map<Cell, byte[]> batchWrites(List<WriteInfo> writes) {
        ImmutableMap.Builder<Cell, byte[]> resultBuilder = ImmutableMap.builder();
        for (WriteInfo writeInfo : writes) {
            SweepableTimestampsTable.SweepableTimestampsRow row = SweepableTimestampsTable.SweepableTimestampsRow
                    .of(KvsSweepQueuePersister.getShard(writeInfo),
                            writeInfo.timestamp() / TS_COARSE_GRANULARITY,
                            strategyCache.getStrategy(writeInfo).toString());
            SweepableTimestampsTable.SweepableTimestampsColumn col = SweepableTimestampsTable.SweepableTimestampsColumn
                    .of(writeInfo.timestamp() % TS_COARSE_GRANULARITY / TS_FINE_GRANULARITY);

            SweepableTimestampsTable.SweepableTimestampsColumnValue colVal =
                    SweepableTimestampsTable.SweepableTimestampsColumnValue.of(col, DUMMY);

            resultBuilder.put(toCell(row, colVal), colVal.persistValue());
        }
        return resultBuilder.build();
    }
}
