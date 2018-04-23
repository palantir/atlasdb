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

import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.primitives.UnsignedBytes;
import com.palantir.atlasdb.keyvalue.api.ColumnSelection;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.RangeRequests;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.schema.generated.SweepableTimestampsTable;
import com.palantir.atlasdb.schema.generated.TargetedSweepTableFactory;
import com.palantir.common.base.ClosableIterator;
import com.palantir.util.PersistableBoolean;

public class SweepableTimestampsReader {
    private final KeyValueService kvs;
    private final TableReference tableRef;

    SweepableTimestampsReader(KeyValueService kvs, TargetedSweepTableFactory factory) {
        this.kvs = kvs;
        this.tableRef = factory.getSweepableTimestampsTable(null).getTableRef();
    }

    Set<SweepableTimestampsTable.SweepableTimestampsColumnValue> getSweptColumns(long shard, long tsBucket, boolean conservative) {
        return getColumns(shard, tsBucket, conservative, -1L);
    }

    Set<SweepableTimestampsTable.SweepableTimestampsColumnValue> getColumns(long shard, long tsBucket, boolean conservative) {
        return getColumns(shard, tsBucket, conservative, 0L);
    }


        Set<SweepableTimestampsTable.SweepableTimestampsColumnValue> getColumns(long shard, long tsBucket, boolean conservative, long ts) {
        byte[] row = SweepableTimestampsTable.SweepableTimestampsRow.of(
                shard, tsBucket, PersistableBoolean.of(conservative).persistToBytes())
                .persistToBytes();
        RangeRequest rangeRequest = RangeRequest.builder()
                .startRowInclusive(row)
                .endRowExclusive(RangeRequests.nextLexicographicName(row))
                .retainColumns(ColumnSelection.all())
                .batchHint(1)
                .build();
        ClosableIterator<RowResult<Value>> rowResultIterator = kvs.getRange(tableRef, rangeRequest, ts + 1L);

        if (!rowResultIterator.hasNext()) {
            return ImmutableSet.of();
        }

        RowResult<Value> rowResult = rowResultIterator.next();

        Preconditions.checkState(!rowResultIterator.hasNext(), "WAT");

        return SweepableTimestampsTable.SweepableTimestampsRowResult.of(RowResult.create(rowResult.getRowName(),
                ImmutableSortedMap.<byte[], byte[]>orderedBy(UnsignedBytes.lexicographicalComparator()).putAll(rowResult.getColumns().entrySet().stream().collect(Collectors.toMap(entry -> entry.getKey(), entry -> entry.getValue().getContents()))).build())).getColumnValues();
    }
}
