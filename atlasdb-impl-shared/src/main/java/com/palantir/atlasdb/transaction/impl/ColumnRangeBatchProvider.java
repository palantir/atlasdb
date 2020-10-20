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
package com.palantir.atlasdb.transaction.impl;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.palantir.atlasdb.keyvalue.api.BatchColumnRangeSelection;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.RangeRequests;
import com.palantir.atlasdb.keyvalue.api.RowColumnRangeIterator;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.common.base.ClosableIterator;
import com.palantir.common.base.ClosableIterators;
import com.palantir.logsafe.Preconditions;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

public class ColumnRangeBatchProvider implements BatchProvider<Map.Entry<Cell, Value>> {
    private final KeyValueService keyValueService;
    private final TableReference tableRef;
    private final byte[] row;
    private final BatchColumnRangeSelection columnRangeSelection;
    private final long timestamp;

    public ColumnRangeBatchProvider(
            KeyValueService keyValueService,
            TableReference tableRef,
            byte[] row,
            BatchColumnRangeSelection columnRangeSelection,
            long timestamp) {
        this.keyValueService = keyValueService;
        this.tableRef = tableRef;
        this.row = row;
        this.columnRangeSelection = columnRangeSelection;
        this.timestamp = timestamp;
    }

    @Override
    public ClosableIterator<Map.Entry<Cell, Value>> getBatch(int batchSize, @Nullable byte[] lastToken) {
        byte[] startCol = columnRangeSelection.getStartCol();
        if (lastToken != null) {
            startCol = RangeRequests.nextLexicographicName(lastToken);
        }
        BatchColumnRangeSelection newRange =
                BatchColumnRangeSelection.create(startCol, columnRangeSelection.getEndCol(), batchSize);
        Map<byte[], RowColumnRangeIterator> range =
                keyValueService.getRowsColumnRange(tableRef, ImmutableList.of(row), newRange, timestamp);
        if (range.isEmpty()) {
            return ClosableIterators.wrap(
                    ImmutableList.<Map.Entry<Cell, Value>>of().iterator());
        }
        return ClosableIterators.wrap(Iterables.getOnlyElement(range.values()));
    }

    @Override
    public boolean hasNext(byte[] lastToken) {
        if (RangeRequests.isLastRowName(lastToken)) {
            return false;
        } else {
            return !Arrays.equals(RangeRequests.nextLexicographicName(lastToken), columnRangeSelection.getEndCol());
        }
    }

    @Override
    public byte[] getLastToken(List<Map.Entry<Cell, Value>> batch) {
        Preconditions.checkArgument(!batch.isEmpty());
        return batch.get(batch.size() - 1).getKey().getColumnName();
    }
}
