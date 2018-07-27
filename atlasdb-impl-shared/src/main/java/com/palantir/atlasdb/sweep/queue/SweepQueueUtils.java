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

import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import com.google.common.base.Suppliers;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.ColumnRangeSelection;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.TargetedSweepMetadata;
import com.palantir.atlasdb.keyvalue.api.WriteReference;
import com.palantir.atlasdb.schema.generated.SweepableCellsTable;
import com.palantir.atlasdb.table.api.ColumnValue;
import com.palantir.common.persist.Persistable;

public final class SweepQueueUtils {
    public static final long REFRESH_TIME = TimeUnit.MINUTES.toMillis(5L);
    public static final long TS_COARSE_GRANULARITY = 10_000_000L;
    public static final long TS_FINE_GRANULARITY = 50_000L;
    public static final int MAX_CELLS_GENERIC = 50;
    public static final int MAX_CELLS_DEDICATED = 100_000;
    public static final int SWEEP_BATCH_SIZE = MAX_CELLS_DEDICATED;
    public static final int BATCH_SIZE_KVS = 1000;
    public static final long READ_TS = Long.MAX_VALUE;
    public static final long INITIAL_TIMESTAMP = -1L;
    public static final ColumnRangeSelection ALL_COLUMNS = allPossibleColumns();
    public static final int MINIMUM_WRITE_INDEX = -TargetedSweepMetadata.MAX_DEDICATED_ROWS;

    private SweepQueueUtils() {
        // utility
    }

    public static long tsPartitionCoarse(long timestamp) {
        return timestamp / TS_COARSE_GRANULARITY;
    }

    public static long tsPartitionFine(long timestamp) {
        return timestamp / TS_FINE_GRANULARITY;
    }

    public static long partitionFineToCoarse(long partitionFine) {
        return partitionFine / (TS_COARSE_GRANULARITY / TS_FINE_GRANULARITY);
    }

    public static long minTsForFinePartition(long finePartition) {
        return finePartition * TS_FINE_GRANULARITY;
    }

    public static long maxTsForFinePartition(long finePartition) {
        return minTsForFinePartition(finePartition) + TS_FINE_GRANULARITY - 1;
    }

    public static Cell toCell(Persistable row, ColumnValue<?> col) {
        return Cell.create(row.persistToBytes(), col.persistColumnName());
    }

    public static WriteInfo toWriteInfo(TableReference tableRef, Map.Entry<Cell, byte[]> write, long timestamp) {
        Cell cell = write.getKey();
        boolean isTombstone = Arrays.equals(write.getValue(), PtBytes.EMPTY_BYTE_ARRAY);
        return WriteInfo.of(WriteReference.of(tableRef, cell, isTombstone), timestamp);
    }

    private static ColumnRangeSelection allPossibleColumns() {
        byte[] startCol = SweepableCellsTable.SweepableCellsColumn.of(0L, MINIMUM_WRITE_INDEX)
                .persistToBytes();
        byte[] endCol = SweepableCellsTable.SweepableCellsColumn.of(TS_FINE_GRANULARITY, 0L)
                .persistToBytes();
        return new ColumnRangeSelection(startCol, endCol);
    }
}
