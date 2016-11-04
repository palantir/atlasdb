/**
 * Copyright 2015 Palantir Technologies
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

package com.palantir.atlasdb.keyvalue.dbkvs.impl;

import java.util.ArrayList;
import java.util.List;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

public final class RowsColumnRangeBatchRequests {
    private RowsColumnRangeBatchRequests() {
        // Utility class
    }

    public static List<byte[]> getAllRowsInOrder(RowsColumnRangeBatchRequest batch) {
        List<byte[]> allRows = new ArrayList<>(batch.getRowsToLoadFully().size() + 2);
        if (batch.getPartialFirstRow().isPresent()) {
            allRows.add(batch.getPartialFirstRow().get().getKey());
        }
        allRows.addAll(batch.getRowsToLoadFully());
        if (batch.getPartialLastRow().isPresent()) {
            allRows.add(batch.getPartialLastRow().get().getKey());
        }
        return allRows;
    }

    /**
     * Partitions the given {@link RowsColumnRangeBatchRequest} into multiple, preserving the ordering of rows. Each
     * partitioned {@link RowsColumnRangeBatchRequest} will have exactly {@code partitionSize} rows in total, except
     * possibly for the last one, which may have fewer rows (but not more). No row will be split across partitions.
     */
    public static List<RowsColumnRangeBatchRequest> partition(RowsColumnRangeBatchRequest batch, int partitionSize) {
        if (getAllRowsInOrder(batch).size() <= partitionSize) {
            return ImmutableList.of(batch);
        }

        List<List<byte[]>> partitionedRows = Lists.partition(getAllRowsInOrder(batch), partitionSize);
        List<RowsColumnRangeBatchRequest> partitions = new ArrayList<>(partitionedRows.size());
        for (int partitionNumber = 0; partitionNumber < partitionedRows.size(); partitionNumber++) {
            List<byte[]> allRowsInPartition = partitionedRows.get(partitionNumber);
            boolean partitionHasPartialFirstRow = partitionNumber == 0 && batch.getPartialFirstRow().isPresent();
            boolean partitionHasPartialLastRow =
                    partitionNumber == partitionedRows.size() - 1 && batch.getPartialLastRow().isPresent();

            ImmutableRowsColumnRangeBatchRequest.Builder partition = ImmutableRowsColumnRangeBatchRequest.builder();
            if (partitionHasPartialFirstRow) {
                partition = partition.partialFirstRow(batch.getPartialFirstRow());
            }
            List<byte[]> rowsToFullyLoad =
                    getRowsToFullyLoad(allRowsInPartition, partitionHasPartialFirstRow, partitionHasPartialLastRow);
            if (!rowsToFullyLoad.isEmpty()) {
                partition = partition.columnRangeSelection(batch.getColumnRangeSelection())
                        .rowsToLoadFully(rowsToFullyLoad);
            }
            if (partitionHasPartialLastRow) {
                partition = partition.partialLastRow(batch.getPartialLastRow());
            }

            partitions.add(partition.build());
        }
        return partitions;
    }

    private static List<byte[]> getRowsToFullyLoad(List<byte[]> rows, boolean partialFirstRow, boolean partialLastRow) {
        int fromIndex = partialFirstRow ? 1 : 0;
        int toIndex = partialLastRow ? rows.size() - 1 : rows.size();
        return rows.subList(fromIndex, toIndex);
    }
}
