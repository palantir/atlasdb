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
        if (batch.hasPartialFirstRow()) {
            allRows.add(batch.getPartialFirstRow().getKey());
        }
        allRows.addAll(batch.getRowsToLoadFully());
        if (batch.hasPartialLastRow()) {
            allRows.add(batch.getPartialLastRow().getKey());
        }
        return allRows;
    }

    public static List<RowsColumnRangeBatchRequest> partition(RowsColumnRangeBatchRequest batch, int partitionSize) {
        if (getAllRowsInOrder(batch).size() <= partitionSize) {
            return ImmutableList.of(batch);
        }

        List<List<byte[]>> partitionedRows = Lists.partition(getAllRowsInOrder(batch), partitionSize);
        List<RowsColumnRangeBatchRequest> partitions = new ArrayList<>(partitionedRows.size());
        for (int partitionNumber = 0; partitionNumber < partitionedRows.size(); partitionNumber++) {
            List<byte[]> allRowsInPartition = partitionedRows.get(partitionNumber);
            boolean partitionHasPartialFirstRow = partitionNumber == 0 && batch.hasPartialFirstRow();
            boolean partitionHasPartialLastRow =
                    partitionNumber == partitionedRows.size() - 1 && batch.hasPartialLastRow();

            RowsColumnRangeBatchRequest.Builder partition = new RowsColumnRangeBatchRequest.Builder();
            if (partitionHasPartialFirstRow) {
                partition = partition.partialFirstRow(batch.getPartialFirstRow());
            }
            List<byte[]> rowsToFullyLoad =
                    getRowsToFullyLoad(allRowsInPartition, partitionHasPartialFirstRow, partitionHasPartialLastRow);
            if (!rowsToFullyLoad.isEmpty()) {
                partition = partition.columnRangeSelectionForFullRows(batch.getColumnRangeSelection())
                                     .fullRows(rowsToFullyLoad);
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
