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
package com.palantir.atlasdb.keyvalue.dbkvs.impl;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.List;

public final class RowsColumnRangeBatchRequests {
    private RowsColumnRangeBatchRequests() {
        // Utility class
    }

    public static List<byte[]> getAllRowsInOrder(RowsColumnRangeBatchRequest batch) {
        List<byte[]> allRows = new ArrayList<>(batch.getRowsToLoadFully().size() + 2);
        batch.getPartialFirstRow().ifPresent(row -> allRows.add(row.getKey()));
        allRows.addAll(batch.getRowsToLoadFully());
        batch.getPartialLastRow().ifPresent(row -> allRows.add(row.getKey()));
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

        partitions.add(getFirstRequestInPartition(batch, partitionedRows.get(0)));
        for (int partitionNumber = 1; partitionNumber < partitionedRows.size() - 1; partitionNumber++) {
            RowsColumnRangeBatchRequest partition = ImmutableRowsColumnRangeBatchRequest.builder()
                    .columnRangeSelection(batch.getColumnRangeSelection())
                    .rowsToLoadFully(partitionedRows.get(partitionNumber))
                    .build();
            partitions.add(partition);
        }
        partitions.add(getLastRequestInPartition(batch, Iterables.getLast(partitionedRows)));

        return ImmutableList.copyOf(partitions);
    }

    /**
     * Assumes that there are at least 2 requests after paritioning (i.e. that the first request in the partition is not
     * also the last).
     */
    private static RowsColumnRangeBatchRequest getFirstRequestInPartition(
            RowsColumnRangeBatchRequest originalRequest, List<byte[]> allRowsInFirstPartition) {
        ImmutableRowsColumnRangeBatchRequest.Builder firstPartition = ImmutableRowsColumnRangeBatchRequest.builder()
                .columnRangeSelection(originalRequest.getColumnRangeSelection());
        if (originalRequest.getPartialFirstRow().isPresent()) {
            firstPartition
                    .partialFirstRow(originalRequest.getPartialFirstRow())
                    .rowsToLoadFully(Iterables.skip(allRowsInFirstPartition, 1));
        } else {
            firstPartition.rowsToLoadFully(allRowsInFirstPartition);
        }
        return firstPartition.build();
    }

    /**
     * Assumes that there are at least 2 requests after paritioning (i.e. that the last request in the partition is not
     * also the first).
     */
    private static RowsColumnRangeBatchRequest getLastRequestInPartition(
            RowsColumnRangeBatchRequest originalRequest, List<byte[]> allRowsInLastPartition) {
        ImmutableRowsColumnRangeBatchRequest.Builder lastPartition = ImmutableRowsColumnRangeBatchRequest.builder()
                .columnRangeSelection(originalRequest.getColumnRangeSelection());
        if (originalRequest.getPartialLastRow().isPresent()) {
            lastPartition
                    .partialLastRow(originalRequest.getPartialLastRow())
                    .rowsToLoadFully(allRowsInLastPartition.subList(0, allRowsInLastPartition.size() - 1));
        } else {
            lastPartition.rowsToLoadFully(allRowsInLastPartition);
        }
        return lastPartition.build();
    }
}
