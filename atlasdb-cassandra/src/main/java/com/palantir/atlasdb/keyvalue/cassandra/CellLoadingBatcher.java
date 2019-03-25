/*
 * (c) Copyright 2019 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.keyvalue.cassandra;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.IntConsumer;
import java.util.function.IntSupplier;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.MultimapBuilder;
import com.google.common.collect.Multimaps;
import com.google.common.primitives.UnsignedBytes;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.keyvalue.api.Cell;

/**
 * Divides a list of {@link com.palantir.atlasdb.keyvalue.api.Cell}s into batches for querying.
 *
 * The batcher partitions cells by columns.
 * If for a given column the number of cells provided is at least the value returned by the
 * crossColumnLoadBatchLimitSupplier, then the cells for that column will exclusively occupy one or more
 * batches, with no batch exceeding the value returned by the singleQueryLoadBatchLimitSupplier.
 * Otherwise, the cells provided may be combined with cells for other columns in batches of size up to the value
 * returned by the crossColumnLoadBatchLimitSupplier. There is no guarantee that all cells for this column will
 * be in the same batch.
 */
final class CellLoadingBatcher {
    private static final int DEFAULT_CROSS_COLUMN_LOAD_BATCH_LIMIT = 200;

    private final IntSupplier crossColumnLoadBatchLimitSupplier;
    private final IntSupplier singleQueryLoadBatchLimitSupplier;

    private final IntConsumer rebatchingManyRowsWarningCallback;

    @VisibleForTesting
    CellLoadingBatcher(
            IntSupplier crossColumnLoadBatchLimitSupplier,
            IntSupplier singleQueryLoadBatchLimitSupplier,
            IntConsumer rebatchingManyRowsWarningCallback) {
        this.crossColumnLoadBatchLimitSupplier = crossColumnLoadBatchLimitSupplier;
        this.singleQueryLoadBatchLimitSupplier = singleQueryLoadBatchLimitSupplier;
        this.rebatchingManyRowsWarningCallback = rebatchingManyRowsWarningCallback;
    }

    public static CellLoadingBatcher create(IntConsumer rebatchingManyRowsWarningCallback) {
        // TODO (jkong): Maybe not the best default for the transaction timestamp batching.
        return new CellLoadingBatcher(
                () -> DEFAULT_CROSS_COLUMN_LOAD_BATCH_LIMIT,
                () -> AtlasDbConstants.TRANSACTION_TIMESTAMP_LOAD_BATCH_LIMIT,
                rebatchingManyRowsWarningCallback);
    }

    List<List<Cell>> partitionIntoBatches(Collection<Cell> cellsToPartition) {
        int multigetMultisliceBatchLimit = crossColumnLoadBatchLimitSupplier.getAsInt();
        int singleQueryLoadBatchLimit = singleQueryLoadBatchLimitSupplier.getAsInt();

        ListMultimap<byte[], Cell> cellsByColumn = indexCellsByColumnName(cellsToPartition);

        List<List<Cell>> batches = Lists.newArrayList();
        List<Cell> cellsForCrossColumnBatching = Lists.newArrayList();
        for (Map.Entry<byte[], List<Cell>> cellColumnPair : Multimaps.asMap(cellsByColumn).entrySet()) {
            if (shouldExplicitlyAllocateBatchToColumn(multigetMultisliceBatchLimit, cellColumnPair.getValue())) {
                batches.addAll(
                        partitionBySingleQueryLoadBatchLimit(cellColumnPair.getValue(), singleQueryLoadBatchLimit));
            } else {
                cellsForCrossColumnBatching.addAll(cellColumnPair.getValue());
            }
        }
        batches.addAll(Lists.partition(cellsForCrossColumnBatching, multigetMultisliceBatchLimit));

        return batches;
    }

    private List<List<Cell>> partitionBySingleQueryLoadBatchLimit(List<Cell> cells, int singleQueryLoadBatchLimit) {
        if (cells.size() > singleQueryLoadBatchLimit) {
            rebatchingManyRowsWarningCallback.accept(cells.size());
            return Lists.partition(cells, singleQueryLoadBatchLimit);
        }
        return ImmutableList.of(cells);
    }

    private static boolean shouldExplicitlyAllocateBatchToColumn(int multigetMultisliceBatchLimit, List<Cell> cells) {
        return cells.size() > multigetMultisliceBatchLimit;
    }

    private static ListMultimap<byte[], Cell> indexCellsByColumnName(Collection<Cell> cells) {
        // Cannot use Multimaps.index(), because byte[] equality is tricky.
        ListMultimap<byte[], Cell> cellsByColumn = MultimapBuilder.treeKeys(UnsignedBytes.lexicographicalComparator())
                .arrayListValues()
                .build();
        for (Cell cell : cells) {
            cellsByColumn.put(cell.getColumnName(), cell);
        }
        return cellsByColumn;
    }
}
