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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.MultimapBuilder;
import com.google.common.collect.Multimaps;
import com.google.common.primitives.UnsignedBytes;
import com.palantir.atlasdb.cassandra.CassandraCellLoadingConfig;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

/**
 * Divides a list of {@link Cell}s into batches for querying.
 *
 * The batcher partitions cells by columns.
 * If for a given column the number of cells provided is at least
 * {@link CassandraCellLoadingConfig#crossColumnLoadBatchLimit()}, then the cells for that column will exclusively
 * occupy one or more batches, with no batch having size greater than
 * {@link CassandraCellLoadingConfig#singleQueryLoadBatchLimit()}.
 * Otherwise, the cells provided may be combined with cells for other columns in batches of size up to the value
 * from {@link CassandraCellLoadingConfig#crossColumnLoadBatchLimit()}. There is no guarantee that all cells for this
 * column will be in the same batch in this case.
 *
 * Live reloading: Batching will take place following some {@link CassandraCellLoadingConfig} available from
 * the supplier during the execution of a partition operation. There is no guarantee as to whether new values
 * available during a partition operation will or will not be applied.
 */
final class CellLoadingBatcher {
    private final Supplier<CassandraCellLoadingConfig> loadingConfigSupplier;
    private final BatchCallback rebatchingManyRowsForColumnCallback;

    CellLoadingBatcher(Supplier<CassandraCellLoadingConfig> loadingConfigSupplier,
            BatchCallback rebatchingManyRowsForColumnCallback) {
        this.loadingConfigSupplier = loadingConfigSupplier;
        this.rebatchingManyRowsForColumnCallback = rebatchingManyRowsForColumnCallback;
    }

    List<List<Cell>> partitionIntoBatches(
            Collection<Cell> cellsToPartition,
            InetSocketAddress host,
            TableReference tableReference) {
        CassandraCellLoadingConfig config = loadingConfigSupplier.get();

        ListMultimap<byte[], Cell> cellsByColumn = indexCellsByColumnName(cellsToPartition);

        List<List<Cell>> batches = Lists.newArrayList();
        List<Cell> cellsForCrossColumnBatching = Lists.newArrayList();
        for (Map.Entry<byte[], List<Cell>> cellColumnPair : Multimaps.asMap(cellsByColumn).entrySet()) {
            if (shouldExplicitlyAllocateBatchToColumn(config, cellColumnPair.getValue())) {
                batches.addAll(partitionBySingleQueryLoadBatchLimit(
                        cellColumnPair.getValue(), config, host, tableReference));
            } else {
                cellsForCrossColumnBatching.addAll(cellColumnPair.getValue());
            }
        }
        batches.addAll(Lists.partition(cellsForCrossColumnBatching, config.crossColumnLoadBatchLimit()));

        return batches;
    }

    private List<List<Cell>> partitionBySingleQueryLoadBatchLimit(
            List<Cell> cells,
            CassandraCellLoadingConfig config,
            InetSocketAddress host,
            TableReference tableReference) {
        if (cells.size() > config.singleQueryLoadBatchLimit()) {
            rebatchingManyRowsForColumnCallback.consume(host, tableReference, cells.size());
            return Lists.partition(cells, config.singleQueryLoadBatchLimit());
        }
        return ImmutableList.of(cells);
    }

    private static boolean shouldExplicitlyAllocateBatchToColumn(CassandraCellLoadingConfig config, List<Cell> cells) {
        return cells.size() > config.crossColumnLoadBatchLimit();
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

    @FunctionalInterface
    interface BatchCallback {
        void consume(InetSocketAddress host, TableReference tableReference, int numRows);
    }
}
