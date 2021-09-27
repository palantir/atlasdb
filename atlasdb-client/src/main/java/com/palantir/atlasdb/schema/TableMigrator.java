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
package com.palantir.atlasdb.schema;

import com.google.common.collect.Ordering;
import com.google.common.primitives.UnsignedBytes;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.ColumnSelection;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.table.description.RowNamePartitioner;
import com.palantir.atlasdb.table.description.UniformRowNamePartitioner;
import com.palantir.atlasdb.table.description.ValueType;
import com.palantir.common.base.Throwables;
import com.palantir.common.concurrent.PTExecutors;
import com.palantir.logsafe.Preconditions;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

public class TableMigrator {
    private final TableReference srcTable;
    private final int partitions;
    private final List<RowNamePartitioner> partitioners;
    private final int readBatchSize;
    private final ExecutorService executor;
    private final AbstractTaskCheckpointer checkpointer;
    private final TaskProgress progress;
    private final ColumnSelection columnSelection;
    private final RangeMigrator rangeMigrator;

    /**
     * See {@link TableMigratorBuilder}.
     */
    TableMigrator(
            TableReference srcTable,
            int partitions,
            List<RowNamePartitioner> partitioners,
            int readBatchSize,
            ExecutorService executor,
            AbstractTaskCheckpointer checkpointer,
            TaskProgress progress,
            ColumnSelection columnSelection,
            RangeMigrator rangeMigrator) {
        this.srcTable = srcTable;
        this.partitions = setPartitions(partitions);
        this.partitioners = partitioners;
        this.readBatchSize = readBatchSize;
        this.executor = executor;
        this.checkpointer = checkpointer;
        this.progress = progress;
        this.columnSelection = columnSelection;
        this.rangeMigrator = rangeMigrator;
    }

    private int setPartitions(int minNumPartitions) {
        Preconditions.checkArgument(minNumPartitions >= 1);

        // round partitions up to a power of 2
        int highestOne = Integer.highestOneBit(minNumPartitions);
        if (highestOne != minNumPartitions) {
            return highestOne * 2;
        }
        return minNumPartitions;
    }

    public void migrate() {
        List<byte[]> rangeBoundaries = getRangeBoundaries();

        int totalTasks = rangeBoundaries.size() - 1;

        progress.beginTask("Migrating table " + srcTable + "...", totalTasks);

        Map<Long, byte[]> boundaryById = new HashMap<>();
        for (long rangeId = 0; rangeId < rangeBoundaries.size() - 1; rangeId++) {
            boundaryById.put(rangeId, rangeBoundaries.get((int) rangeId));
        }
        checkpointer.createCheckpoints(srcTable.getQualifiedName(), boundaryById);

        // Look up the checkpoints and log start point (or done)
        rangeMigrator.logStatus(rangeBoundaries.size());

        List<Future<Void>> futures = new ArrayList<>();
        for (long rangeId = 0; rangeId < rangeBoundaries.size() - 1; rangeId++) {
            byte[] end = rangeBoundaries.get((int) rangeId + 1);
            // the range's start will be set within the transaction
            RangeRequest range = RangeRequest.builder()
                    .endRowExclusive(end)
                    .batchHint(readBatchSize)
                    .retainColumns(columnSelection)
                    .build();

            Callable<Void> task = createMigrationTask(range, rangeId);
            Callable<Void> wrappedTask = PTExecutors.wrap("MigrationTask", task);
            Future<Void> future = executor.submit(wrappedTask);
            futures.add(future);
        }

        waitForFutures(futures);

        progress.taskComplete();
    }

    private void waitForFutures(List<Future<Void>> futures) {
        try {
            for (Future<Void> f : futures) {
                f.get();
            }
        } catch (InterruptedException e) {
            Throwables.throwUncheckedException(e);
        } catch (ExecutionException e) {
            Throwables.throwUncheckedException(e.getCause());
        }
    }

    private Callable<Void> createMigrationTask(final RangeRequest range, final long rangeId) {
        return () -> {
            migrateTableRange(range, rangeId);
            taskComplete();
            return null;
        };
    }

    private void taskComplete() {
        progress.subTaskComplete();
    }

    /**
     * Returns all the range boundaries for the given partitioners.
     * The range boundaries will be sorted and will include the empty byte array at the start and
     * end to ensure that all entries are covered by the ranges.
     * If a table doesn't support partitioning, we'll make fake partitions and hope it helps.
     */
    private List<byte[]> getRangeBoundaries() {
        Set<byte[]> rangeBoundaries = Collections.newSetFromMap(new IdentityHashMap<>());
        // Must use PtBytes.EMPTY_BYTE_ARRAY to avoid duplicate when adding from UniformRowNamePartitioner
        rangeBoundaries.add(PtBytes.EMPTY_BYTE_ARRAY);

        if (partitioners.isEmpty()) {
            rangeBoundaries.addAll(new UniformRowNamePartitioner(ValueType.FIXED_LONG).getPartitions(partitions));
        } else if (partitioners.size() == 1) {
            rangeBoundaries.addAll(partitioners.get(0).getPartitions(partitions));
        } else {
            int splitPartitions = partitions / partitioners.size();
            for (int i = 0; i < partitioners.size(); i++) {
                rangeBoundaries.addAll(partitioners.get(i).getPartitions(splitPartitions));
            }
        }

        List<byte[]> sortedBoundaries =
                Ordering.from(UnsignedBytes.lexicographicalComparator()).sortedCopy(rangeBoundaries);
        sortedBoundaries.add(PtBytes.EMPTY_BYTE_ARRAY);
        return sortedBoundaries;
    }

    private void migrateTableRange(RangeRequest range, long rangeId) {
        rangeMigrator.migrateRange(range, rangeId);
    }
}
