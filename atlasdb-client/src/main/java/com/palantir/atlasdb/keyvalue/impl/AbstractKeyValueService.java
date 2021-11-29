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
package com.palantir.atlasdb.keyvalue.impl;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.util.concurrent.Futures;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.AtlasDbPerformanceConstants;
import com.palantir.atlasdb.keyvalue.api.BatchColumnRangeSelection;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.CheckAndSetCompatibility;
import com.palantir.atlasdb.keyvalue.api.ColumnRangeSelection;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.RangeRequests;
import com.palantir.atlasdb.keyvalue.api.RowColumnRangeIterator;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.common.base.ClosableIterator;
import com.palantir.common.concurrent.PTExecutors;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

@SuppressFBWarnings("SLF4J_ILLEGAL_PASSED_CLASS")
public abstract class AbstractKeyValueService implements KeyValueService {
    protected ExecutorService executor;

    /**
     * Note: This takes ownership of the given executor. It will be shutdown when the key
     * value service is closed.
     */
    public AbstractKeyValueService(ExecutorService executor) {
        this.executor = executor;
    }

    /**
     * Creates a fixed thread pool.
     *
     * @param threadNamePrefix thread name prefix
     * @param corePoolSize size of the core pool
     * @return a new fixed size thread pool with a keep alive time of 1 minute
     */
    protected static ExecutorService createFixedThreadPool(String threadNamePrefix, int corePoolSize) {
        return createThreadPool(threadNamePrefix, corePoolSize, corePoolSize);
    }

    /**
     * Creates a thread pool with number of threads between {@code _corePoolSize} and {@code maxPoolSize}.
     *
     * @param threadNamePrefix thread name prefix
     * @param _corePoolSize size of the core pool
     * @param maxPoolSize maximum size of the pool
     * @return a new fixed size thread pool with a keep alive time of 1 minute
     */
    protected static ExecutorService createThreadPool(String threadNamePrefix, int _corePoolSize, int maxPoolSize) {
        return PTExecutors.newFixedThreadPool(maxPoolSize, threadNamePrefix);
    }

    @Override
    public CheckAndSetCompatibility getCheckAndSetCompatibility() {
        return CheckAndSetCompatibility.supportedBuilder()
                .consistentOnFailure(true)
                .supportsDetailOnFailure(false)
                .build();
    }

    @Override
    public void createTables(Map<TableReference, byte[]> tableRefToTableMetadata) {
        for (Map.Entry<TableReference, byte[]> entry : tableRefToTableMetadata.entrySet()) {
            createTable(entry.getKey(), entry.getValue());
        }
    }

    @Override
    public void dropTables(Set<TableReference> tableRefs) {
        for (TableReference tableName : tableRefs) {
            dropTable(tableName);
        }
    }

    @Override
    public Collection<? extends KeyValueService> getDelegates() {
        return ImmutableList.of();
    }

    /*
     * This version just does a regular read and returns the timestamp of the fetched value.
     * This is slower than it needs to be so implementers are encouraged to make a faster impl.
     */
    @Override
    public Map<Cell, Long> getLatestTimestamps(TableReference tableRef, Map<Cell, Long> keys) {
        return new HashMap<>(Maps.transformValues(get(tableRef, keys), Value.GET_TIMESTAMP));
    }

    @Override
    public Map<TableReference, byte[]> getMetadataForTables() {
        ImmutableMap.Builder<TableReference, byte[]> builder = ImmutableMap.builder();
        for (TableReference table : getAllTableNames()) {
            builder.put(table, getMetadataForTable(table));
        }
        return builder.build();
    }

    protected int getMultiPutBatchCount() {
        return AtlasDbPerformanceConstants.MAX_BATCH_SIZE;
    }

    protected long getMultiPutBatchSizeBytes() {
        return AtlasDbPerformanceConstants.MAX_BATCH_SIZE_BYTES;
    }

    @Override
    public void putMetadataForTables(final Map<TableReference, byte[]> tableRefToMetadata) {
        for (Map.Entry<TableReference, byte[]> entry : tableRefToMetadata.entrySet()) {
            putMetadataForTable(entry.getKey(), entry.getValue());
        }
    }

    @Override
    public void deleteRange(TableReference tableRef, RangeRequest range) {
        try (ClosableIterator<RowResult<Set<Long>>> iterator =
                getRangeOfTimestamps(tableRef, range, AtlasDbConstants.MAX_TS)) {
            while (iterator.hasNext()) {
                RowResult<Set<Long>> rowResult = iterator.next();
                Multimap<Cell, Long> cellsToDelete = HashMultimap.create();

                rowResult.getCells().forEach(entry -> cellsToDelete.putAll(entry.getKey(), entry.getValue()));
                delete(tableRef, cellsToDelete);
            }
        }
    }

    @Override
    public void deleteRows(TableReference tableRef, Iterable<byte[]> rows) {
        rows.forEach(row -> deleteRange(tableRef, RangeRequests.ofSingleRow(row)));
    }

    @Override
    public void close() {
        executor.shutdown();
    }

    @Override
    public void truncateTables(final Set<TableReference> tableRefs) {
        List<Future<Void>> futures = new ArrayList<>();
        for (final TableReference tableRef : tableRefs) {
            futures.add(executor.submit(() -> {
                truncateTable(tableRef);
                return null;
            }));
        }

        for (Future<Void> future : futures) {
            Futures.getUnchecked(future);
        }
    }

    public static String internalTableName(TableReference tableRef) {
        String tableName = tableRef.getQualifiedName();
        if (tableName.startsWith("_")) {
            return tableName;
        }
        return tableName.replaceFirst("\\.", "__");
    }

    @Override
    public Map<byte[], RowColumnRangeIterator> getRowsColumnRange(
            TableReference tableRef,
            Iterable<byte[]> rows,
            BatchColumnRangeSelection batchColumnRangeSelection,
            long timestamp) {
        return KeyValueServices.filterGetRowsToColumnRange(this, tableRef, rows, batchColumnRangeSelection, timestamp);
    }

    @Override
    public RowColumnRangeIterator getRowsColumnRange(
            TableReference tableRef,
            Iterable<byte[]> rows,
            ColumnRangeSelection columnRangeSelection,
            int cellBatchHint,
            long timestamp) {
        return KeyValueServices.mergeGetRowsColumnRangeIntoSingleIterator(
                this, tableRef, rows, columnRangeSelection, cellBatchHint, timestamp);
    }
}
