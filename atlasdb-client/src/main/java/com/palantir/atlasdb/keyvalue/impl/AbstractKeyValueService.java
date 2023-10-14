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

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.primitives.Ints;
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
import com.palantir.common.concurrent.BlockingWorkerPool;
import com.palantir.common.concurrent.PTExecutors;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;

@SuppressFBWarnings("SLF4J_ILLEGAL_PASSED_CLASS")
public abstract class AbstractKeyValueService implements KeyValueService {

    private static final LoadingCache<TableReference, String> internalTableNames = Caffeine.newBuilder()
            .weigher((TableReference key, String value) -> Ints.saturatedCast(key.sizeInBytes() + value.length()))
            .maximumWeight(1_000_000)
            .build(AbstractKeyValueService::computeInternalTableName);

    protected final ExecutorService executor;
    protected final int executorQosSize;

    /**
     * Note: This takes ownership of the given executor. It will be shutdown when the key
     * value service is closed.
     */
    public AbstractKeyValueService(ExecutorService executor) {
        this(executor, 0);
    }

    public AbstractKeyValueService(ExecutorService executor, int executorQosSize) {
        this.executor = executor;
        this.executorQosSize = executorQosSize;
    }

    /**
     * Creates a fixed thread pool.
     *
     * @param name thread name prefix
     * @param corePoolSize size of the core pool
     * @return a new fixed size thread pool with a keep alive time of 1 minute
     */
    protected static ExecutorService createFixedThreadPool(String name, int corePoolSize) {
        return createThreadPool(name, corePoolSize, corePoolSize);
    }

    /**
     * Creates a thread pool with number of threads between {@code _corePoolSize} and {@code maxPoolSize}.
     *
     * @param name thread name prefix
     * @param _corePoolSize size of the core pool
     * @param maxPoolSize maximum size of the pool
     * @return a new fixed size thread pool with a keep alive time of 1 minute
     */
    protected static ExecutorService createThreadPool(String name, int _corePoolSize, int maxPoolSize) {
        return PTExecutors.newFixedThreadPool(maxPoolSize, name);
    }

    /**
     * Creates a thread pool with number of threads between {@code _corePoolSize} and {@code maxPoolSize}.
     * It does not create a span.
     *
     * @param name thread name prefix
     * @param _corePoolSize size of the core pool
     * @param maxPoolSize maximum size of the pool
     * @return a new fixed size thread pool with a keep alive time of 1 minute
     */
    protected static ExecutorService createThreadPoolWithoutSpans(String name, int _corePoolSize, int maxPoolSize) {
        return PTExecutors.newFixedThreadPoolWithoutSpan(maxPoolSize, name);
    }

    @Override
    public CheckAndSetCompatibility getCheckAndSetCompatibility() {
        return CheckAndSetCompatibility.supportedBuilder()
                .supportsMultiCheckAndSetOperations(false)
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
        return builder.buildOrThrow();
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
        BlockingWorkerPool<Void> pool = new BlockingWorkerPool<>(executor, executorQosSize);
        try {
            for (final TableReference tableRef : tableRefs) {
                pool.submitTask(() -> {
                    truncateTable(tableRef);
                });
            }
            pool.waitForSubmittedTasks();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    public static String internalTableName(TableReference tableRef) {
        return internalTableNames.get(tableRef);
    }

    private static String computeInternalTableName(TableReference tableRef) {
        String tableName = tableRef.getQualifiedName();
        if (tableName.startsWith("_")) {
            return tableName;
        }
        int periodIndex = tableName.indexOf('.');
        if (periodIndex > -1) {
            return tableName.substring(0, periodIndex) + "__"
                    + (periodIndex + 1 != tableName.length() ? tableName.substring(periodIndex + 1) : "");
        }
        return tableName;
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
