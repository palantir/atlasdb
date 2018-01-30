/*
 * Copyright 2015 Palantir Technologies, Inc. All rights reserved.
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
package com.palantir.atlasdb.keyvalue.impl;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.google.common.util.concurrent.Futures;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.AtlasDbPerformanceConstants;
import com.palantir.atlasdb.keyvalue.api.BatchColumnRangeSelection;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.ColumnRangeSelection;
import com.palantir.atlasdb.keyvalue.api.KeyAlreadyExistsException;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.RowColumnRangeIterator;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.common.base.ClosableIterator;
import com.palantir.common.base.Throwables;
import com.palantir.common.collect.Maps2;
import com.palantir.common.concurrent.NamedThreadFactory;
import com.palantir.common.concurrent.PTExecutors;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

@SuppressFBWarnings("SLF4J_ILLEGAL_PASSED_CLASS")
public abstract class AbstractKeyValueService implements KeyValueService {
    protected ExecutorService executor;

    protected final TracingPrefsConfig tracingPrefs;
    private final ScheduledExecutorService scheduledExecutor;

    /**
     * Note: This takes ownership of the given executor. It will be shutdown when the key
     * value service is closed.
     */
    public AbstractKeyValueService(ExecutorService executor) {
        this.executor = executor;
        this.tracingPrefs = new TracingPrefsConfig();
        this.scheduledExecutor = PTExecutors.newSingleThreadScheduledExecutor(
                new NamedThreadFactory(getClass().getSimpleName() + "-tracing-prefs", true));
        this.scheduledExecutor.scheduleWithFixedDelay(this.tracingPrefs, 0, 1, TimeUnit.MINUTES); // reload every minute
    }

    /**
     * Creates a fixed thread pool.
     *
     * @param threadNamePrefix thread name prefix
     * @param poolSize fixed thread pool size
     * @return a new fixed size thread pool with a keep alive time of 1 minute
     */
    protected static ExecutorService createFixedThreadPool(String threadNamePrefix, int poolSize) {
        ThreadPoolExecutor executor = PTExecutors.newFixedThreadPool(poolSize,
                new NamedThreadFactory(threadNamePrefix, false));
        executor.setKeepAliveTime(1, TimeUnit.MINUTES);
        return executor;
    }

    @Override
    public boolean supportsCheckAndSet() {
        return true;
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
        return Maps.newHashMap(Maps.transformValues(get(tableRef, keys), Value.GET_TIMESTAMP));
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

    /* (non-Javadoc)
     * @see com.palantir.atlasdb.keyvalue.api.KeyValueService#multiPut(java.util.Map, long)
     */
    @Override
    public void multiPut(Map<TableReference, ? extends Map<Cell, byte[]>> valuesByTable, final long timestamp)
            throws KeyAlreadyExistsException {
        List<Callable<Void>> callables = Lists.newArrayList();
        for (Entry<TableReference, ? extends Map<Cell, byte[]>> e : valuesByTable.entrySet()) {
            final TableReference table = e.getKey();
            // We sort here because some key value stores are more efficient if you store adjacent keys together.
            NavigableMap<Cell, byte[]> sortedMap = ImmutableSortedMap.copyOf(e.getValue());

            Iterable<List<Entry<Cell, byte[]>>> partitions = IterablePartitioner.partitionByCountAndBytes(
                    sortedMap.entrySet(),
                    getMultiPutBatchCount(),
                    getMultiPutBatchSizeBytes(),
                    table,
                    entry -> entry == null ? 0 : entry.getValue().length + Cells.getApproxSizeOfCell(entry.getKey()));

            for (final List<Entry<Cell, byte[]>> p : partitions) {
                callables.add(() -> {
                    String originalName = Thread.currentThread().getName();
                    Thread.currentThread().setName("Atlas multiPut of " + p.size() + " cells into " + table);
                    try {
                        put(table, Maps2.fromEntries(p), timestamp);
                        return null;
                    } finally {
                        Thread.currentThread().setName(originalName);
                    }
                });
            }
        }

        List<Future<Void>> futures;
        try {
            futures = executor.invokeAll(callables);
        } catch (InterruptedException e) {
            throw Throwables.throwUncheckedException(e);
        }
        for (Future<Void> future : futures) {
            try {
                future.get();
            } catch (InterruptedException e) {
                throw Throwables.throwUncheckedException(e);
            } catch (ExecutionException e) {
                throw Throwables.rewrapAndThrowUncheckedException(e.getCause());
            }
        }
    }

    @Override
    public void putMetadataForTables(final Map<TableReference, byte[]> tableRefToMetadata) {
        for (Map.Entry<TableReference, byte[]> entry : tableRefToMetadata.entrySet()) {
            putMetadataForTable(entry.getKey(), entry.getValue());
        }
    }

    @Override
    public void deleteRange(TableReference tableRef, RangeRequest range) {
        try (ClosableIterator<RowResult<Set<Long>>> iterator = getRangeOfTimestamps(tableRef, range,
                AtlasDbConstants.MAX_TS)) {
            while (iterator.hasNext()) {
                RowResult<Set<Long>> rowResult = iterator.next();
                Multimap<Cell, Long> cellsToDelete = HashMultimap.create();

                rowResult.getCells().forEach(entry -> cellsToDelete.putAll(entry.getKey(), entry.getValue()));
                delete(tableRef, cellsToDelete);
            }
        }
    }

    @Override
    public void deleteAllTimestamps(TableReference tableRef,
            Map<Cell, Long> maxTimestampExclusiveByCell) {
        deleteAllTimestampsDefaultImpl(this, tableRef, maxTimestampExclusiveByCell);
    }

    public static void deleteAllTimestampsDefaultImpl(KeyValueService kvs, TableReference tableRef,
            Map<Cell, Long> maxTimestampByCell) {
        if (maxTimestampByCell.isEmpty()) {
            return;
        }

        long maxTimestampExclusive = maxTimestampByCell.values().stream().max(Long::compare).get();

        Multimap<Cell, Long> timestampsByCell = kvs.getAllTimestamps(tableRef, maxTimestampByCell.keySet(),
                maxTimestampExclusive);

        Multimap<Cell, Long> timestampsByCellExcludingSentinels = Multimaps.filterEntries(timestampsByCell, entry -> {
            long maxTimestampForCell = maxTimestampByCell.get(entry.getKey());

            long timestamp = entry.getValue();
            return timestamp < maxTimestampForCell && timestamp != Value.INVALID_VALUE_TIMESTAMP;
        });

        kvs.delete(tableRef, timestampsByCellExcludingSentinels);
    }

    @Override
    public void close() {
        scheduledExecutor.shutdown();
        executor.shutdown();
    }

    @Override
    public void truncateTables(final Set<TableReference> tableRefs) {
        List<Future<Void>> futures = Lists.newArrayList();
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
    public Map<byte[], RowColumnRangeIterator> getRowsColumnRange(TableReference tableRef,
                                                                  Iterable<byte[]> rows,
                                                                  BatchColumnRangeSelection batchColumnRangeSelection,
                                                                  long timestamp) {
        return KeyValueServices.filterGetRowsToColumnRange(this, tableRef, rows, batchColumnRangeSelection, timestamp);
    }

    @Override
    public RowColumnRangeIterator getRowsColumnRange(TableReference tableRef,
                                                     Iterable<byte[]> rows,
                                                     ColumnRangeSelection columnRangeSelection,
                                                     int cellBatchHint,
                                                     long timestamp) {
        return KeyValueServices.mergeGetRowsColumnRangeIntoSingleIterator(this,
                                                                          tableRef,
                                                                          rows,
                                                                          columnRangeSelection,
                                                                          cellBatchHint,
                                                                          timestamp);
    }
}
