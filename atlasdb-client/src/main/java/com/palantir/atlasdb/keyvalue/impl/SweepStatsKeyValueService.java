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
package com.palantir.atlasdb.keyvalue.impl;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Functions;
import com.google.common.base.Preconditions;
import com.google.common.collect.Collections2;
import com.google.common.collect.ConcurrentHashMultiset;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultiset;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multiset;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.MoreExecutors;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.schema.SweepSchema;
import com.palantir.atlasdb.schema.generated.SweepPriorityTable;
import com.palantir.atlasdb.schema.generated.SweepPriorityTable.SweepPriorityNamedColumn;
import com.palantir.atlasdb.schema.generated.SweepPriorityTable.SweepPriorityRow;
import com.palantir.atlasdb.transaction.impl.TransactionConstants;
import com.palantir.common.concurrent.PTExecutors;
import com.palantir.common.persist.Persistables;
import com.palantir.timestamp.TimestampService;

/**
 * This kvs wrapper tracks the approximate number of writes to every table
 * since the last time the table was completely swept. This is used when
 * deciding the order in which tables should be swept.
 */
public class SweepStatsKeyValueService extends ForwardingKeyValueService {

    private static final Logger log = LoggerFactory.getLogger(SweepStatsKeyValueService.class);
    private static final int CLEAR_WEIGHT = 1 << 14;
    private static final int WRITE_THRESHOLD = 1 << 16;
    private static final long FLUSH_DELAY_SECONDS = 42;

    // This is gross and won't work if someone starts namespacing sweep differently
    private static final String SWEEP_PRIORITY_TABLE = SweepSchema.INSTANCE.getNamespace().getName() + '.' + SweepPriorityTable.getRawTableName();

    private final KeyValueService delegate;
    private final TimestampService timestampService;
    private final Multiset<String> writesByTable = ConcurrentHashMultiset.create();
    private final Set<String> clearedTables = Collections.newSetFromMap(new ConcurrentHashMap<String, Boolean>());
    private final AtomicInteger totalModifications = new AtomicInteger();
    private final Lock flushLock = new ReentrantLock();
    private final ScheduledExecutorService flushExecutor = PTExecutors.newSingleThreadScheduledExecutor();

    public SweepStatsKeyValueService(KeyValueService delegate,
                                     TimestampService timestampService) {
        this.delegate = delegate;
        this.timestampService = timestampService;
        this.flushExecutor.scheduleWithFixedDelay(createFlushTask(), FLUSH_DELAY_SECONDS, FLUSH_DELAY_SECONDS, TimeUnit.SECONDS);
    }

    @Override
    protected KeyValueService delegate() {
        return delegate;
    }

    @Override
    public void put(String tableName, Map<Cell, byte[]> values, long timestamp) {
        delegate().put(tableName, values, timestamp);
        writesByTable.add(tableName, values.size());
        recordModifications(values.size());
    }

    @Override
    public void multiPut(Map<String, ? extends Map<Cell, byte[]>> valuesByTable, long timestamp) {
        delegate().multiPut(valuesByTable, timestamp);
        int newWrites = 0;
        for (Entry<String, ? extends Map<Cell, byte[]>> entry : valuesByTable.entrySet()) {
            writesByTable.add(entry.getKey(), entry.getValue().size());
            newWrites += entry.getValue().size();
        }
        recordModifications(newWrites);
    }

    @Override
    public void putWithTimestamps(String tableName, Multimap<Cell, Value> cellValues) {
        delegate().putWithTimestamps(tableName, cellValues);
        writesByTable.add(tableName, cellValues.size());
        recordModifications(cellValues.size());
    }

    @Override
    public void truncateTable(String tableName) {
        delegate().truncateTable(tableName);
        clearedTables.add(tableName);
        recordModifications(CLEAR_WEIGHT);
    }

    @Override
    public void truncateTables(Set<String> tableNames) {
        delegate().truncateTables(tableNames);
        clearedTables.addAll(tableNames);
        recordModifications(CLEAR_WEIGHT * tableNames.size());
    }

    @Override
    public void dropTable(String tableName) {
        delegate().dropTable(tableName);
        clearedTables.add(tableName);
        recordModifications(CLEAR_WEIGHT);
    }

    @Override
    public void close() {
        terminateExecutor(new Runnable() {
            @Override
            public void run() {
                delegate.close();
            }
        });
    }

    @Override
    public void teardown() {
        terminateExecutor(new Runnable() {
            @Override
            public void run() {
                delegate().teardown();
            }
        });
    }

    private void forceFlush() {
        recordModifications(WRITE_THRESHOLD);
        if (flushExecutor.isShutdown()) {
            log.warn("Cannot flush stats while shutting down");
        } else {
            flushExecutor.execute(createFlushTask());
        }
    }

    private void terminateExecutor(Runnable cleanupTask) {
        forceFlush();
        flushExecutor.shutdown();
        try {
            if (flushExecutor.awaitTermination(1, TimeUnit.MINUTES)) {
                log.info("Successfully terminated flush executor");
            } else {
                log.warn("Timed out while waiting for flush executor termination");
                List<Runnable> pendingTasks = flushExecutor.shutdownNow();
                log.info("Attempting to complete flushing {} pending tasks", pendingTasks.size());
                Executor directExecutor = MoreExecutors.directExecutor();
                for (Runnable pendingTask : pendingTasks) {
                    directExecutor.execute(pendingTask);
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("Interrupted while terminating flush executor", e);
        } finally {
            cleanupTask.run();
        }
    }

    // This way of recording the number of writes to tables is obviously not
    // completely correct. It does no synchronization between processes (so
    // updates could be clobbered), and it makes little effort to ensure that
    // all updates are flushed. It is intended only to be "good enough" for
    // determining what tables have been written to a lot.

    private void recordModifications(int newWrites) {
        totalModifications.addAndGet(newWrites);
    }

    private Runnable createFlushTask() {
        return new Runnable() {
            @Override
            public void run() {
                try {
                    if (totalModifications.get() >= WRITE_THRESHOLD && flushLock.tryLock()) {
                        try {
                            if (totalModifications.get() >= WRITE_THRESHOLD) {
                                // snapshot current values while holding the lock and flush
                                totalModifications.set(0);
                                Multiset<String> localWritesByTable = ImmutableMultiset.copyOf(writesByTable);
                                writesByTable.clear();
                                Set<String> localClearedTables = ImmutableSet.copyOf(clearedTables);
                                clearedTables.clear();

                                // apply back pressure by only allowing one flush at a time
                                flushWrites(localWritesByTable, localClearedTables);
                            }
                        } finally {
                            flushLock.unlock();
                        }
                    }
                } catch (Throwable t) {
                    log.error("Error occurred while flushing sweep stats: {}", t, t);
                }
            }
        };
    }

    private void flushWrites(Multiset<String> writes, Set<String> clears) {
        if (writes.isEmpty() && clears.isEmpty()) {
            log.debug("No writes to flush");
            return;
        }

        log.debug("Flushing stats for {} writes and {} clears",
                writes.size(), clears.size());
        log.trace("Flushing writes: {}", writes);
        log.trace("Flushing clears: {}", clears);
        try {
            Set<String> tableNames = Sets.difference(writes.elementSet(), clears);
            Iterable<byte[]> rows = Collections2.transform(tableNames,
                    Functions.compose(Persistables.persistToBytesFunction(), SweepPriorityRow.fromFullTableNameFun()));
            Map<Cell, Value> oldWriteCounts = delegate().getRows(SWEEP_PRIORITY_TABLE, rows,
                    SweepPriorityTable.getColumnSelection(SweepPriorityNamedColumn.WRITE_COUNT), Long.MAX_VALUE);
            Map<Cell, byte[]> newWriteCounts = Maps.newHashMapWithExpectedSize(writes.elementSet().size());
            byte[] col = SweepPriorityNamedColumn.WRITE_COUNT.getShortName();
            for (String tableName : tableNames) {
                Preconditions.checkState(!tableName.startsWith(AtlasDbConstants.NAMESPACE_PREFIX),
                        "The sweep stats kvs should wrap the namespace mapping kvs, not the other way around.");
                byte[] row = SweepPriorityRow.of(tableName).persistToBytes();
                Cell cell = Cell.create(row, col);
                Value oldValue = oldWriteCounts.get(cell);
                long oldCount = oldValue == null ? 0 : SweepPriorityTable.WriteCount.BYTES_HYDRATOR.hydrateFromBytes(oldValue.getContents()).getValue();
                long newValue = clears.contains(tableName) ? writes.count(tableName) : oldCount + writes.count(tableName);
                log.debug("Sweep priority for {} has {} writes (was {})", tableName, newValue, oldCount);
                newWriteCounts.put(cell, SweepPriorityTable.WriteCount.of(newValue).persistValue());
            }
            long timestamp = timestampService.getFreshTimestamp();

            // Committing before writing is intentional, we want the start timestamp to
            // show up in the transaction table before we write do our writes.
            commit(timestamp);
            delegate().put(SWEEP_PRIORITY_TABLE, newWriteCounts, timestamp);
        } catch (RuntimeException e) {
            Set<String> allTableNames = delegate().getAllTableNames();
            if (!allTableNames.contains(SWEEP_PRIORITY_TABLE)
                    || !allTableNames.contains(TransactionConstants.TRANSACTION_TABLE)) {
                // ignore problems when sweep or transaction tables don't exist
                log.warn("Ignoring failed sweep stats flush due to {}", e.getMessage(), e);
            }
            log.error("Unable to flush sweep stats for writes {} and clears {}: {}",
                    writes, clears, e.getMessage(), e);
            throw e;
        }
    }

    private void commit(long timestamp) {
        Cell cell = Cell.create(
                TransactionConstants.getValueForTimestamp(timestamp),
                TransactionConstants.COMMIT_TS_COLUMN);
        byte[] value = TransactionConstants.getValueForTimestamp(timestamp);
        delegate().putUnlessExists(TransactionConstants.TRANSACTION_TABLE, ImmutableMap.of(cell, value));
    }
}
