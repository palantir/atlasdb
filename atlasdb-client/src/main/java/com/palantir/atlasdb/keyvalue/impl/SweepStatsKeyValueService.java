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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Functions;
import com.google.common.collect.Collections2;
import com.google.common.collect.ConcurrentHashMultiset;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultiset;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multiset;
import com.google.common.collect.Sets;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.ClusterAvailabilityStatus;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.schema.SweepSchema;
import com.palantir.atlasdb.schema.generated.SweepPriorityTable;
import com.palantir.atlasdb.schema.generated.SweepPriorityTable.SweepPriorityNamedColumn;
import com.palantir.atlasdb.schema.generated.SweepPriorityTable.SweepPriorityRow;
import com.palantir.atlasdb.transaction.impl.TransactionConstants;
import com.palantir.common.concurrent.PTExecutors;
import com.palantir.common.persist.Persistables;
import com.palantir.logsafe.Preconditions;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.UnsafeArg;
import com.palantir.timestamp.TimestampService;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This kvs wrapper tracks the approximate number of writes to every table
 * since the last time the table was completely swept. This is used when
 * deciding the order in which tables should be swept.
 */
public final class SweepStatsKeyValueService extends ForwardingKeyValueService {

    private static final Logger log = LoggerFactory.getLogger(SweepStatsKeyValueService.class);
    private static final int CLEAR_WEIGHT = 1 << 14; // 16384
    private static final long FLUSH_DELAY_SECONDS = 42;

    // This is gross and won't work if someone starts namespacing sweep differently
    private static final TableReference SWEEP_PRIORITY_TABLE =
            TableReference.create(SweepSchema.INSTANCE.getNamespace(), SweepPriorityTable.getRawTableName());

    private final KeyValueService delegate;
    private final TimestampService timestampService;
    private final Supplier<Integer> writeThreshold; // number of cells which allows write stats to be flushed
    private final Supplier<Long> writeSizeThreshold; // size of values which allows write stats to be flushed
    private final Supplier<Boolean> isEnabled; // for toggling legacy sweep enabled/disabled online

    private final Multiset<TableReference> writesByTable = ConcurrentHashMultiset.create();

    private final Set<TableReference> clearedTables = ConcurrentHashMap.newKeySet();

    private final AtomicInteger totalModifications = new AtomicInteger();
    private final AtomicLong totalModificationsSize = new AtomicLong();
    private final Lock flushLock = new ReentrantLock();
    private final ScheduledExecutorService flushExecutor = PTExecutors.newSingleThreadScheduledExecutor();

    public static SweepStatsKeyValueService create(
            KeyValueService delegate,
            TimestampService timestampService,
            Supplier<Integer> writeThreshold,
            Supplier<Long> writeSizeThreshold,
            Supplier<Boolean> isEnabled) {
        return new SweepStatsKeyValueService(delegate, timestampService, writeThreshold, writeSizeThreshold, isEnabled);
    }

    private SweepStatsKeyValueService(
            KeyValueService delegate,
            TimestampService timestampService,
            Supplier<Integer> writeThreshold,
            Supplier<Long> writeSizeThreshold,
            Supplier<Boolean> isEnabled) {
        this.delegate = delegate;
        this.timestampService = timestampService;
        this.writeThreshold = writeThreshold;
        this.writeSizeThreshold = writeSizeThreshold;
        this.isEnabled = isEnabled;
        this.flushExecutor.scheduleWithFixedDelay(
                createFlushTask(), FLUSH_DELAY_SECONDS, FLUSH_DELAY_SECONDS, TimeUnit.SECONDS);
    }

    @Override
    public KeyValueService delegate() {
        return delegate;
    }

    @Override
    public void put(TableReference tableRef, Map<Cell, byte[]> values, long timestamp) {
        delegate().put(tableRef, values, timestamp);
        if (isEnabled.get()) {
            writesByTable.add(tableRef, values.size());
            recordModifications(values.size());
            recordModificationsSize(values.entrySet().stream()
                    .mapToLong(cellEntry -> cellEntry.getValue().length)
                    .sum());
        }
    }

    @Override
    public void multiPut(Map<TableReference, ? extends Map<Cell, byte[]>> valuesByTable, long timestamp) {
        delegate().multiPut(valuesByTable, timestamp);
        if (isEnabled.get()) {
            int newWrites = 0;
            long writesSize = 0;
            for (Map.Entry<TableReference, ? extends Map<Cell, byte[]>> entry : valuesByTable.entrySet()) {
                writesByTable.add(entry.getKey(), entry.getValue().size());
                newWrites += entry.getValue().size();
                writesSize += entry.getValue().entrySet().stream()
                        .mapToLong(cellEntry -> cellEntry.getValue().length)
                        .sum();
            }
            recordModifications(newWrites);
            recordModificationsSize(writesSize);
        }
    }

    @Override
    public void putWithTimestamps(TableReference tableRef, Multimap<Cell, Value> cellValues) {
        delegate().putWithTimestamps(tableRef, cellValues);
        if (isEnabled.get()) {
            writesByTable.add(tableRef, cellValues.size());
            recordModifications(cellValues.size());
            recordModificationsSize(cellValues.entries().stream()
                    .mapToLong(cellEntry -> cellEntry.getValue().getContents().length)
                    .sum());
        }
    }

    @Override
    public void deleteRange(TableReference tableRef, RangeRequest range) {
        delegate().deleteRange(tableRef, range);
        if (isEnabled.get()) {
            if (RangeRequest.all().equals(range)) {
                // This is equivalent to truncate.
                recordClear(tableRef);
            }
        }
    }

    @Override
    public void truncateTable(TableReference tableRef) {
        delegate().truncateTable(tableRef);
        if (isEnabled.get()) {
            recordClear(tableRef);
        }
    }

    @Override
    public void truncateTables(Set<TableReference> tableRefs) {
        delegate().truncateTables(tableRefs);
        if (isEnabled.get()) {
            clearedTables.addAll(tableRefs);
            recordModifications(CLEAR_WEIGHT * tableRefs.size());
        }
    }

    @Override
    public void dropTable(TableReference tableRef) {
        delegate().dropTable(tableRef);
        if (isEnabled.get()) {
            recordClear(tableRef);
        }
    }

    @Override
    public ClusterAvailabilityStatus getClusterAvailabilityStatus() {
        return delegate().getClusterAvailabilityStatus();
    }

    @Override
    public void close() {
        flushExecutor.shutdownNow();
        delegate.close();
    }

    @VisibleForTesting
    boolean hasBeenCleared(TableReference tableRef) {
        return clearedTables.contains(tableRef);
    }

    // This way of recording the number of writes to tables is obviously not
    // completely correct. It does no synchronization between processes (so
    // updates could be clobbered), and it makes little effort to ensure that
    // all updates are flushed. It is intended only to be "good enough" for
    // determining what tables have been written to a lot.
    private void recordModifications(int newWrites) {
        totalModifications.addAndGet(newWrites);
    }

    private void recordModificationsSize(long modificationSize) {
        totalModificationsSize.addAndGet(modificationSize);
    }

    private void recordClear(TableReference tableRef) {
        clearedTables.add(tableRef);
        recordModifications(CLEAR_WEIGHT);
    }

    private Runnable createFlushTask() {
        return () -> {
            if (!shouldFlush()) {
                log.debug(
                        "Not flushing since the total number modifications is less than threshold — {} < {} "
                                + "— and total size of modifications is less than threshold — {} < {}",
                        SafeArg.of("total modification count", totalModifications),
                        SafeArg.of("count threshold", writeThreshold),
                        SafeArg.of("total modifications size", totalModificationsSize),
                        SafeArg.of("size threshold", writeSizeThreshold));
                return;
            }

            try {
                if (flushLock.tryLock()) {
                    try {
                        if (shouldFlush()) {
                            // snapshot current values while holding the lock and flush
                            totalModifications.set(0);
                            totalModificationsSize.set(0);
                            Multiset<TableReference> localWritesByTable = ImmutableMultiset.copyOf(writesByTable);
                            writesByTable.clear();
                            Set<TableReference> localClearedTables = ImmutableSet.copyOf(clearedTables);
                            clearedTables.clear();

                            // apply back pressure by only allowing one flush at a time
                            flushWrites(localWritesByTable, localClearedTables);
                        }
                    } finally {
                        flushLock.unlock();
                    }
                }
            } catch (Throwable t) {
                if (!Thread.interrupted()) {
                    log.warn("Error occurred while flushing sweep stats", t);
                }
            }
        };
    }

    private boolean shouldFlush() {
        return totalModifications.get() >= writeThreshold.get()
                || totalModificationsSize.get() >= writeSizeThreshold.get();
    }

    private void flushWrites(Multiset<TableReference> writes, Set<TableReference> clears) {
        if (writes.isEmpty() && clears.isEmpty()) {
            log.info("No writes to flush");
            return;
        }

        log.info(
                "Flushing stats for {} writes and {} clears",
                SafeArg.of("writes", writes.size()),
                SafeArg.of("clears", clears.size()));
        log.trace("Flushing writes: {}", UnsafeArg.of("writes", writes));
        log.trace("Flushing clears: {}", UnsafeArg.of("clears", clears));
        try {
            Set<TableReference> tableNames = Sets.difference(writes.elementSet(), clears);
            Collection<byte[]> rows = Collections2.transform(
                    Collections2.transform(tableNames, TableReference::getQualifiedName),
                    Functions.compose(Persistables.persistToBytesFunction(), SweepPriorityRow.fromFullTableNameFun()));
            Map<Cell, Value> oldWriteCounts = delegate()
                    .getRows(
                            SWEEP_PRIORITY_TABLE,
                            rows,
                            SweepPriorityTable.getColumnSelection(SweepPriorityNamedColumn.WRITE_COUNT),
                            Long.MAX_VALUE);
            Map<Cell, byte[]> newWriteCounts =
                    Maps.newHashMapWithExpectedSize(writes.elementSet().size());
            byte[] col = SweepPriorityNamedColumn.WRITE_COUNT.getShortName();
            for (TableReference tableRef : tableNames) {
                Preconditions.checkState(
                        !tableRef.getQualifiedName().startsWith(AtlasDbConstants.NAMESPACE_PREFIX),
                        "The sweep stats kvs should wrap the namespace mapping kvs, not the other way around.");
                byte[] row = SweepPriorityRow.of(tableRef.getQualifiedName()).persistToBytes();
                Cell cell = Cell.create(row, col);
                Value oldValue = oldWriteCounts.get(cell);
                long oldCount = oldValue == null || oldValue.getContents().length == 0
                        ? 0
                        : SweepPriorityTable.WriteCount.BYTES_HYDRATOR
                                .hydrateFromBytes(oldValue.getContents())
                                .getValue();
                long newValue = clears.contains(tableRef) ? writes.count(tableRef) : oldCount + writes.count(tableRef);
                log.debug("Sweep priority for {} has {} writes (was {})", tableRef, newValue, oldCount);
                newWriteCounts.put(
                        cell, SweepPriorityTable.WriteCount.of(newValue).persistValue());
            }
            long timestamp = timestampService.getFreshTimestamp();

            // Committing before writing is intentional, we want the start timestamp to
            // show up in the transaction table before we write do our writes.
            commit(timestamp);
            delegate().put(SWEEP_PRIORITY_TABLE, newWriteCounts, timestamp);
        } catch (RuntimeException e) {
            if (Thread.interrupted()) {
                return;
            }
            Set<TableReference> allTableNames = delegate().getAllTableNames();
            if (!allTableNames.contains(SWEEP_PRIORITY_TABLE)
                    || !allTableNames.contains(TransactionConstants.TRANSACTION_TABLE)) {
                // ignore problems when sweep or transaction tables don't exist
                log.warn("Ignoring failed sweep stats flush due to ", e);
            }
            log.warn("Unable to flush sweep stats for writes {} and clears {}: ", writes, clears, e);
            throw e;
        }
    }

    private void commit(long timestamp) {
        Cell cell = Cell.create(
                TransactionConstants.getValueForTimestamp(timestamp), TransactionConstants.COMMIT_TS_COLUMN);
        byte[] value = TransactionConstants.getValueForTimestamp(timestamp);
        delegate().putUnlessExists(TransactionConstants.TRANSACTION_TABLE, ImmutableMap.of(cell, value));
    }
}
