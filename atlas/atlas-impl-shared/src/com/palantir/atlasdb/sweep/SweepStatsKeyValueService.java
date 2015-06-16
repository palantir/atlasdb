// Copyright 2015 Palantir Technologies
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.palantir.atlasdb.sweep;

import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

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
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.KeyAlreadyExistsException;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.keyvalue.impl.ForwardingKeyValueService;
import com.palantir.atlasdb.schema.generated.SweepPriorityTable;
import com.palantir.atlasdb.schema.generated.SweepPriorityTable.SweepPriorityNamedColumn;
import com.palantir.atlasdb.schema.generated.SweepPriorityTable.SweepPriorityRow;
import com.palantir.atlasdb.transaction.impl.TransactionConstants;
import com.palantir.common.persist.Persistables;
import com.palantir.timestamp.TimestampService;

/**
 * This kvs wrapper tracks the approximate number of writes to every table
 * since the last time the table was completely swept. This is used when
 * deciding the order in which tables should be swept.
 */
public class SweepStatsKeyValueService extends ForwardingKeyValueService {
    private static final int CLEAR_WEIGHT = 1 << 14;
    private static final int WRITE_THRESHOLD = 1 << 16;
    private final KeyValueService delegate;
    private final TimestampService timestampService;
    private final Multiset<String> writesByTable = ConcurrentHashMultiset.create();
    private final Set<String> clearedTables = Sets.newHashSet();
    private final AtomicInteger totalModifications = new AtomicInteger();

    public SweepStatsKeyValueService(KeyValueService delegate,
                                     TimestampService timestampService) {
        this.delegate = delegate;
        this.timestampService = timestampService;
    }

    @Override
    protected KeyValueService delegate() {
        return delegate;
    }

    @Override
    public void put(String tableName, Map<Cell, byte[]> values, long timestamp)
            throws KeyAlreadyExistsException {
        delegate().put(tableName, values, timestamp);
        writesByTable.add(tableName, values.size());
        maybeFlushWrites(values.size());
    }

    @Override
    public void multiPut(Map<String, ? extends Map<Cell, byte[]>> valuesByTable, long timestamp) {
        delegate().multiPut(valuesByTable, timestamp);
        int newWrites = 0;
        for (Entry<String, ? extends Map<Cell, byte[]>> entry : valuesByTable.entrySet()) {
            writesByTable.add(entry.getKey(), entry.getValue().size());
            newWrites += entry.getValue().size();
        }
        maybeFlushWrites(newWrites);
    }

    @Override
    public void putWithTimestamps(String tableName, Multimap<Cell, Value> cellValues) {
        delegate().putWithTimestamps(tableName, cellValues);
        writesByTable.add(tableName, cellValues.size());
    }

    @Override
    public void delete(String tableName, Multimap<Cell, Long> keys) {
        delegate().delete(tableName, keys);
        writesByTable.add(tableName, keys.size());
        maybeFlushWrites(keys.size());
    }

    @Override
    public void truncateTable(String tableName) {
        delegate().truncateTable(tableName);
        clearedTables.add(tableName);
        maybeFlushWrites(CLEAR_WEIGHT);
    }

    @Override
    public void truncateTables(Set<String> tableNames) {
        delegate().truncateTables(tableNames);
        clearedTables.addAll(tableNames);
        maybeFlushWrites(CLEAR_WEIGHT * tableNames.size());
    }

    @Override
    public void dropTable(String tableName) {
        delegate().dropTable(tableName);
        clearedTables.add(tableName);
        maybeFlushWrites(CLEAR_WEIGHT);
    }

    @Override
    public void close() {
        maybeFlushWrites(WRITE_THRESHOLD);
        delegate().close();
    }

    // This way of recording the number of writes to tables is obviously not
    // completely correct. It does no synchronization between processes (so
    // updates could be clobbered), and it makes little effort to ensure that
    // all updates are flushed. It is intended only to be "good enough" for
    // determining what tables have been written to a lot.

    private void maybeFlushWrites(int newWrites) {
        Multiset<String> localWritesByTable = null;
        Set<String> localClearedTables = null;
        synchronized (this) {
            if (totalModifications.addAndGet(newWrites) >= WRITE_THRESHOLD) {
                totalModifications.set(0);
                localWritesByTable = ImmutableMultiset.copyOf(writesByTable);
                writesByTable.clear();
                localClearedTables = ImmutableSet.copyOf(clearedTables);
                clearedTables.clear();
            }
        }
        if (localWritesByTable != null) {
            try {
                flushWrites(localWritesByTable, localClearedTables);
            } catch (RuntimeException e) {
                if (delegate().getAllTableNames().contains(SweepPriorityTable.getTableName()) &&
                        delegate().getAllTableNames().contains(TransactionConstants.TRANSACTION_TABLE)) {
                    // ignore problems when the sweep table or transaction table doesn't exist
                    throw e;
                }
            }
        }
    }

    private synchronized void flushWrites(Multiset<String> writes, Set<String> clears) {
        if (writes.isEmpty() && clears.isEmpty()) {
            return;
        }
        Set<String> tableNames = Sets.difference(writes.elementSet(), clears);
        Iterable<byte[]> rows = Collections2.transform(tableNames,
                Functions.compose(Persistables.persistToBytesFunction(), SweepPriorityRow.fromFullTableNameFun()));
        Map<Cell, Value> oldWriteCounts = delegate().getRows(SweepPriorityTable.getTableName(), rows,
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
            newWriteCounts.put(cell, SweepPriorityTable.WriteCount.of(newValue).persistValue());
        }
        long timestamp = timestampService.getFreshTimestamp();

        // Committing before writing is intentional, we want the start timestamp to
        // show up in the transaction table before we write do our writes.
        commit(timestamp);
        delegate().put(SweepPriorityTable.getTableName(), newWriteCounts, timestamp);
    }

    private void commit(long timestamp) {
        Cell cell = Cell.create(
                TransactionConstants.getValueForTimestamp(timestamp),
                TransactionConstants.COMMIT_TS_COLUMN);
        byte[] value = TransactionConstants.getValueForTimestamp(timestamp);
        delegate().putUnlessExists(TransactionConstants.TRANSACTION_TABLE, ImmutableMap.of(cell, value));
    }
}
