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

package com.palantir.atlasdb.transaction.impl.buffering;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.MoreObjects;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.impl.Cells;
import com.palantir.atlasdb.transaction.impl.TransactionConstants;
import com.palantir.logsafe.SafeArg;

public final class DefaultTransactionWriteBuffer implements TransactionWriteBuffer {
    private static final Logger log = LoggerFactory.getLogger(DefaultTransactionWriteBuffer.class);
    private final ConcurrentMap<TableReference, ConcurrentNavigableMap<Cell, byte[]>> writesByTable;
    private final AtomicLong byteCount = new AtomicLong();

    public DefaultTransactionWriteBuffer(
            ConcurrentMap<TableReference, ConcurrentNavigableMap<Cell, byte[]>> writesByTable) {
        this.writesByTable = writesByTable;
    }

    @Override
    public long byteCount() {
        return byteCount.get();
    }

    @Override
    public void putWrites(TableReference tableRef, Map<Cell, byte[]> values) {
        for (Map.Entry<Cell, byte[]> e : values.entrySet()) {
            byte[] val = MoreObjects.firstNonNull(e.getValue(), PtBytes.EMPTY_BYTE_ARRAY);
            Cell cell = e.getKey();
            if (writesByTableWithInitialization(tableRef).put(cell, val) == null) {
                long toAdd = val.length + Cells.getApproxSizeOfCell(cell);
                byteCount.addAndGet(toAdd);
                if (toAdd >= TransactionConstants.WARN_LEVEL_FOR_QUEUED_BYTES && log.isDebugEnabled()) {
                    log.debug("Single cell write increases the transaction write size over 10MB",
                            SafeArg.of("writeSize", toAdd),
                            new RuntimeException());
                }
            }
        }
    }

    private SortedMap<Cell, byte[]> writesByTableWithInitialization(TableReference tableRef) {
        return writesByTable.computeIfAbsent(tableRef, unused -> new ConcurrentSkipListMap<>());
    }

    @Override
    public void applyPostCondition(BiConsumer<TableReference, Map<Cell, byte[]>> postCondition) {
        writesByTable.forEach(postCondition);
    }

    @Override
    public Collection<Cell> writtenCells(TableReference tableRef) {
        return writesByTable(tableRef).keySet();
    }

    @Override
    public Iterable<TableReference> tablesWrittenTo() {
        return writesByTable.keySet();
    }

    @Override
    public Map<TableReference, ? extends Map<Cell, byte[]>> all() {
        return writesByTable;
    }

    @Override
    public SortedMap<Cell, byte[]> writesByTable(TableReference tableRef) {
        return Collections.unmodifiableSortedMap(writesByTableWithInitialization(tableRef));
    }

    @Override
    public boolean hasWrites() {
        return writesByTable.values().stream().anyMatch(writesForTable -> !writesForTable.isEmpty());
    }

    @Override
    public Multimap<Cell, TableReference> cellsToScrubByCell() {
        Multimap<Cell, TableReference> cellToTableName = HashMultimap.create();
        for (Map.Entry<TableReference, ConcurrentNavigableMap<Cell, byte[]>> entry : writesByTable.entrySet()) {
            TableReference table = entry.getKey();
            Set<Cell> cells = entry.getValue().keySet();
            for (Cell c : cells) {
                cellToTableName.put(c, table);
            }
        }
        return cellToTableName;
    }

    @Override
    public Multimap<TableReference, Cell> cellsToScrubByTable() {
        Multimap<TableReference, Cell> tableRefToCells = HashMultimap.create();
        for (Map.Entry<TableReference, ConcurrentNavigableMap<Cell, byte[]>> entry : writesByTable.entrySet()) {
            TableReference table = entry.getKey();
            Set<Cell> cells = entry.getValue().keySet();
            tableRefToCells.putAll(table, cells);
        }
        return tableRefToCells;
    }
}
