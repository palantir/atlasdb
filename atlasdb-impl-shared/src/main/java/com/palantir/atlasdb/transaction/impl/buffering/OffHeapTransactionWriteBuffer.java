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
import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.MoreObjects;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.Multimap;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.impl.Cells;
import com.palantir.atlasdb.off.heap.PersistentTimestampStore;
import com.palantir.atlasdb.off.heap.PersistentTimestampStore.StoreNamespace;
import com.palantir.atlasdb.transaction.impl.TransactionConstants;
import com.palantir.logsafe.SafeArg;

public final class OffHeapTransactionWriteBuffer implements TransactionWriteBuffer {
    private static final Logger log = LoggerFactory.getLogger(OffHeapTransactionWriteBuffer.class);
    private static final ImmutableSortedMap<Cell, byte[]> EMPTY_WRITES = ImmutableSortedMap.of();

    private final PersistentTimestampStore persistentTimestampStore;
    private final ConcurrentHashMap<TableReference, StoreNamespace> tableMappings = new ConcurrentHashMap<>();
    private final AtomicLong byteCount = new AtomicLong();
    private volatile boolean hasWrites = false;

    OffHeapTransactionWriteBuffer(PersistentTimestampStore persistentTimestampStore) {
        this.persistentTimestampStore = persistentTimestampStore;
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
            StoreNamespace tableStoreNamespace = storeNamespace(tableRef);
            // TODO change this
            Long previous = persistentTimestampStore.get(tableStoreNamespace, 1L);
            persistentTimestampStore.put(tableStoreNamespace, 1L, 2L);
            hasWrites = true;
            // end of changing
            if (previous == null) {
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


    @Override
    public void applyPostCondition(BiConsumer<TableReference, Map<Cell, byte[]>> postCondition) {

    }

    @Override
    public Collection<Cell> writtenCells(TableReference tableRef) {
        // TODO change this
        return getCells(storeNamespace(tableRef));
    }


    @Override
    public Iterable<TableReference> tablesWrittenTo() {
        return tableMappings.keySet();
    }

    @Override
    public Map<TableReference, ? extends Map<Cell, byte[]>> all() {
        ImmutableMap.Builder<TableReference, Map<Cell, byte[]>> builder = ImmutableMap.builder();
        tableMappings.forEach(((tableReference, storeNamespace) ->
                builder.put(tableReference, writesByStoreNamespace(storeNamespace))));
        return builder.build();
    }

    @Override
    public SortedMap<Cell, byte[]> writesByTable(TableReference tableRef) {
        return writesByStoreNamespace(storeNamespace(tableRef));
    }


    @Override
    public boolean hasWrites() {
        return hasWrites;
    }

    @Override
    public Multimap<Cell, TableReference> cellsToScrubByCell() {
        Multimap<Cell, TableReference> cellToTableName = HashMultimap.create();
        tableMappings.forEach((tableReference, storeNamespace) -> {
            Collection<Cell> cells = writtenCells(tableReference);
            for (Cell cell : cells) {
                cellToTableName.put(cell, tableReference);
            }

        });
        return cellToTableName;
    }

    @Override
    public Multimap<TableReference, Cell> cellsToScrubByTable() {
        Multimap<TableReference, Cell> tableRefToCells = HashMultimap.create();
        tableMappings.forEach((tableReference, storeNamespace) ->
                tableRefToCells.putAll(tableReference, getCells(storeNamespace)));
        return tableRefToCells;
    }

    private SortedMap<Cell, byte[]> writesByStoreNamespace(StoreNamespace storeNamespace) {
        return persistentTimestampStore.loadTableData(storeNamespace);
    }

    private Collection<Cell> getCells(StoreNamespace storeNamespace) {
        return persistentTimestampStore.loadAllKeys(storeNamespace);
    }

    private StoreNamespace storeNamespace(TableReference tableRef) {
        return tableMappings.computeIfAbsent(
                tableRef,
                $ -> persistentTimestampStore.createNamespace(tableRef.getQualifiedName()));
    }
}
