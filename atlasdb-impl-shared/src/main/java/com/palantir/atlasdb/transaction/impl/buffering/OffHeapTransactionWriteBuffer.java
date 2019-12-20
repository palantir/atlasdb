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
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.MoreObjects;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Multimap;
import com.google.common.primitives.Bytes;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.impl.Cells;
import com.palantir.atlasdb.local.storage.api.PersistentStore;
import com.palantir.atlasdb.local.storage.api.PersistentStore.Serializer;
import com.palantir.atlasdb.local.storage.api.PersistentStore.StoreNamespace;
import com.palantir.atlasdb.local.storage.api.TransactionWriteBuffer;
import com.palantir.atlasdb.table.description.ValueType;
import com.palantir.atlasdb.transaction.impl.TransactionConstants;
import com.palantir.logsafe.SafeArg;

public final class OffHeapTransactionWriteBuffer implements TransactionWriteBuffer {
    private static final Logger log = LoggerFactory.getLogger(OffHeapTransactionWriteBuffer.class);
    @VisibleForTesting
    static final Serializer<Cell, byte[]> DEFAULT_SERIALIZER = new Serializer<Cell, byte[]>() {
        @Override
        public byte[] serializeKey(Cell key) {
            byte[] encodedRow = ValueType.SIZED_BLOB.convertFromJava(key.getRowName());
            return Bytes.concat(encodedRow, key.getColumnName());
        }

        @Override
        public Cell deserializeKey(byte[] key) {
            byte[] rowName = (byte[]) ValueType.SIZED_BLOB.convertToJava(key, 0);
            int offset = ValueType.VAR_LONG.sizeOf((long) rowName.length) + rowName.length;
            return Cell.create(rowName, (byte[]) ValueType.BLOB.convertToJava(key, offset));
        }

        @Override
        public byte[] serializeValue(Cell key, byte[] value) {
            return value;
        }

        @Override
        public byte[] deserializeValue(Cell key, byte[] value) {
            return value;
        }
    };

    private final PersistentStore persistentStore;
    private final ConcurrentHashMap<TableReference, StoreNamespace<Cell, byte[]>> tableMappings =
            new ConcurrentHashMap<>();
    private final AtomicLong byteCount = new AtomicLong();
    private volatile boolean hasWrites = false;

    public static TransactionWriteBuffer create(PersistentStore persistentStore) {
        return new OffHeapTransactionWriteBuffer(persistentStore);
    }

    OffHeapTransactionWriteBuffer(PersistentStore persistentStore) {
        this.persistentStore = persistentStore;
    }

    @Override
    public long byteCount() {
        return byteCount.get();
    }

    @Override
    public void putWrites(TableReference tableRef, Map<Cell, byte[]> values) {
        StoreNamespace<Cell, byte[]> tableStoreNamespace = storeNamespace(tableRef);
        hasWrites = !values.isEmpty() || hasWrites;
        for (Map.Entry<Cell, byte[]> e : values.entrySet()) {
            byte[] val = MoreObjects.firstNonNull(e.getValue(), PtBytes.EMPTY_BYTE_ARRAY);
            Cell cell = e.getKey();
            byte[] previous = persistentStore.get(tableStoreNamespace, cell);
            persistentStore.put(tableStoreNamespace, cell, val);
            hasWrites = true;
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
        tableMappings.forEach((tableReference, storeNamespace) ->
                postCondition.accept(tableReference, writesByStoreNamespace(storeNamespace)));
    }

    @Override
    public Collection<Cell> writtenCells(TableReference tableRef) {
        return getCells(storeNamespace(tableRef));
    }

    @Override
    public Iterable<TableReference> tablesWrittenTo() {
        return tableMappings.keySet();
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

    @Override
    public void flush(Consumer<Map<TableReference, ? extends Map<Cell, byte[]>>> sink) {
        ImmutableMap.Builder<TableReference, Map<Cell, byte[]>> builder = ImmutableMap.builder();
        tableMappings.forEach((tableReference, storeNamespace) ->
                builder.put(tableReference, writesByStoreNamespace(storeNamespace)));
        sink.accept(builder.build());
    }

    private SortedMap<Cell, byte[]> writesByStoreNamespace(StoreNamespace<Cell, byte[]> storeNamespace) {
        return persistentStore.loadNamespaceEntries(storeNamespace);
    }

    private Collection<Cell> getCells(StoreNamespace<Cell, byte[]> storeNamespace) {
        return persistentStore.loadNamespaceKeys(storeNamespace);
    }

    private StoreNamespace<Cell, byte[]> storeNamespace(TableReference tableRef) {
        return tableMappings.computeIfAbsent(
                tableRef,
                $ -> persistentStore.createNamespace(tableRef.getQualifiedName(), DEFAULT_SERIALIZER));
    }
}
