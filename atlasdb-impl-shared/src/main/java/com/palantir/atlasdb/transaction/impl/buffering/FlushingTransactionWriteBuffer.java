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
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.IntSupplier;
import java.util.function.Supplier;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.local.storage.api.TransactionWriteBuffer;

public final class FlushingTransactionWriteBuffer implements TransactionWriteBuffer {
    private final IntSupplier flushingLimit;
    private final AtomicReference<TransactionWriteBuffer> levelOne;
    private final TransactionWriteBuffer levelTwo;
    private final Supplier<TransactionWriteBuffer> levelOneFactory;

    public static TransactionWriteBuffer create(
            IntSupplier flushingLimit,
            Supplier<TransactionWriteBuffer> levelOneFactory,
            TransactionWriteBuffer levelTwo) {
        return new FlushingTransactionWriteBuffer(flushingLimit, levelOneFactory.get(), levelTwo, levelOneFactory);
    }

    public FlushingTransactionWriteBuffer(
            IntSupplier flushingLimit,
            TransactionWriteBuffer levelOne,
            TransactionWriteBuffer levelTwo,
            Supplier<TransactionWriteBuffer> levelOneFactory) {
        this.flushingLimit = flushingLimit;
        this.levelOne = new AtomicReference<>(levelOne);
        this.levelTwo = levelTwo;
        this.levelOneFactory = levelOneFactory;
    }


    @Override
    public long byteCount() {
        return levelOne.get().byteCount() + levelTwo.byteCount();
    }

    @Override
    public void putWrites(TableReference tableRef, Map<Cell, byte[]> values) {
        levelOne.get().putWrites(tableRef, values);
        int limit = flushingLimit.getAsInt();
        if (levelOne.get().byteCount() >= limit) {
            TransactionWriteBuffer toBeFlushed = null;
            synchronized (this) {
                if (levelOne.get().byteCount() >= limit) {
                    toBeFlushed = levelOne.get();
                    levelOne.set(levelOneFactory.get());
                }
            }
            if (toBeFlushed != null) {
                toBeFlushed.flush(writes -> writes.forEach(levelTwo::putWrites));
            }
        }
    }

    @Override
    public void applyPostCondition(BiConsumer<TableReference, Map<Cell, byte[]>> postCondition) {
        tablesWrittenTo().forEach(tableReference ->
                postCondition.accept(tableReference, writesByTable(tableReference)));
    }

    @Override
    public Collection<Cell> writtenCells(TableReference tableRef) {
        return ImmutableSet.<Cell>builder()
                .addAll(levelOne.get().writtenCells(tableRef))
                .addAll(levelTwo.writtenCells(tableRef))
                .build();
    }

    @Override
    public Iterable<TableReference> tablesWrittenTo() {
        return ImmutableSet.<TableReference>builder()
                .addAll(levelOne.get().tablesWrittenTo())
                .addAll(levelTwo.tablesWrittenTo())
                .build();
    }

    @Override
    public SortedMap<Cell, byte[]> writesByTable(TableReference tableRef) {
        SortedMap<Cell, byte[]> levelTwoWrites = levelTwo.writesByTable(tableRef);

        levelOne.get().writesByTable(tableRef)
                .forEach((cell, bytes) -> levelTwoWrites.merge(cell, bytes, (oldVal, newVal) -> newVal));

        return levelTwoWrites;
    }

    @Override
    public boolean hasWrites() {
        return levelOne.get().hasWrites() || levelTwo.hasWrites();
    }

    @Override
    public Multimap<Cell, TableReference> cellsToScrubByCell() {
        Multimap<Cell, TableReference> combined = HashMultimap.create();
        combined.putAll(levelTwo.cellsToScrubByCell());
        combined.putAll(levelOne.get().cellsToScrubByCell());
        return combined;
    }

    @Override
    public Multimap<TableReference, Cell> cellsToScrubByTable() {
        Multimap<TableReference, Cell> combined = HashMultimap.create();
        combined.putAll(levelTwo.cellsToScrubByTable());
        combined.putAll(levelOne.get().cellsToScrubByTable());
        return combined;
    }

    @Override
    public void flush(Consumer<Map<TableReference, ? extends Map<Cell, byte[]>>> sink) {
        levelTwo.flush(writes ->
                writes.forEach((tableReference, tableWrites) -> levelOne.get().putWrites(tableReference, tableWrites)));
        levelOne.get().flush(sink);
    }
}
