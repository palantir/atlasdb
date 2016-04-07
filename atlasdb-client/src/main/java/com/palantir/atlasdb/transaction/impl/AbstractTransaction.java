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
package com.palantir.atlasdb.transaction.impl;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.primitives.UnsignedBytes;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.keyvalue.impl.Cells;
import com.palantir.atlasdb.transaction.api.Transaction;
import com.palantir.atlasdb.transaction.api.TransactionFailedException;
import com.palantir.atlasdb.transaction.service.TransactionService;
import com.palantir.common.annotation.Idempotent;

public abstract class AbstractTransaction implements Transaction {
    final static int TEMP_TABLE_IN_MEMORY_BYTES_LIMIT = 2 * 1024 * 1024; // 2MB

    protected static final ImmutableSortedMap<byte[], RowResult<byte[]>> EMPTY_SORTED_ROWS =
            ImmutableSortedMap.<byte[], RowResult<byte[]>> orderedBy(
                    UnsignedBytes.lexicographicalComparator()).build();

    private final Set<String> tempTables = Sets.newSetFromMap(Maps.<String, Boolean> newConcurrentMap());
    private final AtomicInteger tempTableCounter = new AtomicInteger();
    private final ConcurrentMap<String, AtomicLong> inMemTempSizeByTable = Maps.newConcurrentMap();
    private final ConcurrentMap<String, AtomicLong> batchNumber = Maps.newConcurrentMap();

    private TransactionType transactionType = TransactionType.DEFAULT;

    @Override
    @Idempotent
    final public TransactionType getTransactionType() {
        return transactionType;
    }

    @Override
    @Idempotent
    final public void setTransactionType(TransactionType transactionType) {
        this.transactionType = transactionType;
    }

    @Override
    final public void delete(String tableName, Set<Cell> cells) {
        put(tableName, Cells.constantValueMap(cells, PtBytes.EMPTY_BYTE_ARRAY));
    }

    public boolean isTempTable(String tableName) {
        return tempTables.contains(tableName);
    }

    protected Set<String> getAllTempTables() {
        return Collections.unmodifiableSet(tempTables);
    }

    protected void dropTempTables() {
        getKeyValueService().dropTables(tempTables);
    }

    private AtomicLong getTempSize(String tableName) {
        AtomicLong atomicLong = inMemTempSizeByTable.get(tableName);
        if (atomicLong == null) {
            inMemTempSizeByTable.putIfAbsent(tableName, new AtomicLong());
            atomicLong = inMemTempSizeByTable.get(tableName);
        }
        return atomicLong;
    }

    private long getNewBatchNumber(String tableName) {
        AtomicLong atomicLong = batchNumber.get(tableName);
        if (atomicLong == null) {
            batchNumber.putIfAbsent(tableName, new AtomicLong());
            atomicLong = batchNumber.get(tableName);
        }
        long ret = atomicLong.getAndIncrement();
        if (ret >= getTimestamp()) {
            throw new IllegalStateException("There have been more writes to this temp table than "
                                          + "all other transactions combined");
        }
        return ret;
    }

    protected void putTempTableWrites(String tableName,
                                      Map<Cell, byte[]> values,
                                      ConcurrentNavigableMap<Cell, byte[]> writes) {
        long addedSize = 0;
        for (Map.Entry<Cell, byte[]> e : values.entrySet()) {
            byte[] val = e.getValue();
            if (val == null) {
                val = PtBytes.EMPTY_BYTE_ARRAY;
            }
            byte[] prevVal = writes.put(e.getKey(), e.getValue());
            if (prevVal == null) {
                addedSize += val.length + Cells.getApproxSizeOfCell(e.getKey());
            } else {
                addedSize += val.length - prevVal.length;
            }
        }
        AtomicLong inMemSize = getTempSize(tableName);
        inMemSize.addAndGet(addedSize);

        if (inMemSize.get() > TEMP_TABLE_IN_MEMORY_BYTES_LIMIT) {
            synchronized (inMemSize) {
                if (inMemSize.get() > TEMP_TABLE_IN_MEMORY_BYTES_LIMIT) {
                    pushWritesToKvService(tableName, inMemSize, writes);
                }
            }
        }
    }

    private void pushWritesToKvService(String tableName,
                                       AtomicLong inMemSize,
                                       ConcurrentNavigableMap<Cell, byte[]> writes) {
        long removedSize = 0;
        ImmutableMap<Cell, byte[]> toWrite = ImmutableMap.copyOf(writes);
        getKeyValueService().put(tableName, toWrite, getNewBatchNumber(tableName));
        for (Map.Entry<Cell, byte[]> e : toWrite.entrySet()) {
            if (writes.remove(e.getKey(), e.getValue())) {
                removedSize += e.getValue().length + Cells.getApproxSizeOfCell(e.getKey());
            }
        }
        inMemSize.addAndGet(-removedSize);
    }

    protected abstract KeyValueService getKeyValueService();

    @Override
    public void commit(TransactionService txService) throws TransactionFailedException {
        commit();
    }
}
