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

import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.Maps;
import com.google.common.primitives.UnsignedBytes;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.keyvalue.api.TableReference;
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

    private final ConcurrentMap<TableReference, AtomicLong> inMemTempSizeByTable = Maps.newConcurrentMap();
    private final ConcurrentMap<TableReference, AtomicLong> batchNumber = Maps.newConcurrentMap();

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

    private AtomicLong getTempSize(TableReference tableRef) {
        AtomicLong atomicLong = inMemTempSizeByTable.get(tableRef);
        if (atomicLong == null) {
            inMemTempSizeByTable.putIfAbsent(tableRef, new AtomicLong());
            atomicLong = inMemTempSizeByTable.get(tableRef);
        }
        return atomicLong;
    }

    private long getNewBatchNumber(TableReference tableRef) {
        AtomicLong atomicLong = batchNumber.get(tableRef);
        if (atomicLong == null) {
            batchNumber.putIfAbsent(tableRef, new AtomicLong());
            atomicLong = batchNumber.get(tableRef);
        }
        long ret = atomicLong.getAndIncrement();
        if (ret >= getTimestamp()) {
            throw new IllegalStateException("There have been more writes to this temp table than "
                                          + "all other transactions combined");
        }
        return ret;
    }

    protected void putTempTableWrites(TableReference tableRef,
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
        AtomicLong inMemSize = getTempSize(tableRef);
        inMemSize.addAndGet(addedSize);

        if (inMemSize.get() > TEMP_TABLE_IN_MEMORY_BYTES_LIMIT) {
            synchronized (inMemSize) {
                if (inMemSize.get() > TEMP_TABLE_IN_MEMORY_BYTES_LIMIT) {
                    pushWritesToKvService(tableRef, inMemSize, writes);
                }
            }
        }
    }

    private void pushWritesToKvService(TableReference tableRef,
                                       AtomicLong inMemSize,
                                       ConcurrentNavigableMap<Cell, byte[]> writes) {
        long removedSize = 0;
        ImmutableMap<Cell, byte[]> toWrite = ImmutableMap.copyOf(writes);
        getKeyValueService().put(tableRef, toWrite, getNewBatchNumber(tableRef));
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
