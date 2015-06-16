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

package com.palantir.atlasdb.schema;

import java.util.Map;

import org.apache.commons.lang.mutable.MutableLong;

import com.google.common.base.Function;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.RangeRequests;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.keyvalue.impl.Cells;
import com.palantir.atlasdb.transaction.api.Transaction;
import com.palantir.atlasdb.transaction.api.TransactionManager;
import com.palantir.atlasdb.transaction.api.TransactionTask;
import com.palantir.atlasdb.transaction.impl.TransactionConstants;
import com.palantir.common.annotation.Output;
import com.palantir.common.base.AbortingVisitor;
import com.palantir.common.base.AbortingVisitors;
import com.palantir.common.base.BatchingVisitable;
import com.palantir.util.Mutable;
import com.palantir.util.Mutables;

public class TransactionRangeMigrator implements RangeMigrator {
    private final String srcTable;
    private final String destTable;
    private final int readBatchSize;
    private final TransactionManager readTxManager;
    private final TransactionManager txManager;
    private final AbstractTaskCheckpointer checkpointer;
    private final Function<RowResult<byte[]>, Map<Cell, byte[]>> rowTransform;

    TransactionRangeMigrator(String srcTable,
                             String destTable,
                             int readBatchSize,
                             TransactionManager readTxManager,
                             TransactionManager txManager,
                             AbstractTaskCheckpointer checkpointer,
                             Function<RowResult<byte[]>, Map<Cell, byte[]>> rowTransform) {
        this.srcTable = srcTable;
        this.destTable = destTable;
        this.readBatchSize = readBatchSize;
        this.readTxManager = readTxManager;
        this.txManager = txManager;
        this.checkpointer = checkpointer;
        this.rowTransform = rowTransform;
    }

    @Override
    public void migrateRange(RangeRequest range, long rangeId) {
        byte[] lastRow;
        do {
            lastRow = copyOneTransaction(range, rangeId);
        } while (!isRangeDone(lastRow));
    }

    private boolean isRangeDone(byte[] row) {
        return row == null || RangeRequests.isLastRowName(row);
    }

    /**
     * The write transaction wraps the read transaction because the write transaction can be
     * aborted, and the read transaction should be new.
     */
    private byte[] copyOneTransaction(final RangeRequest range, final long rangeId) {
        return txManager.runTaskWithRetry(new TransactionTask<byte[], RuntimeException>() {
            @Override
            public byte[] execute(final Transaction writeT) {
                return copyOneTransactionWithReadTransaction(range, rangeId, writeT);
            }
        });
    }

    private byte[] copyOneTransactionWithReadTransaction(final RangeRequest range,
                                                         final long rangeId,
                                                         final Transaction writeT) {
        if (readTxManager == txManager) {
            // don't wrap
            return copyOneTransactionInternal(range, rangeId, writeT, writeT);
        } else {
            return readTxManager.runTaskReadOnly(new TransactionTask<byte[], RuntimeException>() {
                @Override
                public byte[] execute(Transaction readT) {
                    return copyOneTransactionInternal(range, rangeId, readT, writeT);
                }
            });
        }
    }

    private byte[] copyOneTransactionInternal(RangeRequest range,
                                              long rangeId,
                                              Transaction readT,
                                              Transaction writeT) {
        final long maxBytes = TransactionConstants.WARN_LEVEL_FOR_QUEUED_BYTES / 2;
        byte[] start = checkpointer.getCheckpoint(srcTable, rangeId, writeT);
        if (start == null) {
            return null;
        }
        RangeRequest.Builder builder = range.getBuilder().startRowInclusive(start);
        if (builder.isInvalidRange()) {
            return null;
        }
        RangeRequest rangeToUse = builder.build();

        BatchingVisitable<RowResult<byte[]>> bv = readT.getRange(srcTable, rangeToUse);

        byte[] lastRow = internalCopyRange(bv, maxBytes, writeT);

        byte[] nextRow = getNextRowName(lastRow);
        checkpointer.checkpoint(srcTable, rangeId, nextRow, writeT);

        return lastRow;
    }

    private byte[] getNextRowName(byte[] lastRow) {
        if (isRangeDone(lastRow)) {
            return new byte[0];
        }
        return RangeRequests.nextLexicographicName(lastRow);
    }

    private byte[] internalCopyRange(BatchingVisitable<RowResult<byte[]>> bv,
                                     final long maxBytes,
                                     final Transaction t) {
        final Mutable<byte[]> lastRowName = Mutables.newMutable(null);
        final MutableLong bytesPut = new MutableLong(0L);
        bv.batchAccept(readBatchSize, AbortingVisitors.batching(
                new AbortingVisitor<RowResult<byte[]>, RuntimeException>() {
            @Override
            public boolean visit(RowResult<byte[]> rr) {
                return internalCopyRow(rr, maxBytes, t, bytesPut, lastRowName);
            }
        }));
        return lastRowName.get();
    }

    private boolean internalCopyRow(RowResult<byte[]> rr,
                                    long maxBytes,
                                    Transaction writeT,
                                    @Output MutableLong bytesPut,
                                    @Output Mutable<byte[]> lastRowName) {
        Map<Cell, byte[]> values = rowTransform.apply(rr);
        writeT.put(destTable, values);

        for (Map.Entry<Cell, byte[]> e : values.entrySet()) {
            bytesPut.add(e.getValue().length + Cells.getApproxSizeOfCell(e.getKey()));
        }

        if (bytesPut.longValue() >= maxBytes) {
            lastRowName.set(rr.getRowName());
            return false;
        }
        return true;
    }
}
