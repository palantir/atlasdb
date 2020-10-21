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
package com.palantir.atlasdb.schema;

import com.google.common.base.Function;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.google.common.io.BaseEncoding;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.KeyAlreadyExistsException;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.RangeRequests;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.impl.Cells;
import com.palantir.atlasdb.transaction.api.Transaction;
import com.palantir.atlasdb.transaction.api.TransactionManager;
import com.palantir.atlasdb.transaction.impl.TransactionConstants;
import com.palantir.common.annotation.Output;
import com.palantir.common.base.AbortingVisitor;
import com.palantir.common.base.AbortingVisitors;
import com.palantir.common.base.BatchingVisitable;
import com.palantir.common.collect.Maps2;
import com.palantir.util.Mutable;
import com.palantir.util.Mutables;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang3.mutable.MutableLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KvsRangeMigrator implements RangeMigrator {
    private static final Logger log = LoggerFactory.getLogger(KvsRangeMigrator.class);

    private final TableReference srcTable;
    private final TableReference destTable;
    private final int readBatchSize;
    private final TransactionManager readTxManager;
    private final TransactionManager txManager;
    private final KeyValueService writeKvs;
    private final long migrationTimestamp;
    private final AbstractTaskCheckpointer checkpointer;
    private final Function<RowResult<byte[]>, Map<Cell, byte[]>> rowTransform;

    KvsRangeMigrator(
            TableReference srcTable,
            TableReference destTable,
            int readBatchSize,
            TransactionManager readTxManager,
            TransactionManager txManager,
            KeyValueService writeKvs,
            long migrationTimestamp,
            AbstractTaskCheckpointer checkpointer,
            Function<RowResult<byte[]>, Map<Cell, byte[]>> rowTransform) {
        this.srcTable = srcTable;
        this.destTable = destTable;
        this.readBatchSize = readBatchSize;
        this.readTxManager = readTxManager;
        this.txManager = txManager;
        this.writeKvs = writeKvs;
        this.migrationTimestamp = migrationTimestamp;
        this.checkpointer = checkpointer;
        this.rowTransform = rowTransform;
    }

    @Override
    public void logStatus(int numRangeBoundaries) {
        txManager.runTaskWithRetry(transaction -> {
            logStatus(transaction, numRangeBoundaries);
            return null;
        });
    }

    private void logStatus(Transaction tx, int numRangeBoundaries) {
        for (int rangeId = 0; rangeId < numRangeBoundaries; rangeId++) {
            byte[] checkpoint = getCheckpoint(rangeId, tx);
            if (checkpoint != null) {
                log.info(
                        "({}/{}) Migration from table {} to table {} will start/resume at {}",
                        rangeId,
                        numRangeBoundaries,
                        srcTable,
                        destTable,
                        PtBytes.encodeHexString(checkpoint));
                return;
            }
        }
        log.info("Migration from table {} to {} has already been completed", srcTable, destTable);
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

    private byte[] copyOneTransaction(final RangeRequest range, final long rangeId) {
        return txManager.runTaskWithRetry(writeT -> copyOneTransactionFromReadTxManager(range, rangeId, writeT));
    }

    private byte[] copyOneTransactionFromReadTxManager(
            final RangeRequest range, final long rangeId, final Transaction writeT) {
        if (readTxManager == txManager) {
            // don't wrap
            return copyOneTransactionInternal(range, rangeId, writeT, writeT);
        } else {
            // read only, but need to use a write tx in case the source table has SweepStrategy.THOROUGH
            return readTxManager.runTaskWithRetry(readT -> copyOneTransactionInternal(range, rangeId, readT, writeT));
        }
    }

    private byte[] copyOneTransactionInternal(RangeRequest range, long rangeId, Transaction readT, Transaction writeT) {
        final long maxBytes = TransactionConstants.WARN_LEVEL_FOR_QUEUED_BYTES / 2;
        byte[] start = getCheckpoint(rangeId, writeT);
        if (start == null) {
            return null;
        }
        RangeRequest.Builder builder = range.getBuilder().startRowInclusive(start);
        if (builder.isInvalidRange()) {
            return null;
        }
        RangeRequest rangeToUse = builder.build();
        if (log.isTraceEnabled()) {
            log.trace(
                    "Copying table {} range {} from {}  to {}",
                    srcTable,
                    rangeId,
                    BaseEncoding.base16().lowerCase().encode(rangeToUse.getStartInclusive()),
                    BaseEncoding.base16().lowerCase().encode(rangeToUse.getEndExclusive()));
        }

        BatchingVisitable<RowResult<byte[]>> bv = readT.getRange(srcTable, rangeToUse);

        Map<Cell, byte[]> writeMap = new HashMap<>();
        byte[] lastRow = internalCopyRange(bv, maxBytes, writeMap);
        if (log.isTraceEnabled() && (lastRow != null)) {
            log.trace("Copying {} bytes for range {} on table {}", lastRow.length, rangeId, srcTable);
        }
        writeToKvs(writeMap);

        byte[] nextRow = getNextRowName(lastRow);
        checkpointer.checkpoint(srcTable.getQualifiedName(), rangeId, nextRow, writeT);

        return lastRow;
    }

    private byte[] getCheckpoint(long rangeId, Transaction writeT) {
        return checkpointer.getCheckpoint(srcTable.getQualifiedName(), rangeId, writeT);
    }

    protected void writeToKvs(Map<Cell, byte[]> writeMap) {
        try {
            writeKvs.put(destTable, writeMap, migrationTimestamp);
        } catch (KeyAlreadyExistsException e) {
            retryWriteToKvs(writeMap);
        }
    }

    protected void retryWriteToKvs(Map<Cell, byte[]> writeMap) {
        Multimap<Cell, Long> keys =
                Multimaps.forMap(Maps2.createConstantValueMap(writeMap.keySet(), migrationTimestamp));
        writeKvs.delete(destTable, keys);
        writeKvs.put(destTable, writeMap, migrationTimestamp);
    }

    private byte[] getNextRowName(byte[] lastRow) {
        if (isRangeDone(lastRow)) {
            return new byte[0];
        }
        return RangeRequests.nextLexicographicName(lastRow);
    }

    private byte[] internalCopyRange(
            BatchingVisitable<RowResult<byte[]>> bv, final long maxBytes, @Output final Map<Cell, byte[]> writeMap) {
        final Mutable<byte[]> lastRowName = Mutables.newMutable(null);
        final MutableLong bytesPut = new MutableLong(0L);
        bv.batchAccept(
                readBatchSize,
                AbortingVisitors.batching(
                        // Replacing this with a lambda results in an unreported exception compile error
                        // even though no exception can be thrown :-(
                        new AbortingVisitor<RowResult<byte[]>, RuntimeException>() {
                            @Override
                            public boolean visit(RowResult<byte[]> rr) throws RuntimeException {
                                return KvsRangeMigrator.this.internalCopyRow(
                                        rr, maxBytes, writeMap, bytesPut, lastRowName);
                            }
                        }));
        return lastRowName.get();
    }

    private boolean internalCopyRow(
            RowResult<byte[]> rr,
            long maxBytes,
            @Output Map<Cell, byte[]> writeMap,
            @Output MutableLong bytesPut,
            @Output Mutable<byte[]> lastRowName) {
        Map<Cell, byte[]> values = rowTransform.apply(rr);
        writeMap.putAll(values);

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
