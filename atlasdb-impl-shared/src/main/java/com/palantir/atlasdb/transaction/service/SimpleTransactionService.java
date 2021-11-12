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
package com.palantir.atlasdb.transaction.service;

import com.google.common.util.concurrent.ListenableFuture;
import com.palantir.atlasdb.futures.AtlasFutures;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.pue.PutUnlessExistsTable;
import com.palantir.atlasdb.pue.ResilientCommitTimestampPutUnlessExistsTable;
import com.palantir.atlasdb.pue.SimpleCommitTimestampPutUnlessExistsTable;
import com.palantir.atlasdb.transaction.encoding.TicketsEncodingStrategy;
import com.palantir.atlasdb.transaction.encoding.TimestampEncodingStrategy;
import com.palantir.atlasdb.transaction.encoding.V1EncodingStrategy;
import com.palantir.atlasdb.transaction.impl.TransactionConstants;
import java.util.Map;

public final class SimpleTransactionService implements EncodingTransactionService {
    private final PutUnlessExistsTable<Long, Long> txnTable;
    private final TimestampEncodingStrategy<?> encodingStrategy;

    private SimpleTransactionService(
            PutUnlessExistsTable<Long, Long> txnTable, TimestampEncodingStrategy<?> encodingStrategy) {
        this.encodingStrategy = encodingStrategy;
        this.txnTable = txnTable;
    }

    public static SimpleTransactionService createV1(KeyValueService kvs) {
        return new SimpleTransactionService(
                new SimpleCommitTimestampPutUnlessExistsTable(
                        kvs, TransactionConstants.TRANSACTION_TABLE, V1EncodingStrategy.INSTANCE),
                V1EncodingStrategy.INSTANCE);
    }

    public static SimpleTransactionService createV2(KeyValueService kvs) {
        return new SimpleTransactionService(
                new SimpleCommitTimestampPutUnlessExistsTable(
                        kvs, TransactionConstants.TRANSACTIONS2_TABLE, TicketsEncodingStrategy.INSTANCE),
                TicketsEncodingStrategy.INSTANCE);
    }

    public static SimpleTransactionService createV3Simple(KeyValueService kvs) {
        return new SimpleTransactionService(
                new SimpleCommitTimestampPutUnlessExistsTable(
                        kvs, TransactionConstants.TRANSACTIONS3_TABLE, TicketsEncodingStrategy.INSTANCE),
                TicketsEncodingStrategy.INSTANCE);
    }

    public static SimpleTransactionService createV3Complex(KeyValueService kvs) {
        PutUnlessExistsTable<Long, Long> table = new ResilientCommitTimestampPutUnlessExistsTable(null, null);
        return new SimpleTransactionService(table, null);
    }

    @Override
    public Long get(long startTimestamp) {
        return AtlasFutures.getUnchecked(getInternal(startTimestamp));
    }

    @Override
    public Map<Long, Long> get(Iterable<Long> startTimestamps) {
        return AtlasFutures.getUnchecked(getInternal(startTimestamps));
    }

    @Override
    public ListenableFuture<Long> getAsync(long startTimestamp) {
        return getInternal(startTimestamp);
    }

    @Override
    public ListenableFuture<Map<Long, Long>> getAsync(Iterable<Long> startTimestamps) {
        return getInternal(startTimestamps);
    }

    @Override
    public void putUnlessExists(long startTimestamp, long commitTimestamp) {
        txnTable.putUnlessExists(startTimestamp, commitTimestamp);
    }

    @Override
    public void putUnlessExistsMultiple(Map<Long, Long> startTimestampToCommitTimestamp) {
        txnTable.putUnlessExistsMultiple(startTimestampToCommitTimestamp);
    }

    private Cell getTransactionCell(long startTimestamp) {
        return encodingStrategy.encodeStartTimestampAsCell(startTimestamp);
    }

    @Override
    public TimestampEncodingStrategy<?> getEncodingStrategy() {
        return encodingStrategy;
    }

    @Override
    public void close() {
        // we do not close the injected kvs
    }

    private ListenableFuture<Long> getInternal(long startTimestamp) {
        return txnTable.get(startTimestamp);
    }

    private ListenableFuture<Map<Long, Long>> getInternal(Iterable<Long> startTimestamps) {
        return txnTable.get(startTimestamps);
    }
}
