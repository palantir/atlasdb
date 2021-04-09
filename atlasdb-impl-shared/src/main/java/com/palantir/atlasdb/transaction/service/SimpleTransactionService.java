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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.palantir.atlasdb.futures.AtlasFutures;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.logging.DefaultSensitiveLoggingArgProducers;
import com.palantir.atlasdb.logging.LoggingArgs;
import com.palantir.atlasdb.transaction.encoding.TicketsEncodingStrategy;
import com.palantir.atlasdb.transaction.encoding.TimestampEncodingStrategy;
import com.palantir.atlasdb.transaction.encoding.V1EncodingStrategy;
import com.palantir.atlasdb.transaction.impl.TransactionConstants;
import java.util.HashMap;
import java.util.Map;

public final class SimpleTransactionService implements EncodingTransactionService {
    private final KeyValueService kvs;
    private final TimestampEncodingStrategy encodingStrategy;
    private final TableReference transactionsTable;

    // The maximum key-value store timestamp (exclusive) at which data is stored
    // in transaction table.
    // All entries in transaction table are stored with timestamp 0
    private static final long MAX_TIMESTAMP = 1L;
    private final AsyncCellGetter immediateAsyncCellGetter;
    private final AsyncCellGetter asyncCellGetter;

    private SimpleTransactionService(
            KeyValueService kvs, TimestampEncodingStrategy encodingStrategy, TableReference transactionsTable) {
        this.kvs = kvs;
        this.encodingStrategy = encodingStrategy;
        this.transactionsTable = transactionsTable;
        this.immediateAsyncCellGetter = startTsMap -> Futures.immediateFuture(kvs.get(transactionsTable, startTsMap));
        this.asyncCellGetter = startTsMap -> kvs.getAsync(transactionsTable, startTsMap);
    }

    public static SimpleTransactionService createV1(KeyValueService kvs) {
        SimpleTransactionService v1Service =
                new SimpleTransactionService(kvs, V1EncodingStrategy.INSTANCE, TransactionConstants.TRANSACTION_TABLE);
        LoggingArgs.registerSensitiveLoggingArgProducerForTable(
                TransactionConstants.TRANSACTION_TABLE, DefaultSensitiveLoggingArgProducers.ALWAYS_SAFE);
        return v1Service;
    }

    public static SimpleTransactionService createV2(KeyValueService kvs) {
        SimpleTransactionService v2Service = new SimpleTransactionService(
                kvs, TicketsEncodingStrategy.INSTANCE, TransactionConstants.TRANSACTIONS2_TABLE);
        LoggingArgs.registerSensitiveLoggingArgProducerForTable(
                TransactionConstants.TRANSACTIONS2_TABLE, DefaultSensitiveLoggingArgProducers.ALWAYS_SAFE);
        return v2Service;
    }

    @Override
    public Long get(long startTimestamp) {
        return AtlasFutures.getUnchecked(getInternal(startTimestamp, immediateAsyncCellGetter));
    }

    @Override
    public Map<Long, Long> get(Iterable<Long> startTimestamps) {
        return AtlasFutures.getUnchecked(getInternal(startTimestamps, immediateAsyncCellGetter));
    }

    @Override
    public ListenableFuture<Long> getAsync(long startTimestamp) {
        return getInternal(startTimestamp, asyncCellGetter);
    }

    @Override
    public ListenableFuture<Map<Long, Long>> getAsync(Iterable<Long> startTimestamps) {
        return getInternal(startTimestamps, asyncCellGetter);
    }

    @Override
    public void putUnlessExists(long startTimestamp, long commitTimestamp) {
        Cell key = getTransactionCell(startTimestamp);
        byte[] value = encodingStrategy.encodeCommitTimestampAsValue(startTimestamp, commitTimestamp);
        kvs.putUnlessExists(transactionsTable, ImmutableMap.of(key, value));
    }

    @Override
    public void putUnlessExistsMultiple(Map<Long, Long> startTimestampToCommitTimestamp) {
        Map<Cell, byte[]> values = new HashMap<>(startTimestampToCommitTimestamp.size());
        startTimestampToCommitTimestamp.forEach((start, commit) ->
                values.put(getTransactionCell(start), encodingStrategy.encodeCommitTimestampAsValue(start, commit)));
        kvs.putUnlessExists(transactionsTable, values);
    }

    private Cell getTransactionCell(long startTimestamp) {
        return encodingStrategy.encodeStartTimestampAsCell(startTimestamp);
    }

    @Override
    public TimestampEncodingStrategy getEncodingStrategy() {
        return encodingStrategy;
    }

    @Override
    public void close() {
        // we do not close the injected kvs
    }

    private ListenableFuture<Long> getInternal(long startTimestamp, AsyncCellGetter cellGetter) {
        Cell cell = getTransactionCell(startTimestamp);
        return Futures.transform(
                cellGetter.get(ImmutableMap.of(cell, MAX_TIMESTAMP)),
                returnMap -> decodeTimestamp(startTimestamp, cell, returnMap),
                MoreExecutors.directExecutor());
    }

    private ListenableFuture<Map<Long, Long>> getInternal(Iterable<Long> startTimestamps, AsyncCellGetter cellGetter) {
        Map<Cell, Long> startTsMap = new HashMap<>();
        for (Long startTimestamp : startTimestamps) {
            Cell cell = getTransactionCell(startTimestamp);
            startTsMap.put(cell, MAX_TIMESTAMP);
        }

        return Futures.transform(cellGetter.get(startTsMap), this::decodeTimestamps, MoreExecutors.directExecutor());
    }

    private Long decodeTimestamp(long startTimestamp, Cell cell, Map<Cell, Value> rawResults) {
        if (rawResults.containsKey(cell)) {
            return encodingStrategy.decodeValueAsCommitTimestamp(
                    startTimestamp, rawResults.get(cell).getContents());
        }
        return null;
    }

    private Map<Long, Long> decodeTimestamps(Map<Cell, Value> rawResults) {
        Map<Long, Long> result = Maps.newHashMapWithExpectedSize(rawResults.size());
        for (Map.Entry<Cell, Value> e : rawResults.entrySet()) {
            long startTs = encodingStrategy.decodeCellAsStartTimestamp(e.getKey());
            long commitTs = encodingStrategy.decodeValueAsCommitTimestamp(
                    startTs, e.getValue().getContents());
            result.put(startTs, commitTs);
        }
        return result;
    }

    private interface AsyncCellGetter {
        ListenableFuture<Map<Cell, Value>> get(Map<Cell, Long> startTsMap);
    }
}
