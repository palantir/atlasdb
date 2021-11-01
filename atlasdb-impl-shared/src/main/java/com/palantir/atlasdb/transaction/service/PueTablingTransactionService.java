/*
 * (c) Copyright 2021 Palantir Technologies Inc. All rights reserved.
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

import com.google.common.collect.Maps;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.palantir.atlasdb.futures.AtlasFutures;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.KeyAlreadyExistsException;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.pue.ComplexPutUnlessExistsTable;
import com.palantir.atlasdb.keyvalue.pue.DirectPutUnlessExistsTable;
import com.palantir.atlasdb.keyvalue.pue.PutUnlessExistsTable;
import com.palantir.atlasdb.transaction.encoding.TicketsEncodingStrategy;
import com.palantir.atlasdb.transaction.encoding.TimestampEncodingStrategy;
import com.palantir.atlasdb.transaction.impl.TransactionConstants;
import com.palantir.common.streams.KeyedStream;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.jetbrains.annotations.Nullable;

public final class PueTablingTransactionService implements CellEncodingTransactionService {
    private final PutUnlessExistsTable transactionsTable;
    private final TimestampEncodingStrategy encodingStrategy;

    private PueTablingTransactionService(
            PutUnlessExistsTable transactionsTable, TimestampEncodingStrategy encodingStrategy) {
        this.transactionsTable = transactionsTable;
        this.encodingStrategy = encodingStrategy;
    }

    public static CellEncodingTransactionService createV3(KeyValueService keyValueService) {
        return new PueTablingTransactionService(
                getPutUnlessExistsTable(keyValueService), TicketsEncodingStrategy.INSTANCE);
    }

    private static PutUnlessExistsTable getPutUnlessExistsTable(KeyValueService keyValueService) {
        if (keyValueService.checkAndSetMayPersistPartialValuesOnFailure()) {
            return ComplexPutUnlessExistsTable.create(
                    keyValueService,
                    TransactionConstants.TRANSACTIONS3_TABLE,
                    _unused -> TicketsEncodingStrategy.INSTANCE.encodeCommitTimestampAsValue(
                            0, TransactionConstants.FAILED_COMMIT_TS)); // TODO (jkong): 0 is naughty
        }
        return new DirectPutUnlessExistsTable(keyValueService, TransactionConstants.TRANSACTIONS3_TABLE);
    }

    @Override
    public ListenableFuture<Long> getAsync(long startTimestamp) {
        return getInternal(startTimestamp);
    }

    @Override
    public ListenableFuture<Map<Long, Long>> getAsync(Iterable<Long> startTimestamps) {
        return getInternal(startTimestamps);
    }

    @Nullable
    @Override
    public Long get(long startTimestamp) {
        return AtlasFutures.getUnchecked(getInternal(startTimestamp));
    }

    @Override
    public Map<Long, Long> get(Iterable<Long> startTimestamps) {
        return AtlasFutures.getUnchecked(getInternal(startTimestamps));
    }

    @Override
    public void putUnlessExists(long startTimestamp, long commitTimestamp) throws KeyAlreadyExistsException {
        Cell key = getTransactionCell(startTimestamp);
        byte[] value = encodingStrategy.encodeCommitTimestampAsValue(startTimestamp, commitTimestamp);
        transactionsTable.putUnlessExists(key, value);
    }

    @Override
    public void putUnlessExistsMultiple(Map<Long, Long> startTimestampToCommitTimestamp) {
        transactionsTable.putUnlessExistsMultiple(KeyedStream.stream(startTimestampToCommitTimestamp)
                .map(encodingStrategy::encodeCommitTimestampAsValue)
                .mapKeys(encodingStrategy::encodeStartTimestampAsCell)
                .collectToMap());
    }

    @Override
    public void close() {}

    private Cell getTransactionCell(long startTimestamp) {
        return encodingStrategy.encodeStartTimestampAsCell(startTimestamp);
    }

    private ListenableFuture<Long> getInternal(long startTimestamp) {
        Cell cell = getTransactionCell(startTimestamp);
        return Futures.transform(
                transactionsTable.get(cell),
                returnValue -> decodeTimestamp(startTimestamp, returnValue),
                MoreExecutors.directExecutor());
    }

    private Long decodeTimestamp(long startTimestamp, Optional<byte[]> returnValue) {
        return returnValue
                .map(bytes -> encodingStrategy.decodeValueAsCommitTimestamp(startTimestamp, bytes))
                .orElse(null);
    }

    private ListenableFuture<Map<Long, Long>> getInternal(Iterable<Long> startTimestamps) {
        Set<Cell> cells = new HashSet<>();
        for (Long startTimestamp : startTimestamps) {
            Cell cell = getTransactionCell(startTimestamp);
            cells.add(cell);
        }

        return Futures.transform(transactionsTable.get(cells), this::decodeTimestamps, MoreExecutors.directExecutor());
    }

    private Map<Long, Long> decodeTimestamps(Map<Cell, byte[]> rawResults) {
        Map<Long, Long> result = Maps.newHashMapWithExpectedSize(rawResults.size());
        for (Map.Entry<Cell, byte[]> e : rawResults.entrySet()) {
            long startTs = encodingStrategy.decodeCellAsStartTimestamp(e.getKey());
            long commitTs = encodingStrategy.decodeValueAsCommitTimestamp(startTs, e.getValue());
            result.put(startTs, commitTs);
        }
        return result;
    }

    @Override
    public TimestampEncodingStrategy getCellEncodingStrategy() {
        return encodingStrategy;
    }
}
