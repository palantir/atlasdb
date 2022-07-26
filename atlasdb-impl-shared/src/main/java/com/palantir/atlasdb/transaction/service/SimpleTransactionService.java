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
import com.palantir.atlasdb.atomic.*;
import com.palantir.atlasdb.futures.AtlasFutures;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.transaction.encoding.CellEncodingStrategy;
import com.palantir.atlasdb.transaction.encoding.TicketsEncodingStrategy;
import com.palantir.atlasdb.transaction.encoding.TimestampEncodingStrategy;
import com.palantir.atlasdb.transaction.encoding.TwoPhaseEncodingStrategy;
import com.palantir.atlasdb.transaction.encoding.V1EncodingStrategy;
import com.palantir.atlasdb.transaction.impl.TransactionConstants;
import com.palantir.tritium.metrics.registry.TaggedMetricRegistry;
import java.util.Map;
import java.util.function.Supplier;

public final class SimpleTransactionService implements EncodingTransactionService {
    private final AtomicTable<Long, Long> txnTable;
    private final TimestampEncodingStrategy<?> encodingStrategy;

    private SimpleTransactionService(AtomicTable<Long, Long> txnTable, TimestampEncodingStrategy<?> encodingStrategy) {
        this.encodingStrategy = encodingStrategy;
        this.txnTable = txnTable;
    }

    public static SimpleTransactionService createV1(KeyValueService kvs) {
        return createSimple(kvs, TransactionConstants.TRANSACTION_TABLE, V1EncodingStrategy.INSTANCE);
    }

    public static SimpleTransactionService createV2(KeyValueService kvs) {
        return createSimple(kvs, TransactionConstants.TRANSACTIONS2_TABLE, TicketsEncodingStrategy.INSTANCE);
    }

    public static SimpleTransactionService createV3(
            KeyValueService kvs, TaggedMetricRegistry metricRegistry, Supplier<Boolean> acceptStagingReadsAsCommitted) {
        if (kvs.getCheckAndSetCompatibility().consistentOnFailure()) {
            return createSimple(kvs, TransactionConstants.TRANSACTIONS2_TABLE, TicketsEncodingStrategy.INSTANCE);
        }
        return createResilient(
                kvs,
                TransactionConstants.TRANSACTIONS2_TABLE,
                TwoPhaseEncodingStrategy.INSTANCE,
                metricRegistry,
                acceptStagingReadsAsCommitted);
    }

    private static SimpleTransactionService createSimple(
            KeyValueService kvs, TableReference tableRef, TimestampEncodingStrategy<Long> encodingStrategy) {
        AtomicTable<Long, Long> pueTable = new SimpleCommitTimestampAtomicTable(kvs, tableRef, encodingStrategy);
        return new SimpleTransactionService(pueTable, encodingStrategy);
    }

    private static SimpleTransactionService createResilient(
            KeyValueService kvs,
            TableReference tableRef,
            TwoPhaseEncodingStrategy encodingStrategy,
            TaggedMetricRegistry metricRegistry,
            Supplier<Boolean> acceptStagingReadsAsCommitted) {
        ConsensusForgettingStore store = InstrumentedConsensusForgettingStore.create(
                new PueKvsConsensusForgettingStore(kvs, tableRef), metricRegistry);
        AtomicTable<Long, Long> pueTable = new ResilientCommitTimestampAtomicTable(
                store, encodingStrategy, acceptStagingReadsAsCommitted, metricRegistry);
        return new SimpleTransactionService(pueTable, encodingStrategy);
    }

    @Override
    public Long get(long startTimestamp) {
        return AtlasFutures.getUnchecked(getAsync(startTimestamp));
    }

    @Override
    public Map<Long, Long> get(Iterable<Long> startTimestamps) {
        return AtlasFutures.getUnchecked(getAsync(startTimestamps));
    }

    @Override
    public ListenableFuture<Long> getAsync(long startTimestamp) {
        return txnTable.get(startTimestamp);
    }

    @Override
    public ListenableFuture<Map<Long, Long>> getAsync(Iterable<Long> startTimestamps) {
        return txnTable.get(startTimestamps);
    }

    @Override
    public void commit(long startTimestamp, long commitTimestamp) {
        txnTable.update(startTimestamp, commitTimestamp);
    }

    @Override
    public void commitMultiple(Map<Long, Long> startTimestampToCommitTimestamp) {
        txnTable.updateMultiple(startTimestampToCommitTimestamp);
    }

    @Override
    public CellEncodingStrategy getCellEncodingStrategy() {
        return encodingStrategy;
    }

    @Override
    public void close() {
        // we do not close the injected kvs
    }
}
