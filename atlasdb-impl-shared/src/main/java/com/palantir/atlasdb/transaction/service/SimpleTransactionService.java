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
import com.palantir.atlasdb.atomic.AtomicTable;
import com.palantir.atlasdb.atomic.ConsensusForgettingStore;
import com.palantir.atlasdb.atomic.InstrumentedConsensusForgettingStore;
import com.palantir.atlasdb.atomic.KnowledgeableTimestampExtractingAtomicTable;
import com.palantir.atlasdb.atomic.PueConsensusForgettingStore;
import com.palantir.atlasdb.atomic.ResilientCommitTimestampAtomicTable;
import com.palantir.atlasdb.atomic.SimpleCommitTimestampAtomicTable;
import com.palantir.atlasdb.atomic.TimestampExtractingAtomicTable;
import com.palantir.atlasdb.atomic.mcas.MarkAndCasConsensusForgettingStore;
import com.palantir.atlasdb.futures.AtlasFutures;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.transaction.encoding.BaseProgressEncodingStrategy;
import com.palantir.atlasdb.transaction.encoding.CellEncodingStrategy;
import com.palantir.atlasdb.transaction.encoding.TicketsEncodingStrategy;
import com.palantir.atlasdb.transaction.encoding.TransactionStatusEncodingStrategy;
import com.palantir.atlasdb.transaction.encoding.TwoPhaseEncodingStrategy;
import com.palantir.atlasdb.transaction.encoding.V1EncodingStrategy;
import com.palantir.atlasdb.transaction.encoding.V4ProgressEncodingStrategy;
import com.palantir.atlasdb.transaction.impl.TransactionConstants;
import com.palantir.atlasdb.transaction.knowledge.TransactionKnowledgeComponents;
import com.palantir.tritium.metrics.registry.TaggedMetricRegistry;
import java.util.Map;
import java.util.function.Supplier;
import javax.annotation.CheckForNull;

public final class SimpleTransactionService implements EncodingTransactionService {
    private final AtomicTable<Long, Long> txnTable;
    private final AtomicTable<Long, TransactionStatus> txnTableV2;
    private final TransactionStatusEncodingStrategy<?> encodingStrategy;

    private SimpleTransactionService(
            AtomicTable<Long, Long> txnTable,
            AtomicTable<Long, TransactionStatus> txnTableV2,
            TransactionStatusEncodingStrategy<?> encodingStrategy) {
        this.encodingStrategy = encodingStrategy;
        this.txnTable = txnTable;
        this.txnTableV2 = txnTableV2;
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
                new TwoPhaseEncodingStrategy(BaseProgressEncodingStrategy.INSTANCE),
                metricRegistry,
                acceptStagingReadsAsCommitted);
    }

    public static SimpleTransactionService createV4(
            KeyValueService kvs,
            TransactionKnowledgeComponents knowledge,
            TaggedMetricRegistry metricRegistry,
            Supplier<Boolean> acceptStagingReadsAsCommitted) {
        if (kvs.getCheckAndSetCompatibility().consistentOnFailure()) {
            return createSimple(kvs, TransactionConstants.TRANSACTIONS2_TABLE, TicketsEncodingStrategy.INSTANCE);
        }
        return createKnowledgeableResilient(
                kvs,
                metricRegistry,
                TransactionConstants.TRANSACTIONS2_TABLE,
                new TwoPhaseEncodingStrategy(V4ProgressEncodingStrategy.INSTANCE),
                acceptStagingReadsAsCommitted,
                knowledge);
    }

    private static SimpleTransactionService createSimple(
            KeyValueService kvs,
            TableReference tableRef,
            TransactionStatusEncodingStrategy<TransactionStatus> encodingStrategy) {
        SimpleCommitTimestampAtomicTable delegate =
                new SimpleCommitTimestampAtomicTable(kvs, tableRef, encodingStrategy);
        AtomicTable<Long, Long> pueTable = new TimestampExtractingAtomicTable(delegate);
        return new SimpleTransactionService(pueTable, delegate, encodingStrategy);
    }

    private static SimpleTransactionService createResilient(
            KeyValueService kvs,
            TableReference tableRef,
            TwoPhaseEncodingStrategy encodingStrategy,
            TaggedMetricRegistry metricRegistry,
            Supplier<Boolean> acceptStagingReadsAsCommitted) {
        ResilientCommitTimestampAtomicTable delegate =
                getDelegate(kvs, tableRef, encodingStrategy, metricRegistry, acceptStagingReadsAsCommitted);
        AtomicTable<Long, Long> atomicTable = new TimestampExtractingAtomicTable(delegate);
        return new SimpleTransactionService(atomicTable, delegate, encodingStrategy);
    }

    private static SimpleTransactionService createKnowledgeableResilient(
            KeyValueService kvs,
            TaggedMetricRegistry metricRegistry,
            TableReference tableRef,
            TwoPhaseEncodingStrategy encodingStrategy,
            Supplier<Boolean> acceptStagingReadsAsCommitted,
            TransactionKnowledgeComponents knowledge) {
        AtomicTable<Long, TransactionStatus> delegate =
                getMcasDelegate(kvs, tableRef, encodingStrategy, metricRegistry, acceptStagingReadsAsCommitted);
        AtomicTable<Long, Long> atomicTable =
                new KnowledgeableTimestampExtractingAtomicTable(delegate, knowledge, metricRegistry);
        return new SimpleTransactionService(atomicTable, delegate, encodingStrategy);
    }

    private static ResilientCommitTimestampAtomicTable getDelegate(
            KeyValueService kvs,
            TableReference tableRef,
            TwoPhaseEncodingStrategy encodingStrategy,
            TaggedMetricRegistry metricRegistry,
            Supplier<Boolean> acceptStagingReadsAsCommitted) {
        ConsensusForgettingStore store = InstrumentedConsensusForgettingStore.create(
                new PueConsensusForgettingStore(kvs, tableRef), metricRegistry);
        return new ResilientCommitTimestampAtomicTable(
                store, encodingStrategy, acceptStagingReadsAsCommitted, metricRegistry);
    }

    private static ResilientCommitTimestampAtomicTable getMcasDelegate(
            KeyValueService kvs,
            TableReference tableRef,
            TwoPhaseEncodingStrategy encodingStrategy,
            TaggedMetricRegistry metricRegistry,
            Supplier<Boolean> acceptStagingReadsAsCommitted) {
        ConsensusForgettingStore store = InstrumentedConsensusForgettingStore.create(
                new MarkAndCasConsensusForgettingStore(TransactionConstants.TTS_IN_PROGRESS_MARKER, kvs, tableRef),
                metricRegistry);
        return new ResilientCommitTimestampAtomicTable(
                store, encodingStrategy, acceptStagingReadsAsCommitted, metricRegistry);
    }

    @Override
    @Deprecated
    public Long get(long startTimestamp) {
        return AtlasFutures.getUnchecked(getAsync(startTimestamp));
    }

    @Override
    @Deprecated
    public Map<Long, Long> get(Iterable<Long> startTimestamps) {
        return AtlasFutures.getUnchecked(getAsync(startTimestamps));
    }

    @CheckForNull
    @Override
    public TransactionStatus getV2(long startTimestamp) {
        return AtlasFutures.getUnchecked(getAsyncV2(startTimestamp));
    }

    @Override
    public Map<Long, TransactionStatus> getV2(Iterable<Long> startTimestamps) {
        return AtlasFutures.getUnchecked(getAsyncV2(startTimestamps));
    }

    @Override
    public void markInProgress(long startTimestamp) {
        txnTable.markInProgress(startTimestamp);
    }

    @Override
    public void markInProgress(Iterable<Long> startTimestamps) {
        txnTable.markInProgress(startTimestamps);
    }

    @Override
    @Deprecated
    public ListenableFuture<Long> getAsync(long startTimestamp) {
        return txnTable.get(startTimestamp);
    }

    @Override
    @Deprecated
    public ListenableFuture<Map<Long, Long>> getAsync(Iterable<Long> startTimestamps) {
        return txnTable.get(startTimestamps);
    }

    @Override
    public ListenableFuture<TransactionStatus> getAsyncV2(long startTimestamp) {
        return txnTableV2.get(startTimestamp);
    }

    @Override
    public ListenableFuture<Map<Long, TransactionStatus>> getAsyncV2(Iterable<Long> startTimestamps) {
        return txnTableV2.get(startTimestamps);
    }

    @Override
    public void putUnlessExists(long startTimestamp, long commitTimestamp) {
        txnTable.update(startTimestamp, commitTimestamp);
    }

    @Override
    public void putUnlessExists(Map<Long, Long> startTimestampToCommitTimestamp) {
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
