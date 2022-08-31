/*
 * (c) Copyright 2022 Palantir Technologies Inc. All rights reserved.
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

import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.palantir.atlasdb.atomic.AtomicTable;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.transaction.impl.TransactionConstants;
import com.palantir.atlasdb.transaction.impl.TransactionStatusUtils;
import com.palantir.atlasdb.transaction.knowledge.DefaultKnownAbortedTransactions;
import com.palantir.atlasdb.transaction.knowledge.KnownAbortedTransactions;
import com.palantir.atlasdb.transaction.knowledge.KnownConcludedTransactions;
import com.palantir.atlasdb.transaction.knowledge.KnownConcludedTransactionsImpl;
import com.palantir.atlasdb.transaction.knowledge.KnownConcludedTransactionsStore;
import com.palantir.common.streams.KeyedStream;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;
import com.palantir.tritium.metrics.registry.TaggedMetricRegistry;
import java.util.Comparator;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public final class KnowledgeableTransactionService implements AsyncTransactionService {
    // this is timestamp extracting
    private final AtomicTable<Long, Long> txnTable;
    private final KnownConcludedTransactions knownConcludedTransactions;
    private final KnownAbortedTransactions knownAbortedTransactions;
    private final Supplier<Long> lastSeenCommitTs;
    private final boolean readOnly;

    public KnowledgeableTransactionService(
            AtomicTable<Long, Long> atomicTable,
            KeyValueService kvs,
            TaggedMetricRegistry metricRegistry,
            boolean readOnly) {
        this.txnTable = atomicTable;
        this.knownConcludedTransactions =
                KnownConcludedTransactionsImpl.create(KnownConcludedTransactionsStore.create(kvs), metricRegistry);
        this.readOnly = readOnly;

        // todo(snanda): the wiring is pending
        this.knownAbortedTransactions = DefaultKnownAbortedTransactions.create(
                knownConcludedTransactions, null, metricRegistry, Optional.empty());
        this.lastSeenCommitTs = null;
    }

    @Override
    public ListenableFuture<Long> getAsync(long startTimestamp) {
        if (knownConcludedTransactions.isKnownConcluded(
                startTimestamp, KnownConcludedTransactions.Consistency.LOCAL_READ)) {
            Optional<Long> maybeCommitTs =
                    TransactionStatusUtils.maybeGetCommitTs(startTimestamp, getTransactionStatus(startTimestamp));
            return maybeCommitTs.map(Futures::immediateFuture).orElseGet(() -> Futures.immediateFuture(null));
        } else {
            ListenableFuture<Map<Long, Long>> presentValuesFuture = txnTable.get(ImmutableSet.of(startTimestamp));
            return Futures.transform(
                    presentValuesFuture,
                    presentValues -> processReads(presentValues, startTimestamp),
                    MoreExecutors.directExecutor());
        }
    }

    @Override
    public ListenableFuture<Map<Long, Long>> getAsync(Iterable<Long> startTimestamps) {
        long maxStartTs = StreamSupport.stream(startTimestamps.spliterator(), false)
                .max(Comparator.naturalOrder())
                .orElse(TransactionConstants.LOWEST_POSSIBLE_START_TS - 1);
        if (knownConcludedTransactions.isKnownConcluded(
                maxStartTs, KnownConcludedTransactions.Consistency.LOCAL_READ)) {
            return Futures.immediateFuture(KeyedStream.of(startTimestamps)
                    .map(this::getCommitTs)
                    .filter(Objects::nonNull)
                    .collectToMap());
        } else {
            ListenableFuture<Map<Long, Long>> txnTableResult = txnTable.get(startTimestamps);
            return Futures.transform(
                    txnTableResult,
                    result -> {
                        Set<Long> missingInTxnTable = StreamSupport.stream(startTimestamps.spliterator(), false)
                                .filter(ts -> !result.containsKey(ts))
                                .collect(Collectors.toSet());
                        if (missingInTxnTable.isEmpty()) {
                            return result;
                        }
                        // if the value is not present in the transactions table, we know the transaction has been
                        // concluded.
                        knownConcludedTransactions.isKnownConcluded(
                                maxStartTs, KnownConcludedTransactions.Consistency.REMOTE_READ);
                        return KeyedStream.of(startTimestamps)
                                .map(ts -> result.computeIfAbsent(ts, this::getCommitTs))
                                .filter(Objects::nonNull)
                                .collectToMap();
                    },
                    MoreExecutors.directExecutor());
        }
    }

    private long processReads(Map<Long, Long> presentValues, long startTimestamp) {
        if (presentValues.containsKey(startTimestamp)) {
            return presentValues.get(startTimestamp);
        } else {
            // if the value is not present in the transactions table, we know the transaction has been concluded.
            knownConcludedTransactions.isKnownConcluded(
                    startTimestamp, KnownConcludedTransactions.Consistency.REMOTE_READ);
            return getCommitTs(startTimestamp);
        }
    }

    private long getCommitTs(long startTimestamp) {
        return TransactionStatusUtils.maybeGetCommitTs(startTimestamp, getTransactionStatus(startTimestamp))
                .orElse(null);
    }

    private TransactionStatus getTransactionStatus(long startTimestamp) {
        if (knownAbortedTransactions.isKnownAborted(startTimestamp)) {
            return TransactionConstants.ABORTED;
        }

        if (readOnly) {
            long commitTs = lastSeenCommitTs.get();
            if (commitTs < startTimestamp) {
                return TransactionStatuses.committed(commitTs);
            } else {
                throw new SafeIllegalStateException(
                        "Could not determine the values accessible to this read-only transaction. This can happen if"
                                + " the transaction has been alive for more than an hour and is expected to be"
                                + " transient.");
            }
        } else {
            return TransactionStatuses.unknown();
        }
    }
}
