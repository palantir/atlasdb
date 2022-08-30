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

package com.palantir.atlasdb.transaction.knowledge;

import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.palantir.atlasdb.atomic.AtomicTable;
import com.palantir.atlasdb.transaction.impl.TransactionConstants;
import com.palantir.atlasdb.transaction.service.TransactionStatus;
import com.palantir.atlasdb.transaction.service.TransactionStatuses;
import com.palantir.common.streams.KeyedStream;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;

import java.util.Comparator;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;

// only meant for txn4
public final class KnowledgeableTransactionService {
    private final AtomicTable<Long, TransactionStatus> delegate;
    private final KnownConcludedTransactions knownConcludedTransactions;
    private final KnownAbortedTransactions knownAbortedTransactions;
    private final Supplier<Long> lastSeenCommitTs;

    public KnowledgeableTransactionService(AtomicTable<Long, TransactionStatus> delegate, KnownConcludedTransactions knownConcludedTransactions, KnownAbortedTransactions knownAbortedTransactions, Supplier<Long> lastSeenCommitTs) {
        this.delegate = delegate;
        this.knownConcludedTransactions = knownConcludedTransactions;
        this.knownAbortedTransactions = knownAbortedTransactions;
        this.lastSeenCommitTs = lastSeenCommitTs;
    }

    TransactionStatus get(long startTimestamp) {
        if (knownConcludedTransactions.isKnownConcluded(startTimestamp, KnownConcludedTransactions.Consistency.LOCAL_READ)) {
            return getTransactionStatusSchema4(startTimestamp);
        } else {
            Map<Long, TransactionStatus> results = Futures.getUnchecked(delegate.get(ImmutableSet.of(startTimestamp)));
            if (results.containsKey(startTimestamp)) {
                return results.get(startTimestamp);
            } else {
                knownConcludedTransactions.isKnownConcluded(startTimestamp, KnownConcludedTransactions.Consistency.REMOTE_READ);
                return getTransactionStatusSchema4(startTimestamp);
            }
        }
    }

    private TransactionStatus getTransactionStatusSchema4(long startTimestamp) {
        if (knownAbortedTransactions.isKnownAborted(startTimestamp)) {
            return TransactionConstants.ABORTED;
        }
        // todo(snanda): we need to do this for read only and regular transactions :(
        // for read-only we need to ask sweep for last seen commit ts
        if (read-only) {
            long commitTs = lastSeenCommitTs.get();
            if (commitTs < startTimestamp) {
                return TransactionStatuses.committed(commitTs);
            } else {
                throw new SafeIllegalStateException("i do not if I can see this value.");
            }
        } else {
            // have to return unknown here as we do not know the commit ts BUT
            return TransactionStatuses.unknown();
        }
    }

    Map<Long, TransactionStatus> get(Set<Long> startTimestamps) {
        long maxStartTs = startTimestamps.stream().max(Comparator.naturalOrder()).orElse(0L);
        if (knownConcludedTransactions.isKnownConcluded(maxStartTs, KnownConcludedTransactions.Consistency.LOCAL_READ)) {
            return KeyedStream.of(startTimestamps).map(this::getTransactionStatusSchema4).collectToMap();
        } else {
            Map<Long, TransactionStatus> txnTableResult = Futures.getUnchecked(delegate.get(startTimestamps));
            Set<Long> missingInTxnTable = startTimestamps.stream().filter(ts -> !txnTableResult.containsKey(ts)).collect(Collectors.toSet());
            if (missingInTxnTable.isEmpty()) {
                return txnTableResult;
            }
            knownConcludedTransactions.isKnownConcluded(maxStartTs, KnownConcludedTransactions.Consistency.REMOTE_READ);
            return KeyedStream.of(startTimestamps).map(ts -> txnTableResult.computeIfAbsent(ts, this::getTransactionStatusSchema4)).collectToMap();
        }
    }
}
