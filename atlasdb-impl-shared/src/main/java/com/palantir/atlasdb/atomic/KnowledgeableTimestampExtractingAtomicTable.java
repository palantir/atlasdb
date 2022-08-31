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

package com.palantir.atlasdb.atomic;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.palantir.atlasdb.futures.AtlasFutures;
import com.palantir.atlasdb.keyvalue.api.KeyAlreadyExistsException;
import com.palantir.atlasdb.transaction.impl.TransactionConstants;
import com.palantir.atlasdb.transaction.impl.TransactionStatusUtils;
import com.palantir.atlasdb.transaction.knowledge.KnownAbortedTransactions;
import com.palantir.atlasdb.transaction.knowledge.KnownConcludedTransactions;
import com.palantir.atlasdb.transaction.service.TransactionStatus;
import com.palantir.atlasdb.transaction.service.TransactionStatuses;
import com.palantir.common.streams.KeyedStream;
import java.util.Comparator;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.StreamSupport;

public class KnowledgeableTimestampExtractingAtomicTable implements AtomicTable<Long, Long> {
    private final AtomicTable<Long, TransactionStatus> delegate;
    private final KnownConcludedTransactions knownConcludedTransactions;
    private final KnownAbortedTransactions knownAbortedTransactions;
    private final Supplier<Long> lastSeenCommitTsSupplier;
    private final boolean readOnly;

    public KnowledgeableTimestampExtractingAtomicTable(
            AtomicTable<Long, TransactionStatus> delegate,
            KnownAbortedTransactions knownAbortedTransactions,
            KnownConcludedTransactions knownConcludedTransactions,
            Supplier<Long> lastSeenCommitTsSupplier,
            boolean readOnly) {
        this.delegate = delegate;
        this.knownConcludedTransactions = knownConcludedTransactions;
        this.knownAbortedTransactions = knownAbortedTransactions;
        this.lastSeenCommitTsSupplier = lastSeenCommitTsSupplier;
        this.readOnly = readOnly;
    }

    @Override
    public void markInProgress(Iterable<Long> keys) {
        delegate.markInProgress(keys);
    }

    @Override
    public void updateMultiple(Map<Long, Long> keyValues) throws KeyAlreadyExistsException {
        delegate.updateMultiple(KeyedStream.stream(keyValues)
                .map(TransactionStatusUtils::fromTimestamp)
                .collectToMap());
    }

    /**
     * Returns commit timestamp for the start timestamp supplied as arg.
     * For transactions that are successfully committed, returns the respective commit timestamps.
     * For transactions that are aborted, returns -1.
     * For transactions that are unknown, returns startTs as commitTs for read-write transactions.
     * For read-only transactions, only returns if the greatestSeenCommitTS < startTs, otherwise throws.
     * Start timestamps for transactions that are in progress are not included in the result.
     * */
    @Override
    public ListenableFuture<Long> get(Long startTimestamp) {
        return getInternal(startTimestamp, lastSeenCommitTsSupplier.get());
    }

    @Override
    public ListenableFuture<Map<Long, Long>> get(Iterable<Long> keys) {
        long lastSeenCommitTs = lastSeenCommitTsSupplier.get();
        Map<Long, ListenableFuture<Long>> futures = KeyedStream.of(
                        StreamSupport.stream(keys.spliterator(), false).sorted(Comparator.reverseOrder()))
                .map(startTs -> getInternal(startTs, lastSeenCommitTs))
                .collectToMap();
        return AtlasFutures.allAsMap(futures, MoreExecutors.directExecutor());
    }

    private ListenableFuture<Long> getInternal(long startTimestamp, long lastSeenCommitTs) {
        if (knownConcludedTransactions.isKnownConcluded(
                startTimestamp, KnownConcludedTransactions.Consistency.LOCAL_READ)) {
            return Futures.immediateFuture(getCommitTsForConcludedTransaction(startTimestamp, lastSeenCommitTs));
        } else {
            ListenableFuture<TransactionStatus> presentValueFuture = delegate.get(startTimestamp);
            return Futures.transform(
                    presentValueFuture,
                    presentValue -> getCommitTsFromStatus(startTimestamp, presentValue, lastSeenCommitTs),
                    MoreExecutors.directExecutor());
        }
    }

    private Long getCommitTsFromStatus(long startTs, TransactionStatus status, long lastSeenCommitTs) {
        if (status.equals(TransactionStatuses.unknown())) {
            knownConcludedTransactions.isKnownConcluded(startTs, KnownConcludedTransactions.Consistency.REMOTE_READ);
            return getCommitTsForConcludedTransaction(startTs, lastSeenCommitTs);
        } else {
            return TransactionStatusUtils.maybeGetCommitTs(status).orElse(null);
        }
    }

    private long getCommitTsForConcludedTransaction(long startTs, long lastSeenCommitTs) {
        if (knownAbortedTransactions.isKnownAborted(startTs)) {
            return TransactionConstants.FAILED_COMMIT_TS;
        }

        if (readOnly) {
            // For read-only transactions, we have chosen to be more defensive and return the
            // greatest commitTs seen by sweep so far.
            return lastSeenCommitTs;
        } else {
            // For read-write transactions, unknown would mean that the transaction table has been swept for startTs.
            // This is only possible if the values written at startTs were committed before any currently alive
            // read-write transactions.
            return startTs;
        }
    }
}
