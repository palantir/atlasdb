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

import com.google.common.annotations.VisibleForTesting;
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
import java.util.stream.StreamSupport;

public class KnowledgeableTimestampExtractingAtomicTable implements AtomicTable<Long, Long> {
    private final AtomicTable<Long, TransactionStatus> delegate;
    private final KnownConcludedTransactions knownConcludedTransactions;
    private final KnownAbortedTransactions knownAbortedTransactions;

    public KnowledgeableTimestampExtractingAtomicTable(
            AtomicTable<Long, TransactionStatus> delegate,
            KnownConcludedTransactions knownConcludedTransactions,
            KnownAbortedTransactions knownAbortedTransactions) {
        this.delegate = delegate;
        this.knownConcludedTransactions = knownConcludedTransactions;
        this.knownAbortedTransactions = knownAbortedTransactions;
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
     * For transactions with a known commit timestamp, returns the respective commit timestamps.
     * For transactions that are aborted, returns -1.
     * For transactions that are known ot be committed but have unknown commitTs, returns startTs as commitTs for
     * read-write transactions.
     * For read-only transactions, only returns if the greatestSeenCommitTS < startTs, otherwise throws.
     * Start timestamps for transactions that are in progress return a void future.
     * */
    @Override
    public ListenableFuture<Long> get(Long startTimestamp) {
        return getInternal(startTimestamp);
    }

    /**
     * Returns commit timestamps against start timestamps supplied as arg.
     * Delegates to {@link KnowledgeableTimestampExtractingAtomicTable#get(Long)} serially.
     * Start timestamps for transactions that are in progress are not included in the map.
     * */
    @Override
    public ListenableFuture<Map<Long, Long>> get(Iterable<Long> keys) {
        Map<Long, ListenableFuture<Long>> futures = KeyedStream.of(
                        StreamSupport.stream(keys.spliterator(), false).sorted(Comparator.reverseOrder()))
                .map(this::getInternal)
                .collectToMap();
        return AtlasFutures.allAsMap(futures, MoreExecutors.directExecutor());
    }

    @VisibleForTesting
    ListenableFuture<Long> getInternal(long startTimestamp) {
        if (knownConcludedTransactions.isKnownConcluded(
                startTimestamp, KnownConcludedTransactions.Consistency.LOCAL_READ)) {
            return Futures.immediateFuture(getCommitTsForConcludedTransaction(startTimestamp, knownAbortedTransactions));
        } else {
            ListenableFuture<TransactionStatus> presentValueFuture = delegate.get(startTimestamp);
            return Futures.transform(
                    presentValueFuture,
                    presentValue -> getCommitTsFromStatus(startTimestamp, presentValue, knownAbortedTransactions),
                    MoreExecutors.directExecutor());
        }
    }

    public static long getCommitTsFromStatus(long startTs,
            TransactionStatus status,
            KnownAbortedTransactions knownAbortedTransactions) {
        return TransactionStatuses.caseOf(status)
                .unknown(() -> getCommitTsForConcludedTransaction(startTs, knownAbortedTransactions))
                .otherwise(() -> TransactionStatusUtils.maybeGetCommitTs(status).orElse(null));
    }

    private static long getCommitTsForConcludedTransaction(long startTs,
            KnownAbortedTransactions knownAbortedTransactions) {
        return knownAbortedTransactions.isKnownAborted(startTs)
                ? TransactionConstants.FAILED_COMMIT_TS
                : getCommitTsForNonAbortedUnknownTransaction(startTs);
    }

    public static long getCommitTsForNonAbortedUnknownTransaction(long startTs) {
        return startTs;
    }
}
