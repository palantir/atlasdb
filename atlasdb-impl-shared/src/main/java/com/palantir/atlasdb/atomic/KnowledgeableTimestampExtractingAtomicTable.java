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
import com.palantir.atlasdb.transaction.knowledge.KnownAbandonedTransactions;
import com.palantir.atlasdb.transaction.knowledge.KnownConcludedTransactions;
import com.palantir.atlasdb.transaction.knowledge.TransactionKnowledgeComponents;
import com.palantir.atlasdb.transaction.knowledge.VerificationModeMetrics;
import com.palantir.atlasdb.transaction.service.TransactionStatus;
import com.palantir.common.streams.KeyedStream;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import com.palantir.tritium.metrics.registry.TaggedMetricRegistry;
import java.util.Comparator;
import java.util.Map;
import java.util.Optional;
import java.util.stream.StreamSupport;

public class KnowledgeableTimestampExtractingAtomicTable implements AtomicTable<Long, Long> {
    private static final SafeLogger log = SafeLoggerFactory.get(KnowledgeableTimestampExtractingAtomicTable.class);
    private static final boolean IS_VALIDATION_MODE = true;

    private final AtomicTable<Long, TransactionStatus> delegate;
    private final KnownConcludedTransactions knownConcludedTransactions;
    private final KnownAbandonedTransactions knownAbandonedTransactions;
    private final VerificationModeMetrics metrics;

    public KnowledgeableTimestampExtractingAtomicTable(
            AtomicTable<Long, TransactionStatus> delegate,
            TransactionKnowledgeComponents knowledge,
            TaggedMetricRegistry metricRegistry) {
        this.delegate = delegate;
        this.metrics = VerificationModeMetrics.of(metricRegistry);
        this.knownConcludedTransactions = knowledge.concluded();
        this.knownAbandonedTransactions = knowledge.abandoned();
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
     * For transaction with a known commit timestamp, returns the respective commit timestamp.
     * For transaction that is aborted, returns -1.
     * For transaction that is known to be committed but have unknown commitTs, returns startTs as commitTs for
     * read-write transaction. For read-only transactions, only returns if the greatestSeenCommitTS < startTs,
     * otherwise throws.
     * For transactions that are in-progress, returns a void future.
     * */
    @Override
    public ListenableFuture<Long> get(Long startTimestamp) {
        if (IS_VALIDATION_MODE) {
            return Futures.transform(
                    delegate.get(startTimestamp),
                    status -> verifyAndGetCommitTs(startTimestamp, status).orElse(null),
                    MoreExecutors.directExecutor());
        }
        return getInternal(startTimestamp);
    }

    /**
     * Returns commit timestamps against start timestamps supplied as arg.
     * Delegates to {@link KnowledgeableTimestampExtractingAtomicTable#get(Long)} serially.
     * Start timestamps for transactions that are in progress are not included in the map.
     * */
    @Override
    public ListenableFuture<Map<Long, Long>> get(Iterable<Long> keys) {
        if (IS_VALIDATION_MODE) {
            // No verification for in-progress transactions right now.
            return Futures.transform(
                    delegate.get(keys),
                    statuses -> KeyedStream.stream(statuses)
                            .map(this::verifyAndGetCommitTs)
                            .flatMap(Optional::stream)
                            .collectToMap(),
                    MoreExecutors.directExecutor());
        } else {
            Map<Long, ListenableFuture<Long>> futures = KeyedStream.of(
                            StreamSupport.stream(keys.spliterator(), false).sorted(Comparator.reverseOrder()))
                    .map(this::getInternal)
                    .collectToMap();
            return AtlasFutures.allAsMap(futures, MoreExecutors.directExecutor());
        }
    }

    private Optional<Long> verifyAndGetCommitTs(long startTimestamp, TransactionStatus transactionStatus) {
        Optional<Long> maybeGetCommitTs = Optional.ofNullable(TransactionStatusUtils.getCommitTsFromStatus(
                startTimestamp, transactionStatus, knownAbandonedTransactions::isKnownAbandoned));

        maybeGetCommitTs.ifPresent(commitTs -> {
            boolean isConcluded = knownConcludedTransactions.isKnownConcluded(
                    startTimestamp, KnownConcludedTransactions.Consistency.REMOTE_READ);

            if (!isConcluded) {
                return;
            }

            boolean abandonedTransaction = knownAbandonedTransactions.isKnownAbandoned(startTimestamp);
            boolean transactionStatusAborted = commitTs == TransactionConstants.FAILED_COMMIT_TS;

            if (abandonedTransaction && !transactionStatusAborted) {
                metrics.inconsistencies().inc();
                log.error(
                        "Found a transaction marked abandoned that was actually committed.",
                        SafeArg.of("startTimestamp", startTimestamp),
                        SafeArg.of("commitTs", commitTs),
                        SafeArg.of(
                                "lastKnownConcluded", knownConcludedTransactions.lastLocallyKnownConcludedTimestamp()));
            }

            if (!abandonedTransaction && transactionStatusAborted) {
                metrics.inconsistencies().inc();
                log.error(
                        "Found a concluded non-abandoned transaction that was actually aborted.",
                        SafeArg.of("startTimestamp", startTimestamp),
                        SafeArg.of(
                                "lastKnownConcluded", knownConcludedTransactions.lastLocallyKnownConcludedTimestamp()));
            }
        });
        return maybeGetCommitTs;
    }

    @VisibleForTesting
    ListenableFuture<Long> getInternal(long startTimestamp) {
        if (knownConcludedTransactions.isKnownConcluded(
                startTimestamp, KnownConcludedTransactions.Consistency.LOCAL_READ)) {
            return Futures.immediateFuture(TransactionStatusUtils.getCommitTsForConcludedTransaction(
                    startTimestamp, knownAbandonedTransactions::isKnownAbandoned));
        } else {
            ListenableFuture<TransactionStatus> presentValueFuture = delegate.get(startTimestamp);
            return Futures.transform(
                    presentValueFuture,
                    presentValue -> TransactionStatusUtils.getCommitTsFromStatus(
                            startTimestamp, presentValue, knownAbandonedTransactions::isKnownAbandoned),
                    MoreExecutors.directExecutor());
        }
    }
}
