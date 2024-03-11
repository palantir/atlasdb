/*
 * (c) Copyright 2024 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.transaction.impl;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.palantir.atlasdb.futures.AtlasFutures;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.transaction.api.CommitTimestampLoader;
import com.palantir.atlasdb.transaction.api.TransactionSerializableConflictException;
import com.palantir.atlasdb.transaction.impl.SerializableTransaction.PartitionedTimestamps;
import com.palantir.atlasdb.transaction.impl.metrics.TransactionOutcomeMetrics;
import org.eclipse.collections.api.LongIterable;
import org.eclipse.collections.api.factory.primitive.LongLongMaps;
import org.eclipse.collections.api.factory.primitive.LongSets;
import org.eclipse.collections.api.map.primitive.LongLongMap;
import org.eclipse.collections.api.map.primitive.MutableLongLongMap;
import org.eclipse.collections.api.set.primitive.LongSet;
import org.eclipse.collections.api.set.primitive.MutableLongSet;
import org.jetbrains.annotations.Nullable;

/**
 * Loads commit timestamps for read validation, considering a simulated state of the world where the serializable
 * transaction that we're performing validation for has already committed.
 */
public final class ReadValidationCommitTimestampLoader implements CommitTimestampLoader {
    private final CommitTimestampLoader delegate;
    private final long startTs;
    private final long commitTs;
    private final TransactionOutcomeMetrics transactionOutcomeMetrics;

    public ReadValidationCommitTimestampLoader(
            CommitTimestampLoader delegate,
            long startTs,
            long commitTs,
            TransactionOutcomeMetrics transactionOutcomeMetrics) {
        this.delegate = delegate;
        this.startTs = startTs;
        this.commitTs = commitTs;
        this.transactionOutcomeMetrics = transactionOutcomeMetrics;
    }

    @Override
    public ListenableFuture<LongLongMap> getCommitTimestamps(
            @Nullable TableReference tableRef, LongIterable startTimestamps, boolean shouldWaitForCommitterToComplete) {
        PartitionedTimestamps partitionedTimestamps = splitTransactionBeforeAndAfter(startTs, startTimestamps);

        ListenableFuture<LongLongMap> postStartCommitTimestamps =
                getCommitTimestampsForTransactionsStartedAfterMe(tableRef, partitionedTimestamps.afterStart());

        // We are ok to block here because if there is a cycle of transactions that could result in a deadlock,
        // then at least one of them will be in the ab
        ListenableFuture<LongLongMap> preStartCommitTimestamps;
        if (partitionedTimestamps.beforeStart().isEmpty()) {
            preStartCommitTimestamps = Futures.immediateFuture(LongLongMaps.immutable.empty());
        } else {
            preStartCommitTimestamps = delegate.getCommitTimestamps(
                    tableRef, partitionedTimestamps.beforeStart(), shouldWaitForCommitterToComplete);
        }

        return Futures.whenAllComplete(postStartCommitTimestamps, preStartCommitTimestamps)
                .call(
                        () -> {
                            MutableLongLongMap map =
                                    LongLongMaps.mutable.withAll(AtlasFutures.getDone(preStartCommitTimestamps));
                            map.putAll(AtlasFutures.getDone(postStartCommitTimestamps));
                            map.putAll(partitionedTimestamps.myCommittedTransaction());
                            return map.toImmutable();
                        },
                        MoreExecutors.directExecutor());
    }

    private ListenableFuture<LongLongMap> getCommitTimestampsForTransactionsStartedAfterMe(
            @Nullable TableReference tableRef, LongSet startTimestamps) {
        if (startTimestamps.isEmpty()) {
            return Futures.immediateFuture(LongLongMaps.immutable.empty());
        }

        return Futures.transform(
                // We do not block when waiting for results that were written after our start timestamp.
                // If we block here it may lead to deadlock if two transactions (or a cycle of any length) have
                // all written their data and all doing checks before committing.
                delegate.getCommitTimestamps(tableRef, startTimestamps, false),
                startToCommitTimestamps -> {
                    if (startToCommitTimestamps.keySet().containsAll(startTimestamps)) {
                        return startToCommitTimestamps;
                    }
                    // If we do not get back all these results we may be in the deadlock case so we
                    // should just fail out early.  It may be the case that abort more transactions
                    // than needed to break the deadlock cycle, but this should be pretty rare.
                    transactionOutcomeMetrics.markReadWriteConflict(tableRef);
                    throw new TransactionSerializableConflictException(
                            "An uncommitted conflicting read was written after our start timestamp for table "
                                    + tableRef + ".  This case can cause deadlock and is very likely to be a "
                                    + "read write conflict.",
                            tableRef);
                },
                MoreExecutors.directExecutor());
    }

    /**
     * Partitions {@code startTimestamps} in two sets, based on their relation to the start timestamp provided.
     *
     * @param myStart start timestamp of this transaction
     * @param startTimestamps of transactions we are interested in
     * @return a {@link PartitionedTimestamps} object containing split timestamps
     */
    private PartitionedTimestamps splitTransactionBeforeAndAfter(long myStart, LongIterable startTimestamps) {
        ImmutablePartitionedTimestamps.Builder builder =
                ImmutablePartitionedTimestamps.builder().myCommitTimestamp(commitTs);
        MutableLongSet beforeStart = LongSets.mutable.empty();
        MutableLongSet afterStart = LongSets.mutable.empty();
        startTimestamps.forEach(startTimestamp -> {
            if (startTimestamp == myStart) {
                builder.splittingStartTimestamp(myStart);
            } else if (startTimestamp < myStart) {
                beforeStart.add(startTimestamp);
            } else {
                afterStart.add(startTimestamp);
            }
        });
        builder.beforeStart(beforeStart.asUnmodifiable());
        builder.afterStart(afterStart.asUnmodifiable());
        return builder.build();
    }
}
