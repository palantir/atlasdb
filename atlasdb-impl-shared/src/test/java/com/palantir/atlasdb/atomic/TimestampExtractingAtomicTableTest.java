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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.Futures;
import com.palantir.atlasdb.transaction.impl.TransactionConstants;
import com.palantir.atlasdb.transaction.knowledge.KnownAbortedTransactions;
import com.palantir.atlasdb.transaction.knowledge.KnownConcludedTransactions;
import com.palantir.atlasdb.transaction.service.TransactionStatus;
import com.palantir.atlasdb.transaction.service.TransactionStatuses;
import com.palantir.common.streams.KeyedStream;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;
import org.junit.Test;

public class TimestampExtractingAtomicTableTest {
    private final AtomicTable<Long, TransactionStatus> delegate = mock(AtomicTable.class);
    private final KnownAbortedTransactions knownAbortedTransactions = mock(KnownAbortedTransactions.class);
    private final KnownConcludedTransactions knownConcludedTransactions = mock(KnownConcludedTransactions.class);
    private final Supplier<Long> lastSeenCommitTs = mock(Supplier.class);

    private final TimestampExtractingAtomicTable readOnlyAtomicTable = new TimestampExtractingAtomicTable(
            delegate, knownAbortedTransactions, knownConcludedTransactions, lastSeenCommitTs, true);
    private final TimestampExtractingAtomicTable defaultAtomicTable = new TimestampExtractingAtomicTable(
            delegate, knownAbortedTransactions, knownConcludedTransactions, lastSeenCommitTs, false);

    @Test
    public void canExtractCommittedTransaction() throws ExecutionException, InterruptedException {
        Iterable<Long> keys = ImmutableList.of(1L, 2L, 3L);
        Map<Long, TransactionStatus> commits = KeyedStream.of(keys)
                .map(key -> TransactionStatuses.committed(commitTs(key)))
                .collectToMap();
        when(delegate.get(keys)).thenReturn(Futures.immediateFuture(commits));

        Map<Long, Long> expected = KeyedStream.of(keys).map(this::commitTs).collectToMap();
        assertThat(defaultAtomicTable.get(keys).get()).isEqualTo(expected);
    }

    @Test
    public void canExtractAbortedTransaction() throws ExecutionException, InterruptedException {
        Iterable<Long> keys = ImmutableList.of(1L);
        Map<Long, TransactionStatus> commits =
                KeyedStream.of(keys).map(_key -> TransactionConstants.ABORTED).collectToMap();
        when(delegate.get(keys)).thenReturn(Futures.immediateFuture(commits));

        Map<Long, Long> expected = KeyedStream.of(keys)
                .map(_unused -> TransactionConstants.FAILED_COMMIT_TS)
                .collectToMap();
        assertThat(defaultAtomicTable.get(keys).get()).isEqualTo(expected);
    }

    @Test
    public void canExtractCommittedOrAbortedTransaction() throws ExecutionException, InterruptedException {
        long committedTs = 1L;
        long abortedTs = 2L;
        Iterable<Long> keys = ImmutableList.of(committedTs, abortedTs);
        Map<Long, TransactionStatus> commits = ImmutableMap.of(
                committedTs,
                TransactionStatuses.committed(commitTs(committedTs)),
                abortedTs,
                TransactionConstants.ABORTED);
        when(delegate.get(keys)).thenReturn(Futures.immediateFuture(commits));

        Map<Long, Long> expected =
                ImmutableMap.of(committedTs, commitTs(committedTs), abortedTs, TransactionConstants.FAILED_COMMIT_TS);
        assertThat(defaultAtomicTable.get(keys).get()).isEqualTo(expected);
    }

    @Test
    public void ignoresInProgressTransaction() throws ExecutionException, InterruptedException {
        Iterable<Long> keys = ImmutableList.of(1L);
        Map<Long, TransactionStatus> commits = KeyedStream.of(keys)
                .map(_key -> TransactionConstants.IN_PROGRESS)
                .collectToMap();
        when(delegate.get(keys)).thenReturn(Futures.immediateFuture(commits));

        assertThat(defaultAtomicTable.get(keys).get()).isEmpty();
    }

    @Test
    public void ignoresUnknownTransaction() throws ExecutionException, InterruptedException {
        Iterable<Long> keys = ImmutableList.of(1L);
        Map<Long, TransactionStatus> commits =
                KeyedStream.of(keys).map(_key -> TransactionConstants.UNKNOWN).collectToMap();
        when(delegate.get(keys)).thenReturn(Futures.immediateFuture(commits));

        assertThat(defaultAtomicTable.get(keys).get()).isEmpty();
    }
    //
    //    @Test
    //    public void canGetAbortedTransactionStateFromLocal() throws ExecutionException, InterruptedException {
    //        long startTs = 24L;
    //        when(knownConcludedTransactions.isKnownConcluded(startTs,
    // KnownConcludedTransactions.Consistency.LOCAL_READ))
    //                .thenReturn(true);
    //        when(knownAbortedTransactions.isKnownAborted(startTs)).thenReturn(true);
    //
    //        // read-write transaction
    //        assertThat(transactionService.getAsync(startTs).get()).isNull();
    //
    //        // read-only transaction
    //        assertThat(readOnlyTransactionService.getAsync(startTs).get()).isNull();
    //    }
    //
    //    @Test
    //    public void canGetStatusFromLocalForReadWriteTxn() throws ExecutionException, InterruptedException {
    //        long startTs = 24L;
    //        when(knownConcludedTransactions.isKnownConcluded(startTs,
    // KnownConcludedTransactions.Consistency.LOCAL_READ))
    //                .thenReturn(true);
    //        when(knownAbortedTransactions.isKnownAborted(startTs)).thenReturn(false);
    //
    //        assertThat(transactionService.getAsync(startTs).get()).isEqualTo(startTs);
    //    }
    //
    //    @Test
    //    public void canGetStatusFromTxnTableForReadWriteTxn() throws ExecutionException, InterruptedException {
    //        long startTs = 24L;
    //        when(knownConcludedTransactions.isKnownConcluded(startTs,
    // KnownConcludedTransactions.Consistency.LOCAL_READ))
    //                .thenReturn(false);
    //        //        when(atomicTable.get(startTs)).thenReturn(Immutable);
    //
    //        assertThat(transactionService.getAsync(startTs).get()).isEqualTo(startTs);
    //    }

    private long commitTs(long startTs) {
        return 4 * startTs;
    }
}
