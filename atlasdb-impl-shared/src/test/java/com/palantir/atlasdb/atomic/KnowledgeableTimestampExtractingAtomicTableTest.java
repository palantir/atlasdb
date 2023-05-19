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
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.Futures;
import com.palantir.atlasdb.transaction.impl.TransactionConstants;
import com.palantir.atlasdb.transaction.knowledge.ImmutableTransactionKnowledgeComponents;
import com.palantir.atlasdb.transaction.knowledge.KnownAbandonedTransactions;
import com.palantir.atlasdb.transaction.knowledge.KnownConcludedTransactions;
import com.palantir.atlasdb.transaction.knowledge.KnownConcludedTransactions.Consistency;
import com.palantir.atlasdb.transaction.knowledge.VerificationModeMetrics;
import com.palantir.atlasdb.transaction.service.TransactionStatus;
import com.palantir.tritium.metrics.registry.DefaultTaggedMetricRegistry;
import java.util.concurrent.ExecutionException;
import org.junit.Test;

public class KnowledgeableTimestampExtractingAtomicTableTest {
    private final AtomicTable<Long, TransactionStatus> delegate = mock(AtomicTable.class);
    private final KnownConcludedTransactions knownConcludedTransactions = mock(KnownConcludedTransactions.class);
    private final KnownAbandonedTransactions knownAbandonedTransactions = mock(KnownAbandonedTransactions.class);
    private final DefaultTaggedMetricRegistry registry = new DefaultTaggedMetricRegistry();
    private final VerificationModeMetrics metrics = VerificationModeMetrics.of(registry);
    private final KnowledgeableTimestampExtractingAtomicTable tsExtractingTable =
            new KnowledgeableTimestampExtractingAtomicTable(
                    delegate,
                    ImmutableTransactionKnowledgeComponents.builder()
                            .concluded(knownConcludedTransactions)
                            .abandoned(knownAbandonedTransactions)
                            .lastSeenCommitSupplier(() -> Long.MAX_VALUE)
                            .build(),
                    registry);

    @Test
    public void canGetTsOfConcludedTxn() throws ExecutionException, InterruptedException {
        long startTs = 27l;
        when(knownConcludedTransactions.isKnownConcluded(startTs, KnownConcludedTransactions.Consistency.LOCAL_READ))
                .thenReturn(true);
        when(knownAbandonedTransactions.isKnownAbandoned(startTs)).thenReturn(false);

        assertThat(tsExtractingTable.getInternal(startTs).get()).isEqualTo(startTs);
    }

    @Test
    public void canGetTsOfConcludedAbortedTxn() throws ExecutionException, InterruptedException {
        long startTs = 27l;
        when(knownConcludedTransactions.isKnownConcluded(startTs, KnownConcludedTransactions.Consistency.LOCAL_READ))
                .thenReturn(true);
        when(knownAbandonedTransactions.isKnownAbandoned(startTs)).thenReturn(true);

        assertThat(tsExtractingTable.getInternal(startTs).get()).isEqualTo(TransactionConstants.FAILED_COMMIT_TS);
    }

    @Test
    public void canGetTsOfNonConcludedTxn() throws ExecutionException, InterruptedException {
        long startTs = 27l;
        long commitTs = 127L;
        when(knownConcludedTransactions.isKnownConcluded(startTs, KnownConcludedTransactions.Consistency.LOCAL_READ))
                .thenReturn(false);
        when(delegate.get(startTs)).thenReturn(Futures.immediateFuture(TransactionStatus.committed(commitTs)));

        assertThat(tsExtractingTable.getInternal(startTs).get()).isEqualTo(commitTs);
    }

    @Test
    public void canGetTsOfNonConcludedAbortedTxn() throws ExecutionException, InterruptedException {
        long startTs = 27l;
        when(knownConcludedTransactions.isKnownConcluded(startTs, KnownConcludedTransactions.Consistency.LOCAL_READ))
                .thenReturn(false);
        when(delegate.get(startTs)).thenReturn(Futures.immediateFuture(TransactionStatus.aborted()));

        assertThat(tsExtractingTable.getInternal(startTs).get()).isEqualTo(TransactionConstants.FAILED_COMMIT_TS);
    }

    @Test
    public void canGetTsOfNonConcludedInProgressTxn() throws ExecutionException, InterruptedException {
        long startTs = 27l;
        when(knownConcludedTransactions.isKnownConcluded(startTs, KnownConcludedTransactions.Consistency.LOCAL_READ))
                .thenReturn(false);
        when(delegate.get(startTs)).thenReturn(Futures.immediateFuture(TransactionStatus.inProgress()));

        assertThat(tsExtractingTable.getInternal(startTs).get()).isNull();
    }

    @Test
    public void canGetTsOfRemotelyConcludedTxn() throws ExecutionException, InterruptedException {
        long startTs = 27l;
        when(knownConcludedTransactions.isKnownConcluded(startTs, KnownConcludedTransactions.Consistency.LOCAL_READ))
                .thenReturn(false);
        when(delegate.get(startTs)).thenReturn(Futures.immediateFuture(TransactionStatus.unknown()));
        when(knownAbandonedTransactions.isKnownAbandoned(startTs)).thenReturn(false);

        assertThat(tsExtractingTable.getInternal(startTs).get()).isEqualTo(startTs);
    }

    @Test
    public void canGetTsOfRemotelyConcludedAbortedTxn() throws ExecutionException, InterruptedException {
        long startTs = 27l;
        when(knownConcludedTransactions.isKnownConcluded(startTs, KnownConcludedTransactions.Consistency.LOCAL_READ))
                .thenReturn(false);
        when(delegate.get(startTs)).thenReturn(Futures.immediateFuture(TransactionStatus.unknown()));
        when(knownAbandonedTransactions.isKnownAbandoned(startTs)).thenReturn(true);

        assertThat(tsExtractingTable.getInternal(startTs).get()).isEqualTo(TransactionConstants.FAILED_COMMIT_TS);
    }

    // Verification mode tests
    @Test
    public void noVerificationIfTransactionInProgress() {
        long startTs = 27l;
        when(delegate.get(startTs)).thenReturn(Futures.immediateFuture(TransactionStatus.inProgress()));
        tsExtractingTable.get(startTs);

        verifyNoInteractions(knownAbandonedTransactions);
        verifyNoInteractions(knownConcludedTransactions);
        assertNoVerification();
    }

    @Test
    public void noVerificationInBatchedGetIfTransactionInProgress() {
        long startTs = 27l;
        when(delegate.get(ImmutableSet.of(startTs)))
                .thenReturn(Futures.immediateFuture(ImmutableMap.of(startTs, TransactionStatus.inProgress())));
        tsExtractingTable.get(ImmutableSet.of(startTs));

        verifyNoInteractions(knownAbandonedTransactions);
        verifyNoInteractions(knownConcludedTransactions);
        assertNoVerification();
    }

    @Test
    public void noVerificationIfTransactionNotConcluded() {
        long startTs = 27l;
        long commitTs = 35l;
        when(delegate.get(startTs)).thenReturn(Futures.immediateFuture(TransactionStatus.committed(commitTs)));
        when(knownConcludedTransactions.isKnownConcluded(startTs, Consistency.REMOTE_READ))
                .thenReturn(false);

        tsExtractingTable.get(startTs);

        verify(knownConcludedTransactions).isKnownConcluded(startTs, Consistency.REMOTE_READ);
        verifyNoInteractions(knownAbandonedTransactions);
        assertNoVerification();
    }

    @Test
    public void verifiesConcludedTransaction() {
        long startTs = 27l;
        long commitTs = 35l;
        when(delegate.get(startTs)).thenReturn(Futures.immediateFuture(TransactionStatus.committed(commitTs)));
        when(knownConcludedTransactions.isKnownConcluded(startTs, Consistency.REMOTE_READ))
                .thenReturn(true);
        when(knownAbandonedTransactions.isKnownAbandoned(startTs)).thenReturn(false);

        tsExtractingTable.get(startTs);

        verify(knownConcludedTransactions).isKnownConcluded(startTs, Consistency.REMOTE_READ);
        verify(knownAbandonedTransactions).isKnownAbandoned(startTs);
        assertThat(metrics.success().getCount()).isEqualTo(1);
        assertThat(metrics.inconsistencies().getCount()).isEqualTo(0);
    }

    @Test
    public void verifiesConcludedAbandonedTransaction() {
        long startTs = 27l;

        when(delegate.get(startTs)).thenReturn(Futures.immediateFuture(TransactionStatus.aborted()));
        when(knownConcludedTransactions.isKnownConcluded(startTs, Consistency.REMOTE_READ))
                .thenReturn(true);
        when(knownAbandonedTransactions.isKnownAbandoned(startTs)).thenReturn(true);

        tsExtractingTable.get(startTs);

        verify(knownConcludedTransactions).isKnownConcluded(startTs, Consistency.REMOTE_READ);
        verify(knownAbandonedTransactions).isKnownAbandoned(startTs);
        assertThat(metrics.success().getCount()).isEqualTo(1);
        assertThat(metrics.inconsistencies().getCount()).isEqualTo(0);
    }

    @Test
    public void catchesInconsistencyIfCommittedTransactionIsMarkedAbandoned() {
        long startTs = 27l;
        long commitTs = 35l;

        when(delegate.get(startTs)).thenReturn(Futures.immediateFuture(TransactionStatus.committed(commitTs)));
        when(knownConcludedTransactions.isKnownConcluded(startTs, Consistency.REMOTE_READ))
                .thenReturn(true);
        when(knownAbandonedTransactions.isKnownAbandoned(startTs)).thenReturn(true);

        tsExtractingTable.get(startTs);

        verify(knownConcludedTransactions).isKnownConcluded(startTs, Consistency.REMOTE_READ);
        verify(knownAbandonedTransactions).isKnownAbandoned(startTs);
        assertThat(metrics.success().getCount()).isEqualTo(0);
        assertThat(metrics.inconsistencies().getCount()).isEqualTo(1);
    }

    @Test
    public void catchesInconsistencyIfAbortedTransactionIsNotMarkedAbandoned() {
        long startTs = 27l;

        when(delegate.get(startTs)).thenReturn(Futures.immediateFuture(TransactionStatus.aborted()));
        when(knownConcludedTransactions.isKnownConcluded(startTs, Consistency.REMOTE_READ))
                .thenReturn(true);
        when(knownAbandonedTransactions.isKnownAbandoned(startTs)).thenReturn(false);

        tsExtractingTable.get(startTs);

        verify(knownConcludedTransactions).isKnownConcluded(startTs, Consistency.REMOTE_READ);
        verify(knownAbandonedTransactions).isKnownAbandoned(startTs);
        assertThat(metrics.success().getCount()).isEqualTo(0);
        assertThat(metrics.inconsistencies().getCount()).isEqualTo(1);
    }

    private void assertNoVerification() {
        assertThat(metrics.success().getCount()).isEqualTo(0);
        assertThat(metrics.inconsistencies().getCount()).isEqualTo(0);
    }
}
