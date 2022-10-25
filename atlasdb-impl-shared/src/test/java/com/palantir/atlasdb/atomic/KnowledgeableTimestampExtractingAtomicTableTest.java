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

import com.google.common.util.concurrent.Futures;
import com.palantir.atlasdb.transaction.impl.TransactionConstants;
import com.palantir.atlasdb.transaction.knowledge.ImmutableTransactionKnowledgeComponents;
import com.palantir.atlasdb.transaction.knowledge.KnownAbandonedTransactions;
import com.palantir.atlasdb.transaction.knowledge.KnownConcludedTransactions;
import com.palantir.atlasdb.transaction.service.TransactionStatus;
import com.palantir.atlasdb.transaction.service.TransactionStatuses;
import com.palantir.tritium.metrics.registry.DefaultTaggedMetricRegistry;
import java.util.concurrent.ExecutionException;
import org.junit.Test;

public class KnowledgeableTimestampExtractingAtomicTableTest {
    private final AtomicTable<Long, TransactionStatus> delegate = mock(AtomicTable.class);
    private final KnownConcludedTransactions knownConcludedTransactions = mock(KnownConcludedTransactions.class);
    private final KnownAbandonedTransactions knownAbandonedTransactions = mock(KnownAbandonedTransactions.class);
    private final KnowledgeableTimestampExtractingAtomicTable tsExtractingTable =
            new KnowledgeableTimestampExtractingAtomicTable(
                    delegate,
                    ImmutableTransactionKnowledgeComponents.builder()
                            .concluded(knownConcludedTransactions)
                            .abandoned(knownAbandonedTransactions)
                            .lastSeenCommitSupplier(() -> Long.MAX_VALUE)
                            .build(),
                    new DefaultTaggedMetricRegistry());

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
        when(delegate.get(startTs)).thenReturn(Futures.immediateFuture(TransactionStatuses.committed(commitTs)));

        assertThat(tsExtractingTable.getInternal(startTs).get()).isEqualTo(commitTs);
    }

    @Test
    public void canGetTsOfNonConcludedAbortedTxn() throws ExecutionException, InterruptedException {
        long startTs = 27l;
        when(knownConcludedTransactions.isKnownConcluded(startTs, KnownConcludedTransactions.Consistency.LOCAL_READ))
                .thenReturn(false);
        when(delegate.get(startTs)).thenReturn(Futures.immediateFuture(TransactionStatuses.aborted()));

        assertThat(tsExtractingTable.getInternal(startTs).get()).isEqualTo(TransactionConstants.FAILED_COMMIT_TS);
    }

    @Test
    public void canGetTsOfNonConcludedInProgressTxn() throws ExecutionException, InterruptedException {
        long startTs = 27l;
        when(knownConcludedTransactions.isKnownConcluded(startTs, KnownConcludedTransactions.Consistency.LOCAL_READ))
                .thenReturn(false);
        when(delegate.get(startTs)).thenReturn(Futures.immediateFuture(TransactionStatuses.inProgress()));

        assertThat(tsExtractingTable.getInternal(startTs).get()).isNull();
    }

    @Test
    public void canGetTsOfRemotelyConcludedTxn() throws ExecutionException, InterruptedException {
        long startTs = 27l;
        when(knownConcludedTransactions.isKnownConcluded(startTs, KnownConcludedTransactions.Consistency.LOCAL_READ))
                .thenReturn(false);
        when(delegate.get(startTs)).thenReturn(Futures.immediateFuture(TransactionStatuses.unknown()));
        when(knownAbandonedTransactions.isKnownAbandoned(startTs)).thenReturn(false);

        assertThat(tsExtractingTable.getInternal(startTs).get()).isEqualTo(startTs);
    }

    @Test
    public void canGetTsOfRemotelyConcludedAbortedTxn() throws ExecutionException, InterruptedException {
        long startTs = 27l;
        when(knownConcludedTransactions.isKnownConcluded(startTs, KnownConcludedTransactions.Consistency.LOCAL_READ))
                .thenReturn(false);
        when(delegate.get(startTs)).thenReturn(Futures.immediateFuture(TransactionStatuses.unknown()));
        when(knownAbandonedTransactions.isKnownAbandoned(startTs)).thenReturn(true);

        assertThat(tsExtractingTable.getInternal(startTs).get()).isEqualTo(TransactionConstants.FAILED_COMMIT_TS);
    }
}
