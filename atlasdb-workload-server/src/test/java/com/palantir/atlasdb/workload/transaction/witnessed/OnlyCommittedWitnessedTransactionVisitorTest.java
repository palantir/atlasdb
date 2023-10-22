/*
 * (c) Copyright 2023 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.workload.transaction.witnessed;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.palantir.atlasdb.workload.store.ReadOnlyTransactionStore;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
public class OnlyCommittedWitnessedTransactionVisitorTest {

    private static final FullyWitnessedTransaction READ_ONLY_WITNESSED_TRANSACTION =
            FullyWitnessedTransaction.builder().startTimestamp(100L).build();

    private static final MaybeWitnessedTransaction MAYBE_WITNESSED_TRANSACTION = MaybeWitnessedTransaction.builder()
            .startTimestamp(100L)
            .commitTimestamp(101L)
            .build();

    @Mock
    private ReadOnlyTransactionStore transactionStore;

    @Test
    public void fullyCommittedTransactionsAreNeverChecked() {
        assertThat(READ_ONLY_WITNESSED_TRANSACTION.accept(
                        new OnlyCommittedWitnessedTransactionVisitor(transactionStore)))
                .contains(READ_ONLY_WITNESSED_TRANSACTION);
        verifyNoInteractions(transactionStore);
    }

    @Test
    public void maybeCommittedTransactionReturnsEmptyWhenNotCommitted() {
        when(transactionStore.isCommitted(anyLong())).thenReturn(false);
        assertThat(MAYBE_WITNESSED_TRANSACTION.accept(new OnlyCommittedWitnessedTransactionVisitor(transactionStore)))
                .isEmpty();
    }

    @Test
    public void maybeCommittedTransactionReturnsFullyWitnessedWhenCommitted() {
        when(transactionStore.isCommitted(anyLong())).thenReturn(true);
        assertThat(MAYBE_WITNESSED_TRANSACTION.accept(new OnlyCommittedWitnessedTransactionVisitor(transactionStore)))
                .contains(MAYBE_WITNESSED_TRANSACTION.toFullyWitnessed());
    }
}
