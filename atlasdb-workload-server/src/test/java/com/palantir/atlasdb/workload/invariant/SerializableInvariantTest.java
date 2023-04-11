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

package com.palantir.atlasdb.workload.invariant;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.Iterables;
import com.palantir.atlasdb.workload.store.ReadableTransactionStore;
import com.palantir.atlasdb.workload.transaction.WitnessedTransactionsBuilder;
import com.palantir.atlasdb.workload.transaction.witnessed.InvalidWitnessedTransaction;
import com.palantir.atlasdb.workload.transaction.witnessed.InvalidWitnessedTransactionAction;
import com.palantir.atlasdb.workload.transaction.witnessed.WitnessedTransaction;
import com.palantir.atlasdb.workload.workflow.ImmutableWorkflowHistory;
import com.palantir.atlasdb.workload.workflow.WorkflowHistory;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class SerializableInvariantTest {

    @Mock
    private ReadableTransactionStore readableTransactionStore;

    @Test
    public void handlesLocalWrites() {
        AtomicReference<List<InvalidWitnessedTransaction>> invalidTransactions = new AtomicReference<>();
        List<WitnessedTransaction> transactions = new WitnessedTransactionsBuilder("table")
                .startTransaction()
                .read(5, 10)
                .write(5, 10, 15)
                .read(5, 10, 15)
                .endTransaction()
                .build();
        WorkflowHistory workflowHistory = ImmutableWorkflowHistory.builder()
                .history(transactions)
                .transactionStore(readableTransactionStore)
                .build();
        SerializableInvariant.INSTANCE.accept(workflowHistory, invalidTransactions::set);
        assertThat(invalidTransactions.get()).isEmpty();
    }

    @Test
    public void noInvalidTransactionsWhenSerializable() {
        AtomicReference<List<InvalidWitnessedTransaction>> invalidTransactions = new AtomicReference<>();
        List<WitnessedTransaction> transactions = new WitnessedTransactionsBuilder("table")
                .startTransaction()
                .write(5, 10, 15)
                .endTransaction()
                .startTransaction()
                .read(5, 10, 15)
                .write(23, 1, 20)
                .write(7, 15, 14)
                .endTransaction()
                .startTransaction()
                .read(5, 10, 15)
                .read(23, 1, 20)
                .read(7, 15, 14)
                .endTransaction()
                .build();
        WorkflowHistory workflowHistory = ImmutableWorkflowHistory.builder()
                .history(transactions)
                .transactionStore(readableTransactionStore)
                .build();
        SerializableInvariant.INSTANCE.accept(workflowHistory, invalidTransactions::set);
        assertThat(invalidTransactions.get()).isEmpty();
    }

    @Test
    public void catchesWriteSkew() {
        AtomicReference<List<InvalidWitnessedTransaction>> invalidTransactions = new AtomicReference<>();
        List<WitnessedTransaction> transactions = new WitnessedTransactionsBuilder("table")
                .startTransaction()
                .write(5, 10, 15)
                .write(6, 10, 15)
                .endTransaction()
                .startTransaction()
                .read(5, 10, 15)
                .read(6, 10, 15)
                .write(5, 10, 0)
                .endTransaction()
                .startTransaction()
                .read(5, 10, 15)
                .read(6, 10, 15)
                .write(6, 10, 0)
                .endTransaction()
                .build();
        WorkflowHistory workflowHistory = ImmutableWorkflowHistory.builder()
                .history(transactions)
                .transactionStore(readableTransactionStore)
                .build();
        SerializableInvariant.INSTANCE.accept(workflowHistory, invalidTransactions::set);
        assertThat(invalidTransactions).hasValueSatisfying(invalidWitnessedTransactions -> {
            InvalidWitnessedTransaction invalidWitnessedTransaction =
                    Iterables.getOnlyElement(invalidWitnessedTransactions);
            assertThat(invalidWitnessedTransaction.transaction()).isEqualTo(Iterables.getLast(transactions));

            InvalidWitnessedTransactionAction invalidWitnessedTransactionAction =
                    Iterables.getOnlyElement(invalidWitnessedTransaction.invalidActions());
            assertThat(invalidWitnessedTransactionAction.mismatchedValue())
                    .isEqualTo(MismatchedValue.of(Optional.of(15), Optional.of(0)));
        });
    }

    @Test
    public void handlesDeletes() {
        AtomicReference<List<InvalidWitnessedTransaction>> invalidTransactions = new AtomicReference<>();
        List<WitnessedTransaction> transactions = new WitnessedTransactionsBuilder("table")
                .startTransaction()
                .write(5, 10, 15)
                .write(6, 10, 15)
                .endTransaction()
                .startTransaction()
                .read(5, 10, 15)
                .delete(5, 10)
                .read(5, 10)
                .endTransaction()
                .startTransaction()
                .read(5, 10)
                .endTransaction()
                .build();
        WorkflowHistory workflowHistory = ImmutableWorkflowHistory.builder()
                .history(transactions)
                .transactionStore(readableTransactionStore)
                .build();
        SerializableInvariant.INSTANCE.accept(workflowHistory, invalidTransactions::set);
        assertThat(invalidTransactions)
                .hasValueSatisfying(invalidWitnessedTransactions ->
                        assertThat(invalidWitnessedTransactions).isEmpty());
    }
}
