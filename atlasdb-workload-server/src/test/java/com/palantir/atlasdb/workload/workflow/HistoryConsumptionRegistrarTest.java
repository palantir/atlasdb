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

package com.palantir.atlasdb.workload.workflow;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.SettableFuture;
import com.palantir.atlasdb.workload.store.ImmutableWorkloadCell;
import com.palantir.atlasdb.workload.store.ReadableTransactionStore;
import com.palantir.atlasdb.workload.transaction.witnessed.ImmutableWitnessedTransaction;
import com.palantir.atlasdb.workload.transaction.witnessed.WitnessedDeleteTransactionAction;
import com.palantir.atlasdb.workload.transaction.witnessed.WitnessedTransaction;
import com.palantir.common.concurrent.PTExecutors;
import com.palantir.logsafe.exceptions.SafeRuntimeException;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.function.Consumer;
import org.junit.Test;

@SuppressWarnings("unchecked") // Mocks of known types
public class HistoryConsumptionRegistrarTest {
    private final ReadableTransactionStore readableTransactionStore = mock(ReadableTransactionStore.class);
    private final Consumer<WorkflowHistory> historyConsumerOne = mock(Consumer.class);
    private final Consumer<WorkflowHistory> historyConsumerTwo = mock(Consumer.class);
    private final HistoryConsumptionRegistrar registrar = new HistoryConsumptionRegistrar(readableTransactionStore);

    @Test
    public void passesArgumentsAndStoreToAllConsumers() throws ExecutionException, InterruptedException {
        SettableFuture<List<WitnessedTransaction>> witnessedTransactionsFuture = SettableFuture.create();
        ExecutorService separateThreadExecutor = PTExecutors.newSingleThreadExecutor();

        registrar.addConsumer(historyConsumerOne);
        registrar.addConsumer(historyConsumerTwo);
        Future<?> blockingTaskFuture =
                separateThreadExecutor.submit(() -> registrar.runOnConsumersBlocking(witnessedTransactionsFuture));

        assertThat(blockingTaskFuture).isNotDone();
        verify(historyConsumerOne, never()).accept(any());
        verify(historyConsumerTwo, never()).accept(any());

        List<WitnessedTransaction> witnessedTransactions = ImmutableList.of(ImmutableWitnessedTransaction.builder()
                .startTimestamp(5)
                .commitTimestamp(9)
                .addActions(WitnessedDeleteTransactionAction.of("foo", ImmutableWorkloadCell.of(1, 2)))
                .build());
        WorkflowHistory history = ImmutableWorkflowHistory.builder()
                .transactionStore(readableTransactionStore)
                .history(witnessedTransactions)
                .build();
        witnessedTransactionsFuture.set(witnessedTransactions);
        blockingTaskFuture.get();

        verify(historyConsumerOne).accept(history);
        verify(historyConsumerTwo).accept(history);
    }

    @Test
    public void propagatesExceptionsFromTransactionTasks() {
        RuntimeException taskException = new RuntimeException();

        registrar.addConsumer(historyConsumerOne);
        registrar.addConsumer(historyConsumerTwo);
        assertThatThrownBy(() -> registrar.runOnConsumersBlocking(Futures.immediateFailedFuture(taskException)))
                .isInstanceOf(SafeRuntimeException.class)
                .hasMessageContaining("Error when running workflow.")
                .hasCause(taskException);
    }
}
