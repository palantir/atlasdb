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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.palantir.atlasdb.workload.store.ReadOnlyTransactionStore;
import com.palantir.atlasdb.workload.store.TransactionStore;
import com.palantir.atlasdb.workload.transaction.witnessed.OnlyCommittedWitnessedTransactionVisitor;
import com.palantir.atlasdb.workload.transaction.witnessed.WitnessedTransaction;
import com.palantir.logsafe.exceptions.SafeRuntimeException;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import one.util.streamex.StreamEx;

public final class DefaultWorkflow<T extends TransactionStore> implements Workflow {
    private final ConcurrentTransactionRunner<T> concurrentTransactionRunner;
    private final KeyedTransactionTask<T> transactionTask;
    private final WorkflowConfiguration workflowConfiguration;
    private final ReadOnlyTransactionStore readOnlyTransactionStore;

    private DefaultWorkflow(
            ConcurrentTransactionRunner<T> concurrentTransactionRunner,
            KeyedTransactionTask<T> transactionTask,
            WorkflowConfiguration workflowConfiguration,
            ReadOnlyTransactionStore readOnlyTransactionStore) {
        this.concurrentTransactionRunner = concurrentTransactionRunner;
        this.transactionTask = transactionTask;
        this.workflowConfiguration = workflowConfiguration;
        this.readOnlyTransactionStore = readOnlyTransactionStore;
    }

    public static <T extends TransactionStore> DefaultWorkflow<T> create(
            T store,
            KeyedTransactionTask<T> transactionTask,
            WorkflowConfiguration configuration,
            ListeningExecutorService executionExecutor) {
        return new DefaultWorkflow<>(
                new ConcurrentTransactionRunner<>(store, executionExecutor),
                transactionTask,
                configuration,
                new ReadOnlyTransactionStore(store));
    }

    @Override
    public WorkflowHistory run() {
        return ImmutableWorkflowHistory.builder()
                .history(sortAndFilterTransactions(runTransactionTask()))
                .transactionStore(readOnlyTransactionStore)
                .build();
    }

    private List<WitnessedTransaction> runTransactionTask() {
        Future<List<WitnessedTransaction>> transactionsFuture =
                concurrentTransactionRunner.runConcurrentTransactionTask(
                        transactionTask, workflowConfiguration.iterationCount());
        try {
            return transactionsFuture.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new SafeRuntimeException(e);
        } catch (ExecutionException e) {
            throw new SafeRuntimeException("Error when running workflow task", e.getCause());
        }
    }

    /**
     * Sorts transactions by their effective timestamp and filters out transactions that are not fully committed.
     */
    @VisibleForTesting
    List<WitnessedTransaction> sortAndFilterTransactions(List<WitnessedTransaction> unorderedTransactions) {
        OnlyCommittedWitnessedTransactionVisitor visitor =
                new OnlyCommittedWitnessedTransactionVisitor(readOnlyTransactionStore);
        return StreamEx.of(unorderedTransactions)
                .mapPartial(transaction -> transaction.accept(visitor))
                .sorted(Comparator.comparingLong(DefaultWorkflow::effectiveTimestamp))
                .toList();
    }

    private static long effectiveTimestamp(WitnessedTransaction witnessedTransaction) {
        return witnessedTransaction.commitTimestamp().orElseGet(witnessedTransaction::startTimestamp);
    }
}
