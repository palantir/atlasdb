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

import com.palantir.atlasdb.workload.store.TransactionStore;
import com.palantir.atlasdb.workload.transaction.witnessed.WitnessedTransaction;
import java.util.List;
import java.util.concurrent.Future;
import java.util.function.Consumer;

public class DefaultWorkflow implements Workflow {
    private final ConcurrentTransactionRunner concurrentTransactionRunner;
    private final HistoryConsumptionRegistrar historyConsumptionRegistrar;
    private final IndexedTransactionTask transactionTask;
    private final WorkflowConfiguration workflowConfiguration;

    private DefaultWorkflow(
            ConcurrentTransactionRunner concurrentTransactionRunner,
            HistoryConsumptionRegistrar historyConsumptionRegistrar,
            IndexedTransactionTask transactionTask,
            WorkflowConfiguration workflowConfiguration) {
        this.concurrentTransactionRunner = concurrentTransactionRunner;
        this.historyConsumptionRegistrar = historyConsumptionRegistrar;
        this.transactionTask = transactionTask;
        this.workflowConfiguration = workflowConfiguration;
    }

    public static Workflow create(
            TransactionStore store,
            IndexedTransactionTask transactionTask,
            WorkflowConfiguration configuration) {
        return new DefaultWorkflow(
                new ConcurrentTransactionRunner(store, configuration.executionExecutor()),
                new HistoryConsumptionRegistrar(store),
                transactionTask,
                configuration);
    }

    @Override
    public void run() {
        Future<List<WitnessedTransaction>> witnessedTransactions =
                concurrentTransactionRunner.runConcurrentTransactionTask(
                        transactionTask, workflowConfiguration.iterationCount());
        historyConsumptionRegistrar.runOnConsumersBlocking(witnessedTransactions);
    }

    @Override
    public Workflow onComplete(Consumer<WorkflowHistory> consumer) {
        historyConsumptionRegistrar.addConsumer(consumer);
        return this;
    }
}
