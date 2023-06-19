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

import com.google.common.util.concurrent.ListeningExecutorService;
import com.palantir.atlasdb.workload.runner.AntithesisWorkflowValidatorRunner;
import com.palantir.atlasdb.workload.store.ReadOnlyTransactionStore;
import com.palantir.atlasdb.workload.store.TransactionStore;
import com.palantir.atlasdb.workload.transaction.witnessed.WitnessedTransaction;
import com.palantir.atlasdb.workload.transaction.witnessed.WitnessedTransactions;
import com.palantir.logsafe.exceptions.SafeRuntimeException;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public final class DefaultWorkflow<T extends TransactionStore> implements Workflow {
    private static final SafeLogger log = SafeLoggerFactory.get(DefaultWorkflow.class);

    private final ConcurrentTransactionRunner<T> concurrentTransactionRunner;
    private final KeyedTransactionTask<T> transactionTask;
    private final WorkflowConfiguration workflowConfiguration;
    private final ReadOnlyTransactionStore readOnlyTransactionStore;
    private final Supplier<Optional<WitnessedTransaction>> bootstrap;

    private DefaultWorkflow(
            ConcurrentTransactionRunner<T> concurrentTransactionRunner,
            KeyedTransactionTask<T> transactionTask,
            WorkflowConfiguration workflowConfiguration,
            ReadOnlyTransactionStore readOnlyTransactionStore,
            Supplier<Optional<WitnessedTransaction>> bootstrap) {
        this.concurrentTransactionRunner = concurrentTransactionRunner;
        this.transactionTask = transactionTask;
        this.workflowConfiguration = workflowConfiguration;
        this.readOnlyTransactionStore = readOnlyTransactionStore;
        this.bootstrap = bootstrap;
    }

    public static <T extends TransactionStore> DefaultWorkflow<T> create(
            T store,
            KeyedTransactionTask<T> transactionTask,
            WorkflowConfiguration configuration,
            ListeningExecutorService executionExecutor,
            KeyedTransactionTask<T> setupTask) {
        return new DefaultWorkflow<>(
                new ConcurrentTransactionRunner<>(store, executionExecutor),
                transactionTask,
                configuration,
                new ReadOnlyTransactionStore(store),
                () -> setupTask.apply(store, 0));
    }

    public static <T extends TransactionStore> DefaultWorkflow<T> create(
            T store,
            KeyedTransactionTask<T> transactionTask,
            WorkflowConfiguration configuration,
            ListeningExecutorService executionExecutor) {
        return create(store, transactionTask, configuration, executionExecutor, (s, i) -> Optional.empty());
    }

    @Override
    public WorkflowHistory run() {
        Optional<WitnessedTransaction> bootstrapWitness = bootstrap.get();
        log.info("antithesis: start_faults");
        return ImmutableWorkflowHistory.builder()
                .history(WitnessedTransactions.sortAndFilterTransactions(
                        readOnlyTransactionStore,
                        Stream.concat(bootstrapWitness.stream(), runTransactionTask().stream())
                                .collect(Collectors.toList())))
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
}
