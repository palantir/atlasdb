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

import com.palantir.atlasdb.workload.store.ReadableTransactionStore;
import com.palantir.atlasdb.workload.transaction.witnessed.WitnessedTransaction;
import com.palantir.logsafe.exceptions.SafeRuntimeException;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.function.Consumer;

public class HistoryConsumptionRegistrar {
    private static final SafeLogger log = SafeLoggerFactory.get(HistoryConsumptionRegistrar.class);

    private final ReadableTransactionStore readableTransactionStore;
    private final Deque<Consumer<WorkflowHistory>> consumers = new ConcurrentLinkedDeque<>();

    public HistoryConsumptionRegistrar(ReadableTransactionStore readableTransactionStore) {
        this.readableTransactionStore = readableTransactionStore;
    }

    public void addConsumer(Consumer<WorkflowHistory> consumer) {
        consumers.add(consumer);
    }

    public void runOnConsumersBlocking(Future<List<WitnessedTransaction>> witnessedTransactions) {
        try {
            WorkflowHistory workflowHistory = ImmutableWorkflowHistory.builder()
                    .history(witnessedTransactions.get())
                    .transactionStore(readableTransactionStore)
                    .build();
            for (Consumer<WorkflowHistory> consumer : consumers) {
                consumer.accept(workflowHistory);
            }
        } catch (ExecutionException e) {
            log.warn("Error when running workflow.", e);
            throw new SafeRuntimeException(e.getCause());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new SafeRuntimeException(e);
        }
    }
}
