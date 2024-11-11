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
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.palantir.atlasdb.workload.store.ImmutableWorkloadCell;
import com.palantir.atlasdb.workload.store.InteractiveTransactionStore;
import com.palantir.atlasdb.workload.store.ReadOnlyTransactionStore;
import com.palantir.atlasdb.workload.store.WorkloadCell;
import com.palantir.atlasdb.workload.transaction.DeleteTransactionAction;
import com.palantir.atlasdb.workload.transaction.WriteTransactionAction;
import com.palantir.atlasdb.workload.transaction.witnessed.WitnessedTransaction;
import com.palantir.atlasdb.workload.transaction.witnessed.WitnessedTransactions;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * This workflow attempts to manipulate a single cell in a table, scheduling transactions that perform writes and
 * deletes, along with transactions that simply read. Notice that unlike other workflows, because the load is
 * considerably heterogeneous, we do not use a {@link DefaultWorkflow} to schedule transactions.
 */
public final class SingleBusyCellWorkflows {
    private SingleBusyCellWorkflows() {
        // utility
    }

    @VisibleForTesting
    static final WorkloadCell BUSY_CELL = ImmutableWorkloadCell.of(1, 1);

    public static Workflow create(
            InteractiveTransactionStore store,
            SingleBusyCellWorkflowConfiguration configuration,
            ListeningExecutorService readExecutor,
            ListeningExecutorService writeExecutor) {
        return () -> {
            List<ListenableFuture<Optional<WitnessedTransaction>>> reads =
                    scheduleReadsAndTouches(store, configuration, readExecutor);
            List<ListenableFuture<Optional<WitnessedTransaction>>> writes =
                    scheduleWrites(store, configuration, writeExecutor);

            ListenableFuture<List<WitnessedTransaction>> witnessedTransactions = Futures.transform(
                    Futures.allAsList(
                            Stream.of(reads, writes).flatMap(Collection::stream).collect(Collectors.toList())),
                    maybeWitnessedTransactions -> maybeWitnessedTransactions.stream()
                            .filter(Optional::isPresent)
                            .map(Optional::get)
                            .collect(Collectors.toList()),
                    MoreExecutors.directExecutor());
            ReadOnlyTransactionStore readOnlyTransactionStore = new ReadOnlyTransactionStore(store);
            return ImmutableWorkflowHistory.builder()
                    .addAllHistory(WitnessedTransactions.sortAndFilterTransactions(
                            readOnlyTransactionStore, Futures.getUnchecked(witnessedTransactions)))
                    .transactionStore(readOnlyTransactionStore)
                    .build();
        };
    }

    private static List<ListenableFuture<Optional<WitnessedTransaction>>> scheduleReadsAndTouches(
            InteractiveTransactionStore store,
            SingleBusyCellWorkflowConfiguration configuration,
            ListeningExecutorService readExecutor) {
        return IntStream.range(0, configuration.iterationCount() / 2)
                .mapToObj(idx -> readExecutor.submit(() -> store.readWrite(txn -> {
                    Optional<Integer> value =
                            txn.read(configuration.tableConfiguration().tableName(), BUSY_CELL);
                    txn.write(configuration.tableConfiguration().tableName(), BUSY_CELL, value.orElse(-1));
                })))
                .collect(Collectors.toList());
    }

    private static List<ListenableFuture<Optional<WitnessedTransaction>>> scheduleWrites(
            InteractiveTransactionStore store,
            SingleBusyCellWorkflowConfiguration configuration,
            ListeningExecutorService writeExecutor) {
        return IntStream.range(0, configuration.iterationCount() / 4)
                .boxed()
                .flatMap(index -> Stream.of(
                        writeExecutor.submit(() -> store.readWrite(List.of(WriteTransactionAction.of(
                                configuration.tableConfiguration().tableName(), BUSY_CELL, index)))),
                        writeExecutor.submit(() -> store.readWrite(List.of(DeleteTransactionAction.of(
                                configuration.tableConfiguration().tableName(), BUSY_CELL))))))
                .collect(Collectors.toList());
    }
}
