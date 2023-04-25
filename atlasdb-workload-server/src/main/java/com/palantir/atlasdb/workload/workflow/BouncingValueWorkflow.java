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
import com.palantir.atlasdb.workload.transaction.witnessed.FullyWitnessedTransaction;
import com.palantir.atlasdb.workload.transaction.witnessed.OnlyCommittedWitnessedTransactionVisitor;
import com.palantir.atlasdb.workload.transaction.witnessed.WitnessedTransaction;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import one.util.streamex.StreamEx;

public class BouncingValueWorkflow {
    public static final String TEST_TABLE = "bouncingValue";
    private static final WorkloadCell BUSY_CELL = ImmutableWorkloadCell.of(1, 1);

    public static Workflow createBouncingValue(InteractiveTransactionStore store) {
        return new Workflow() {
            @Override
            public WorkflowHistory run() {
                ListeningExecutorService readExecutor =
                        MoreExecutors.listeningDecorator(Executors.newSingleThreadExecutor());
                ListeningExecutorService writeExecutor =
                        MoreExecutors.listeningDecorator(Executors.newSingleThreadExecutor());
                List<ListenableFuture<Optional<WitnessedTransaction>>> reads = IntStream.range(0, 200)
                        .mapToObj(idx -> readExecutor.submit(() -> {
                            return store.readWrite(txn -> {
                                Optional<Integer> value = txn.read(TEST_TABLE, BUSY_CELL);
                                txn.write(TEST_TABLE, BUSY_CELL, value.orElse(-1));
                            });
                        }))
                        .collect(Collectors.toList());

                List<ListenableFuture<Optional<WitnessedTransaction>>> writes = new ArrayList<>();
                for (int i = 0; i < 100; i++) {
                    int stableI = i;
                    writes.add(writeExecutor.submit(
                            () -> store.readWrite(List.of(WriteTransactionAction.of(TEST_TABLE, BUSY_CELL, stableI)))));
                    writes.add(writeExecutor.submit(
                            () -> store.readWrite(List.of(DeleteTransactionAction.of(TEST_TABLE, BUSY_CELL)))));
                }
                ListenableFuture<List<WitnessedTransaction>> witnessedTransactions = Futures.transform(
                        Futures.allAsList(Stream.of(reads, writes)
                                .flatMap(Collection::stream)
                                .collect(Collectors.toList())),
                        maybeWitnessedTransactions -> maybeWitnessedTransactions.stream()
                                .flatMap(Optional::stream)
                                .collect(Collectors.toList()),
                        MoreExecutors.directExecutor());
                ReadOnlyTransactionStore readOnlyTransactionStore = new ReadOnlyTransactionStore(store);
                return ImmutableWorkflowHistory.builder()
                        .addAllHistory(sortAndFilterTransactions(
                                readOnlyTransactionStore, Futures.getUnchecked(witnessedTransactions)))
                        .transactionStore(readOnlyTransactionStore)
                        .build();
            }
        };
    }

    // Naughty
    static List<FullyWitnessedTransaction> sortAndFilterTransactions(
            ReadOnlyTransactionStore readOnlyTransactionStore, List<WitnessedTransaction> unorderedTransactions) {
        OnlyCommittedWitnessedTransactionVisitor visitor =
                new OnlyCommittedWitnessedTransactionVisitor(readOnlyTransactionStore);
        return StreamEx.of(unorderedTransactions)
                .mapPartial(transaction -> transaction.accept(visitor))
                .sorted(Comparator.comparingLong(BouncingValueWorkflow::effectiveTimestamp))
                .toList();
    }

    private static long effectiveTimestamp(WitnessedTransaction witnessedTransaction) {
        return witnessedTransaction.commitTimestamp().orElseGet(witnessedTransaction::startTimestamp);
    }
}
