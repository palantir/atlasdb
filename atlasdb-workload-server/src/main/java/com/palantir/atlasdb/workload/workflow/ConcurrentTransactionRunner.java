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
import com.palantir.atlasdb.workload.store.TransactionStore;
import com.palantir.atlasdb.workload.transaction.witnessed.WitnessedTransaction;
import com.palantir.logsafe.Preconditions;
import com.palantir.logsafe.SafeArg;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public final class ConcurrentTransactionRunner {
    private final TransactionStore transactionStore;
    private final ListeningExecutorService listeningExecutorService;

    public ConcurrentTransactionRunner(
            TransactionStore transactionStore, ListeningExecutorService listeningExecutorService) {
        this.transactionStore = transactionStore;
        this.listeningExecutorService = listeningExecutorService;
    }

    public Future<List<WitnessedTransaction>> runConcurrentTransactionTask(
            IndexedTransactionTask transactionTask, int taskMultiplicity) {
        Preconditions.checkArgument(
                taskMultiplicity >= 0,
                "Tasks must be run a non-negative number of times",
                SafeArg.of("providedTaskMultiplicity", taskMultiplicity));
        List<ListenableFuture<Optional<WitnessedTransaction>>> taskFutures = IntStream.range(0, taskMultiplicity)
                .mapToObj(
                        index -> listeningExecutorService.submit(() -> transactionTask.apply(transactionStore, index)))
                .collect(Collectors.toList());
        return Futures.transform(
                Futures.allAsList(taskFutures),
                outcome -> outcome.stream().flatMap(Optional::stream).collect(Collectors.toList()),
                MoreExecutors.directExecutor());
    }
}
