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
import com.palantir.atlasdb.buggify.impl.DefaultNativeSamplingSecureRandomFactory;
import com.palantir.atlasdb.workload.store.ImmutableWorkloadCell;
import com.palantir.atlasdb.workload.store.InteractiveTransactionStore;
import com.palantir.atlasdb.workload.store.ReadOnlyTransactionStore;
import com.palantir.atlasdb.workload.store.WorkloadCell;
import com.palantir.atlasdb.workload.transaction.DeleteTransactionAction;
import com.palantir.atlasdb.workload.transaction.WriteTransactionAction;
import com.palantir.atlasdb.workload.transaction.witnessed.WitnessedTransaction;
import com.palantir.atlasdb.workload.transaction.witnessed.WitnessedTransactions;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import java.security.SecureRandom;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * This workflow attempts to heavily manipulate a few cells across different rows in an attempt to create a large number
 * of tombstones across multiple DB nodes. This aims to verify that Sweep successfully remains up to date as much as
 * possible, even as Cassandra nodes die, without overwhelming the cluster. This differs from SingleRow/SingleCell
 * workflows as it attempts to spread data across nodes, and also intends to do a far greater number of writes to
 * create a large number of AtlasDB tombstones.
 * We're a bit sneaky here, and interpret the iteration count as _per cell_, rather than the total number of iterations
 * across all cells
 * TODO(mdaudali): Configure Cassandra so that we run with more than just 3 Cassandra nodes.
 */
public final class MultipleBusyCellWorkflows {
    private static final SecureRandom SECURE_RANDOM = DefaultNativeSamplingSecureRandomFactory.INSTANCE.create();
    private static final SafeLogger log = SafeLoggerFactory.get(MultipleBusyCellWorkflows.class);

    private MultipleBusyCellWorkflows() {
        // utility
    }

    public static Workflow create(
            InteractiveTransactionStore store,
            MultipleBusyCellWorkflowConfiguration configuration,
            ListeningExecutorService readExecutor,
            ListeningExecutorService writeExecutor) {
        return () -> {
            int actualNumberOfCells = SECURE_RANDOM.nextInt(configuration.maxCells()) + 1;
            log.info("Number of cells to write to: {}", SafeArg.of("numberOfCells", actualNumberOfCells));
            List<WorkloadCell> cells = IntStream.range(0, actualNumberOfCells)
                    .mapToObj(_x -> ImmutableWorkloadCell.of(SECURE_RANDOM.nextInt(), SECURE_RANDOM.nextInt()))
                    .collect(Collectors.toList());
            int maxUpdatesPerCell =
                    Math.toIntExact(Math.round(configuration.maxUpdates() / (double) actualNumberOfCells));
            int maxReadsPerCell = Math.toIntExact(Math.round(configuration.maxReads() / (double) actualNumberOfCells));
            List<ListenableFuture<Optional<WitnessedTransaction>>> writes = cells.stream()
                    .flatMap(cell -> scheduleUpdates(
                            store,
                            cell,
                            maxUpdatesPerCell,
                            configuration.tableConfiguration().tableName(),
                            configuration.deleteProbability(),
                            writeExecutor)
                            .stream())
                    .collect(Collectors.toList());
            List<ListenableFuture<Optional<WitnessedTransaction>>> reads = cells.stream()
                    .flatMap(cell -> scheduleReads(
                            store,
                            cell,
                            maxReadsPerCell,
                            configuration.tableConfiguration().tableName(),
                            readExecutor)
                            .stream())
                    .collect(Collectors.toList());
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

    private static List<ListenableFuture<Optional<WitnessedTransaction>>> scheduleReads(
            InteractiveTransactionStore store,
            WorkloadCell cell,
            int maxReadsPerCell,
            String tableName,
            ListeningExecutorService readExecutor) {
        int actualNumberOfReads = SECURE_RANDOM.nextInt(maxReadsPerCell) + 1;
        log.info(
                "Number of reads for cell {}: {}",
                SafeArg.of("cell", cell),
                SafeArg.of("numberOfReads", actualNumberOfReads));

        return IntStream.range(0, actualNumberOfReads)
                .mapToObj(idx -> readExecutor.submit(() -> store.readWrite(txn -> txn.read(tableName, cell))))
                .collect(Collectors.toList());
    }

    private static List<ListenableFuture<Optional<WitnessedTransaction>>> scheduleUpdates(
            InteractiveTransactionStore store,
            WorkloadCell cell,
            int maxUpdatesPerCell,
            String tableName,
            double deleteProbability,
            ListeningExecutorService writeExecutor) {
        int actualNumberOfWrites = SECURE_RANDOM.nextInt(maxUpdatesPerCell) + 1;
        log.info(
                "Number of writes for cell {}: {}",
                SafeArg.of("cell", cell),
                SafeArg.of("numberOfWrites", actualNumberOfWrites));

        return IntStream.range(0, actualNumberOfWrites)
                .mapToObj(index -> {
                    if (SECURE_RANDOM.nextDouble() < deleteProbability) {
                        log.info("Issuing delete for cell {}", SafeArg.of("cell", cell));
                        return writeExecutor.submit(
                                () -> store.readWrite(List.of(DeleteTransactionAction.of(tableName, cell))));
                    } else {
                        log.info("Issuing write for cell {}", SafeArg.of("cell", cell));
                        return writeExecutor.submit(
                                () -> store.readWrite(List.of(WriteTransactionAction.of(tableName, cell, index))));
                    }
                })
                .collect(Collectors.toList());
    }
}
