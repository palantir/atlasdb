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
import com.palantir.atlasdb.buggify.impl.DefaultNativeSamplingSecureRandomFactory;
import com.palantir.atlasdb.workload.invariant.CrossCellInconsistency;
import com.palantir.atlasdb.workload.invariant.ImmutableInvariantReporter;
import com.palantir.atlasdb.workload.invariant.IndexInvariant;
import com.palantir.atlasdb.workload.invariant.IndexInvariantLogReporter;
import com.palantir.atlasdb.workload.invariant.Invariant;
import com.palantir.atlasdb.workload.invariant.InvariantReporter;
import com.palantir.atlasdb.workload.store.ImmutableValueReference;
import com.palantir.atlasdb.workload.store.ImmutableWorkloadCell;
import com.palantir.atlasdb.workload.store.InteractiveTransactionStore;
import com.palantir.atlasdb.workload.store.ReadableTransactionStore;
import com.palantir.atlasdb.workload.store.TableAndWorkloadCell;
import com.palantir.atlasdb.workload.store.WorkloadCell;
import com.palantir.atlasdb.workload.transaction.witnessed.WitnessedReadTransactionAction;
import com.palantir.atlasdb.workload.transaction.witnessed.WitnessedTransaction;
import com.palantir.atlasdb.workload.transaction.witnessed.WitnessedTransactionAction;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import one.util.streamex.EntryStream;
import one.util.streamex.StreamEx;

public final class TransientRowsWorkflows {
    private static final SafeLogger log = SafeLoggerFactory.get(TransientRowsWorkflows.class);

    @VisibleForTesting
    static final int SUMMARY_ROW = -1;

    @VisibleForTesting
    static final int COLUMN = 1;

    @VisibleForTesting
    static final int VALUE = 0;

    private static final SecureRandom SECURE_RANDOM = DefaultNativeSamplingSecureRandomFactory.INSTANCE.create();

    private TransientRowsWorkflows() {
    }

    public static Workflow create(
            InteractiveTransactionStore store,
            TransientRowsWorkflowConfiguration workflowConfiguration,
            ListeningExecutorService executionExecutor) {
        return DefaultWorkflow.create(
                store,
                (txn, taskIndex) -> run(store, workflowConfiguration, taskIndex),
                workflowConfiguration,
                executionExecutor);
    }

    private static Optional<WitnessedTransaction> run(
            InteractiveTransactionStore store, TransientRowsWorkflowConfiguration configuration, int taskIndex) {
        return store.readWrite(txn -> {
            String tableName = configuration.tableConfiguration().tableName();
            txn.write(tableName, ImmutableWorkloadCell.of(SUMMARY_ROW, taskIndex), VALUE);
            txn.write(tableName, ImmutableWorkloadCell.of(taskIndex, COLUMN), VALUE);
            if (taskIndex > 0) {
                int indexToRead = SECURE_RANDOM.nextInt(taskIndex);
                txn.read(tableName, ImmutableWorkloadCell.of(SUMMARY_ROW, indexToRead));
                txn.read(tableName, ImmutableWorkloadCell.of(indexToRead, COLUMN));

                Optional<Integer> primaryCellValue = txn.read(tableName, ImmutableWorkloadCell.of(taskIndex - 1, COLUMN));
                if (primaryCellValue.isPresent()) {
                    txn.delete(tableName, ImmutableWorkloadCell.of(SUMMARY_ROW, taskIndex - 1));
                    txn.delete(tableName, ImmutableWorkloadCell.of(taskIndex - 1, COLUMN));
                }
            }
        });
    }

    public static InvariantReporter<List<CrossCellInconsistency>> invariantReporter(TransientRowsWorkflowConfiguration configuration) {
        IndexInvariant indexInvariant = IndexInvariant.createWithinSameTable(
                store -> getPrimaryRowValues(store, configuration),
                store -> getSummaryRowValues(store, configuration),
                base -> ImmutableWorkloadCell.of(SUMMARY_ROW, base.key()),
                index -> ImmutableWorkloadCell.of(index.column(), COLUMN));
        return new IndexInvariantLogReporter(indexInvariant);
    }

    private static Map<TableAndWorkloadCell, Integer> getSummaryRowValues(ReadableTransactionStore store, TransientRowsWorkflowConfiguration configuration) {
        // Unfortunately, mapToEntryPartial does not exist.
        return StreamEx.of(IntStream.range(0, configuration.iterationCount()).boxed())
                .map(index -> TableAndWorkloadCell.of(configuration.tableConfiguration().tableName(), ImmutableWorkloadCell.of(SUMMARY_ROW, index)))
                .mapToEntry(k -> k, v -> v)
                .mapToValuePartial((tableAndWorkloadCell, unused) -> store.get(tableAndWorkloadCell.tableName(), tableAndWorkloadCell.cell()))
                .toMap();
    }

    private static Map<TableAndWorkloadCell, Integer> getPrimaryRowValues(ReadableTransactionStore store, TransientRowsWorkflowConfiguration configuration) {
        // Unfortunately, mapToEntryPartial does not exist.
        return StreamEx.of(IntStream.range(0, configuration.iterationCount()).boxed())
                .map(index -> TableAndWorkloadCell.of(configuration.tableConfiguration().tableName(), ImmutableWorkloadCell.of(index, COLUMN)))
                .mapToEntry(k -> k, v -> v)
                .mapToValuePartial((tableAndWorkloadCell, unused) -> store.get(tableAndWorkloadCell.tableName(), tableAndWorkloadCell.cell()))
                .toMap();
    }
}
