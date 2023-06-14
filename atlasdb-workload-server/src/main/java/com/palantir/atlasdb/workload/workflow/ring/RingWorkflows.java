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

package com.palantir.atlasdb.workload.workflow.ring;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.palantir.atlasdb.workload.store.ImmutableWorkloadCell;
import com.palantir.atlasdb.workload.store.InteractiveTransactionStore;
import com.palantir.atlasdb.workload.transaction.InteractiveTransaction;
import com.palantir.atlasdb.workload.transaction.witnessed.WitnessedTransaction;
import com.palantir.atlasdb.workload.workflow.DefaultWorkflow;
import com.palantir.atlasdb.workload.workflow.Workflow;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Runs a workflow which creates a representation of a ring using key-value pairs.
 * The ring is validated and shuffled after every iteration. If the ring ever becomes invalid (cycles, missing data),
 * this indicates that there is a bug in our underlying storage infrastructure, as snapshot isolation has been violated.
 * Snapshot isolation would be violated here, as it indicates that we did not detect a write-write conflict,
 * as every write will be of a correct ring.
 */
public final class RingWorkflows {

    private static final SafeLogger log = SafeLoggerFactory.get(RingWorkflows.class);

    private static final int COLUMN = 0;

    private RingWorkflows() {
        // static factory
    }

    public static Workflow create(
            InteractiveTransactionStore store,
            RingWorkflowConfiguration ringWorkflowConfiguration,
            ListeningExecutorService executionExecutor) {
        AtomicBoolean skipRunning = new AtomicBoolean(false);
        return DefaultWorkflow.create(
                store,
                (txnStore, _index) -> run(txnStore, ringWorkflowConfiguration, skipRunning),
                ringWorkflowConfiguration,
                executionExecutor);
    }

    @VisibleForTesting
    static Workflow create(
            InteractiveTransactionStore store,
            RingWorkflowConfiguration ringWorkflowConfiguration,
            ListeningExecutorService executionExecutor,
            AtomicBoolean skipRunning) {
        return DefaultWorkflow.create(
                store,
                (txnStore, _index) -> run(txnStore, ringWorkflowConfiguration, skipRunning),
                ringWorkflowConfiguration,
                executionExecutor);
    }

    private static Optional<WitnessedTransaction> run(
            InteractiveTransactionStore store,
            RingWorkflowConfiguration workflowConfiguration,
            AtomicBoolean skipRunning) {
        if (skipRunning.get()) {
            return Optional.empty();
        }

        workflowConfiguration.transactionRateLimiter().acquire();
        String table = workflowConfiguration.tableConfiguration().tableName();
        Integer ringSize = workflowConfiguration.ringSize();
        return store.readWrite(txn -> {
            Map<Integer, Optional<Integer>> data = fetchData(table, ringSize, txn);
            try {
                RingGraph ringGraph = RingGraph.fromPartial(data);
                ringGraph
                        .generateNewRing()
                        .asMap()
                        .forEach((rootNode, nextNode) -> txn.write(table, cell(rootNode), nextNode));
            } catch (IllegalArgumentException e) {
                if (skipRunning.compareAndSet(false, true)) {
                    log.error("Detected violation with our ring.", e);
                }
            }
        });
    }

    private static Map<Integer, Optional<Integer>> fetchData(
            String table, int ringSize, InteractiveTransaction transaction) {
        return IntStream.range(0, ringSize)
                .boxed()
                .collect(Collectors.toMap(Function.identity(), index -> transaction.read(table, cell(index))));
    }

    @VisibleForTesting
    static ImmutableWorkloadCell cell(Integer node) {
        return ImmutableWorkloadCell.of(node, COLUMN);
    }
}
