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
import com.palantir.atlasdb.buggify.api.BuggifyFactory;
import com.palantir.atlasdb.buggify.impl.DefaultBuggifyFactory;
import com.palantir.atlasdb.buggify.impl.DefaultNativeSamplingSecureRandomFactory;
import com.palantir.atlasdb.workload.store.ImmutableWorkloadCell;
import com.palantir.atlasdb.workload.store.InteractiveTransactionStore;
import com.palantir.atlasdb.workload.store.WorkloadCell;
import com.palantir.atlasdb.workload.transaction.witnessed.WitnessedTransaction;
import java.security.SecureRandom;
import java.util.Optional;
import java.util.function.IntFunction;

/**
 * The idea of this class is to catch bugs around tombstone drops, by having a
 * workflow which _mostly_ writes once first and then on deletes once for a given cell.
 * Probability and randomness are used to enforce this behaviour, thus it's possible to have a write without a
 * delete, or visa versa; some cells may randomly be written or deleted multiple times as well.
 */
public final class WriteOnceDeleteOnceWorkflows {

    private static final SecureRandom RANDOM = DefaultNativeSamplingSecureRandomFactory.INSTANCE.create();

    private static final Integer COLUMN = 0;

    private WriteOnceDeleteOnceWorkflows() {
        // Utility class
    }

    public static Workflow create(
            InteractiveTransactionStore store,
            WriteOnceDeleteOnceWorkflowConfiguration configuration,
            ListeningExecutorService executionExecutor) {
        return create(store, configuration, executionExecutor, DefaultBuggifyFactory.INSTANCE, RANDOM::nextInt);
    }

    @VisibleForTesting
    static Workflow create(
            InteractiveTransactionStore store,
            WriteOnceDeleteOnceWorkflowConfiguration configuration,
            ListeningExecutorService executionExecutor,
            BuggifyFactory buggifyFactory,
            IntFunction<Integer> randomNumberGenerator) {
        WriteOnceDeleteOnceWorkflow workflow =
                new WriteOnceDeleteOnceWorkflow(buggifyFactory, randomNumberGenerator, configuration);
        return DefaultWorkflow.create(store, workflow::run, configuration, executionExecutor);
    }

    private static class WriteOnceDeleteOnceWorkflow {
        private final BuggifyFactory buggifyFactory;
        private final IntFunction<Integer> rowKeyGenerator;
        private final WriteOnceDeleteOnceWorkflowConfiguration configuration;

        public WriteOnceDeleteOnceWorkflow(
                BuggifyFactory buggifyFactory,
                IntFunction<Integer> rowKeyGenerator,
                WriteOnceDeleteOnceWorkflowConfiguration configuration) {
            this.buggifyFactory = buggifyFactory;
            this.rowKeyGenerator = rowKeyGenerator;
            this.configuration = configuration;
        }

        public Optional<WitnessedTransaction> run(InteractiveTransactionStore store, Integer taskIndex) {
            configuration.transactionRateLimiter().acquire();
            return store.readWrite(txn -> {
                Integer rowKey = rowKeyGenerator.apply(configuration.maxKey());
                if (buggifyFactory
                        .maybe(calculateChanceForDelete(taskIndex, configuration.iterationCount()))
                        .asBoolean()) {
                    txn.delete(configuration.tableConfiguration().tableName(), cell(rowKey));
                } else {
                    txn.write(configuration.tableConfiguration().tableName(), cell(rowKey), RANDOM.nextInt(10000));
                }
            });
        }
    }

    @VisibleForTesting
    static Double calculateChanceForDelete(Integer taskIndex, Integer iterationCount) {
        // Index generation is [inclusive, exclusive], so we need to add 1 to the task index.
        return (taskIndex + 1.0) / iterationCount;
    }

    @VisibleForTesting
    static WorkloadCell cell(Integer index) {
        return ImmutableWorkloadCell.of(index, COLUMN);
    }
}
