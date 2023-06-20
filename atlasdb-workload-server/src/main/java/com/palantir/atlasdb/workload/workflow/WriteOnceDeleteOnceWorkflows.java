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
import com.palantir.atlasdb.buggify.impl.DefaultBuggifyFactory;
import com.palantir.atlasdb.buggify.impl.DefaultNativeSamplingSecureRandomFactory;
import com.palantir.atlasdb.workload.store.ImmutableWorkloadCell;
import com.palantir.atlasdb.workload.store.InteractiveTransactionStore;
import com.palantir.atlasdb.workload.store.WorkloadCell;
import com.palantir.atlasdb.workload.transaction.witnessed.WitnessedTransaction;
import java.security.SecureRandom;
import java.util.Optional;

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
        return DefaultWorkflow.create(
                store, (txnStore, index) -> run(txnStore, index, configuration), configuration, executionExecutor);
    }

    private static Optional<WitnessedTransaction> run(
            InteractiveTransactionStore store, int taskIndex, WriteOnceDeleteOnceWorkflowConfiguration configuration) {
        configuration.transactionRateLimiter().acquire();
        return store.readWrite(txn -> {
            Integer cellIndex = RANDOM.nextInt(100);
            if (DefaultBuggifyFactory.INSTANCE.maybe(taskIndex * 0.01).asBoolean()) {
                txn.delete(configuration.tableConfiguration().tableName(), cell(cellIndex));
            } else {
                txn.write(configuration.tableConfiguration().tableName(), cell(cellIndex), RANDOM.nextInt(10000));
            }
        });
    }

    private static WorkloadCell cell(Integer index) {
        return ImmutableWorkloadCell.of(index, COLUMN);
    }
}
