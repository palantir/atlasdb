/*
 * (c) Copyright 2018 Palantir Technologies Inc. All rights reserved.
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
package com.palantir.atlasdb.transaction.impl;

import java.util.function.Supplier;

import com.palantir.atlasdb.cache.TimestampCache;
import com.palantir.atlasdb.transaction.api.ConditionAwareTransactionTask;
import com.palantir.atlasdb.transaction.api.PreCommitCondition;
import com.palantir.atlasdb.transaction.api.PreCommitConditionWithWatches;
import com.palantir.atlasdb.transaction.api.PreCommitConditions;
import com.palantir.atlasdb.transaction.api.TransactionTask;
import com.palantir.atlasdb.util.MetricsManager;

public abstract class AbstractConditionAwareTransactionManager extends AbstractTransactionManager {
    private final Supplier<TransactionRetryStrategy> retryStrategy;

    AbstractConditionAwareTransactionManager(
            MetricsManager metricsManager, TimestampCache timestampCache, Supplier<TransactionRetryStrategy> retryStrategy) {
        super(metricsManager, timestampCache);
        this.retryStrategy = retryStrategy;
    }

    @Override
    public <T, C extends PreCommitConditionWithWatches, E extends Exception> T runTaskWithConditionWithRetry(
            Supplier<C> conditionSupplier, ConditionAwareTransactionTask<T, C, E> task) throws E {
        return retryStrategy.get().runWithRetry(this::shouldStopRetrying, () -> {
            checkOpen();
            C condition = conditionSupplier.get();
            T result = runTaskWithConditionThrowOnConflict(condition, task);
            return result;
        });
    }

    @Override
    public <T, E extends Exception> T runTaskThrowOnConflict(TransactionTask<T, E> task) throws E {
        return runTaskWithConditionThrowOnConflict(PreCommitConditions.NO_OP, (txn, condition) -> task.execute(txn));
    }

    @Override
    public <T, E extends Exception> T runTaskWithRetry(TransactionTask<T, E> task) throws E {
        return runTaskWithConditionWithRetry(() -> PreCommitConditions.NO_OP, (txn, condition) -> task.execute(txn));
    }

    @Override
    public <T, E extends Exception> T runTaskReadOnly(TransactionTask<T, E> task) throws E {
        return runTaskWithConditionReadOnly(PreCommitConditions.NO_OP,
                (transaction, condition) -> task.execute(transaction));
    }
}
