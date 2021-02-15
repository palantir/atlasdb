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

import com.palantir.atlasdb.cache.TimestampCache;
import com.palantir.atlasdb.transaction.api.ConditionAwareTransactionTask;
import com.palantir.atlasdb.transaction.api.PreCommitCondition;
import com.palantir.atlasdb.transaction.api.TransactionTask;
import com.palantir.atlasdb.util.MetricsManager;
import java.util.function.Supplier;

public abstract class AbstractConditionAwareTransactionManager extends AbstractTransactionManager {
    private final Supplier<TransactionRetryStrategy> retryStrategy;

    protected static final PreCommitCondition NO_OP_CONDITION = new PreCommitCondition() {
        @Override
        public void throwIfConditionInvalid(long timestamp) {}

        @Override
        public void cleanup() {}
    };

    AbstractConditionAwareTransactionManager(
            MetricsManager metricsManager,
            TimestampCache timestampCache,
            Supplier<TransactionRetryStrategy> retryStrategy) {
        super(metricsManager, timestampCache);
        this.retryStrategy = retryStrategy;
    }

    @Override
    public <T, C extends PreCommitCondition, E extends Exception> T runTaskWithConditionWithRetry(
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
        return runTaskWithConditionThrowOnConflict(NO_OP_CONDITION, (txn, condition) -> task.execute(txn));
    }

    @Override
    public <T, E extends Exception> T runTaskWithRetry(TransactionTask<T, E> task) throws E {
        return runTaskWithConditionWithRetry(() -> NO_OP_CONDITION, (txn, condition) -> task.execute(txn));
    }

    @Override
    public <T, E extends Exception> T runTaskReadOnly(TransactionTask<T, E> task) throws E {
        return runTaskWithConditionReadOnly(NO_OP_CONDITION, (transaction, condition) -> task.execute(transaction));
    }
}
