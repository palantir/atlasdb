/*
 * Copyright 2017 Palantir Technologies, Inc. All rights reserved.
 *
 * Licensed under the BSD-3 License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://opensource.org/licenses/BSD-3-Clause
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.palantir.atlasdb.transaction.impl;

import java.util.UUID;

import com.google.common.base.Supplier;
import com.palantir.atlasdb.transaction.api.ConditionAwareTransactionManager;
import com.palantir.atlasdb.transaction.api.ConditionAwareTransactionTask;
import com.palantir.atlasdb.transaction.api.PreCommitCondition;
import com.palantir.atlasdb.transaction.api.TransactionFailedException;
import com.palantir.atlasdb.transaction.api.TransactionTask;
import com.palantir.atlasdb.util.MetricsManager;
import com.palantir.common.base.Throwables;
import com.palantir.exception.NotInitializedException;
import com.palantir.logsafe.SafeArg;

public abstract class AbstractConditionAwareTransactionManager extends AbstractTransactionManager
        implements ConditionAwareTransactionManager {

    protected static final PreCommitCondition NO_OP_CONDITION = new PreCommitCondition() {
        @Override
        public void throwIfConditionInvalid(long timestamp) {}

        @Override
        public void cleanup() {}
    };

    AbstractConditionAwareTransactionManager(MetricsManager metricsManager, Supplier<Long> timestampCacheSize) {
        super(metricsManager, timestampCacheSize);
    }

    @Override
    public <T, C extends PreCommitCondition, E extends Exception> T runTaskWithConditionWithRetry(
            Supplier<C> conditionSupplier, ConditionAwareTransactionTask<T, C, E> task) throws E {
        int failureCount = 0;
        UUID runId = UUID.randomUUID();
        while (true) {
            checkOpen();
            try {
                C condition = conditionSupplier.get();
                T result = runTaskWithConditionThrowOnConflict(condition, task);
                if (failureCount > 0) {
                    log.info("[{}] Successfully completed transaction after {} retries.",
                            SafeArg.of("runId", runId),
                            SafeArg.of("failureCount", failureCount));
                }
                return result;
            } catch (TransactionFailedException e) {
                if (!e.canTransactionBeRetried()) {
                    log.warn("[{}] Non-retriable exception while processing transaction.",
                            SafeArg.of("runId", runId),
                            SafeArg.of("failureCount", failureCount));
                    throw e;
                }
                failureCount++;
                if (shouldStopRetrying(failureCount)) {
                    log.warn("[{}] Failing after {} tries.",
                            SafeArg.of("runId", runId),
                            SafeArg.of("failureCount", failureCount), e);
                    throw Throwables.rewrap(String.format("Failing after %d tries.", failureCount), e);
                }
                log.info("[{}] Retrying transaction after {} failure(s).",
                        SafeArg.of("runId", runId),
                        SafeArg.of("failureCount", failureCount), e);
            } catch (NotInitializedException e) {
                log.info("TransactionManager is not initialized. Aborting transaction with runTaskWithRetry", e);
                throw e;
            } catch (RuntimeException e) {
                log.warn("[{}] RuntimeException while processing transaction.", SafeArg.of("runId", runId), e);
                throw e;
            }
            sleepForBackoff(failureCount);
        }
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
        return runTaskReadOnlyWithCondition(NO_OP_CONDITION, (transaction, condition) -> task.execute(transaction));
    }
}
