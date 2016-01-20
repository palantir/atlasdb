/**
 * Copyright 2015 Palantir Technologies
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

import org.apache.commons.lang.Validate;

import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.palantir.atlasdb.transaction.api.LockAcquisitionException;
import com.palantir.atlasdb.transaction.api.LockAwareTransactionManager;
import com.palantir.atlasdb.transaction.api.LockAwareTransactionTask;
import com.palantir.atlasdb.transaction.api.LockAwareTransactionTasks;
import com.palantir.atlasdb.transaction.api.TransactionFailedException;
import com.palantir.atlasdb.transaction.api.TransactionTask;
import com.palantir.common.collect.IterableUtils;
import com.palantir.lock.HeldLocksToken;
import com.palantir.lock.LockClient;
import com.palantir.lock.LockRequest;

public abstract class AbstractLockAwareTransactionManager extends AbstractTransactionManager implements LockAwareTransactionManager {

    @Override
    public <T, E extends Exception> T runTaskWithLocksWithRetry(Iterable<HeldLocksToken> lockTokens,
                                                                Supplier<LockRequest> lockSupplier,
                                                                LockAwareTransactionTask<T, E> task) throws E, InterruptedException {
        int failureCount = 0;
        while (true) {
            LockRequest lockRequest = lockSupplier.get();
            HeldLocksToken lockToken = null;
            if (lockRequest != null) {
                Validate.isTrue(lockRequest.getVersionId() == null, "Using a version id is not allowed");
                HeldLocksToken response = getLockService().lockAndGetHeldLocks(LockClient.ANONYMOUS.getClientId(), lockRequest);
                if (response == null) {
                    RuntimeException e = new LockAcquisitionException("Failed to lock using the provided lock request: " + lockRequest);
                    log.warn("Could not lock successfullly", e);
                    failureCount++;
                    if (shouldStopRetrying(failureCount)) {
                        log.warn("Failing after " + failureCount + " tries", e);
                        throw e;
                    }
                    sleepForBackoff(failureCount);
                    continue;
                }
                lockToken = response;
            }

            try {
                if (lockToken == null) {
                    return runTaskWithLocksThrowOnConflict(lockTokens, task);
                } else {
                    return runTaskWithLocksThrowOnConflict(IterableUtils.append(lockTokens, lockToken), task);
                }
            } catch (TransactionFailedException e) {
                if (!e.canTransactionBeRetried()) {
                    log.warn("Non-retriable exception while processing transaction.", e);
                    throw e;
                }
                failureCount++;
                if (shouldStopRetrying(failureCount)) {
                    log.warn("Failing after " + failureCount + " tries", e);
                    throw e;
                }
                log.info("retrying transaction", e);
            } catch (RuntimeException e) {
                log.warn("RuntimeException while processing transaction.", e);
                throw e;
            } finally {
                if (lockToken != null) {
                    getLockService().unlock(lockToken.getLockRefreshToken());
                }
            }

            sleepForBackoff(failureCount);
        }
    }

    @Override
    public <T, E extends Exception> T runTaskThrowOnConflict(TransactionTask<T, E> task) throws E {
        return runTaskWithLocksThrowOnConflict(ImmutableList.<HeldLocksToken>of(), LockAwareTransactionTasks.asLockAware(task));
    }

    @Override
    public <T, E extends Exception> T runTaskWithLocksWithRetry(Supplier<LockRequest> lockSupplier,
                                                                LockAwareTransactionTask<T, E> task)
            throws E, InterruptedException {
        return runTaskWithLocksWithRetry(ImmutableList.<HeldLocksToken>of(), lockSupplier, task);
    }

}
