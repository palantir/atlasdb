/*
 * Copyright 2015 Palantir Technologies, Inc. All rights reserved.
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

import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.apache.commons.lang3.Validate;

import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import com.palantir.atlasdb.transaction.api.LockAcquisitionException;
import com.palantir.atlasdb.transaction.api.LockAwareTransactionManager;
import com.palantir.atlasdb.transaction.api.LockAwareTransactionTask;
import com.palantir.atlasdb.transaction.api.LockAwareTransactionTasks;
import com.palantir.atlasdb.transaction.api.TransactionFailedException;
import com.palantir.atlasdb.transaction.api.TransactionLockTimeoutException;
import com.palantir.atlasdb.transaction.api.TransactionTask;
import com.palantir.common.collect.IterableUtils;
import com.palantir.lock.HeldLocksToken;
import com.palantir.lock.LockClient;
import com.palantir.lock.LockRefreshToken;
import com.palantir.lock.LockRequest;

public abstract class AbstractLockAwareTransactionManager
        extends AbstractTransactionManager
        implements LockAwareTransactionManager {

    AbstractLockAwareTransactionManager(Supplier<Long> timestampCacheSize) {
        super(timestampCacheSize);
    }

    @Override
    public <T, E extends Exception> T runTaskWithLocksWithRetry(
            Iterable<HeldLocksToken> lockTokens,
            Supplier<LockRequest> lockSupplier,
            LockAwareTransactionTask<T, E> task) throws E, InterruptedException {
        int failureCount = 0;
        UUID runId = UUID.randomUUID();
        while (true) {
            checkOpen();
            LockRequest lockRequest = lockSupplier.get();
            HeldLocksToken lockToken = null;
            if (lockRequest != null) {
                Validate.isTrue(lockRequest.getVersionId() == null, "Using a version id is not allowed");
                HeldLocksToken response = getLockService()
                        .lockAndGetHeldLocks(LockClient.ANONYMOUS.getClientId(), lockRequest);
                if (response == null) {
                    RuntimeException ex = new LockAcquisitionException(
                            "Failed to lock using the provided lock request: " + lockRequest);
                    log.warn("[{}] Could not lock successfully", runId, ex);
                    failureCount++;
                    if (shouldStopRetrying(failureCount)) {
                        log.warn("[{}] Failing after {} tries", runId, failureCount, ex);
                        throw ex;
                    }
                    sleepForBackoff(failureCount);
                    continue;
                }
                lockToken = response;
            }

            try {
                if (lockToken == null) {
                    T result = runTaskWithLocksThrowOnConflict(lockTokens, task);
                    logSuccess(runId, failureCount);
                    return result;
                } else {
                    T result = runTaskWithLocksThrowOnConflict(IterableUtils.append(lockTokens, lockToken), task);
                    logSuccess(runId, failureCount);
                    return result;
                }
            } catch (TransactionFailedException e) {
                if (!e.canTransactionBeRetried()) {
                    log.warn("[{}] Non-retriable exception while processing transaction.", runId, e);
                    throw e;
                }
                if (e instanceof TransactionLockTimeoutException) {
                    refreshAfterLockTimeout(lockTokens, (TransactionLockTimeoutException) e);
                }
                failureCount++;
                if (shouldStopRetrying(failureCount)) {
                    log.warn("[{}] Failing after {} tries", runId, failureCount, e);
                    throw e;
                }
                log.info("[{}] Retrying transaction", runId, e);
            } catch (RuntimeException e) {
                log.warn("[{}] RuntimeException while processing transaction.", runId, e);
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
    public <T, E extends Exception> T runTaskWithLocksWithRetry(
            Supplier<LockRequest> lockSupplier,
            LockAwareTransactionTask<T, E> task)
            throws E, InterruptedException {
        checkOpen();
        return runTaskWithLocksWithRetry(ImmutableList.of(), lockSupplier, task);
    }

    @Override
    public <T, E extends Exception> T runTaskThrowOnConflict(TransactionTask<T, E> task) throws E {
        checkOpen();
        return runTaskWithLocksThrowOnConflict(ImmutableList.of(), LockAwareTransactionTasks.asLockAware(task));
    }

    private void logSuccess(UUID runId, int failureCount) {
        if (failureCount > 0) {
            log.info("[{}] Successfully completed transaction after {} retries.", runId, failureCount);
        }
    }

    private void refreshAfterLockTimeout(Iterable<HeldLocksToken> lockTokens, TransactionLockTimeoutException ex) {
        Set<LockRefreshToken> toRequest = StreamSupport.stream(lockTokens.spliterator(), false)
                .map(HeldLocksToken::getLockRefreshToken)
                .collect(Collectors.toSet());
        if (toRequest.isEmpty()) {
            return;
        }
        Set<LockRefreshToken> refreshedTokens = getLockService().refreshLockRefreshTokens(toRequest);
        Set<LockRefreshToken> failedTokens = Sets.difference(toRequest, refreshedTokens);
        if (!failedTokens.isEmpty()) {
            throw new TransactionLockTimeoutException("Provided lock tokens expired. Retry is not possible. tokens: "
                    + failedTokens,
                    ex);
        }
    }
}
