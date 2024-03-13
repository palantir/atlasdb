/*
 * (c) Copyright 2024 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.transaction.impl.precommit;


import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.transaction.api.PreCommitCondition;
import com.palantir.atlasdb.transaction.api.TransactionFailedException;
import com.palantir.atlasdb.transaction.api.TransactionLockTimeoutException;
import com.palantir.atlasdb.transaction.api.precommit.PreCommitRequirementValidator;
import com.palantir.atlasdb.transaction.impl.ImmutableTimestampLockManager;
import com.palantir.atlasdb.transaction.impl.ImmutableTimestampLockManager.ExpiredLocks;
import com.palantir.atlasdb.transaction.impl.metrics.TransactionOutcomeMetrics;
import com.palantir.lock.v2.LockToken;
import com.palantir.logsafe.UnsafeArg;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import java.util.Map;
import java.util.Optional;

public class DefaultPreCommitRequirementValidator implements PreCommitRequirementValidator {
    private static final SafeLogger log = SafeLoggerFactory.get(DefaultPreCommitRequirementValidator.class);

    private final PreCommitCondition userPreCommitCondition;
    private final TransactionOutcomeMetrics metrics;
    private final ImmutableTimestampLockManager immutableTimestampLockManager;

    public DefaultPreCommitRequirementValidator(
            PreCommitCondition userPreCommitCondition,
            TransactionOutcomeMetrics metrics,
            ImmutableTimestampLockManager immutableTimestampLockManager) {
        this.userPreCommitCondition = userPreCommitCondition;
        this.metrics = metrics;
        this.immutableTimestampLockManager = immutableTimestampLockManager;
    }

    @Override
    public void throwIfPreCommitConditionInvalid(long timestamp) {
        try {
            userPreCommitCondition.throwIfConditionInvalid(timestamp);
        } catch (TransactionFailedException ex) {
            metrics.markPreCommitCheckFailed();
            throw ex;
        }
    }

    @Override
    public void throwIfPreCommitConditionInvalidAtCommitOnWriteTransaction(
            Map<TableReference, ? extends Map<Cell, byte[]>> mutations, long timestamp) {
        try {
            userPreCommitCondition.throwIfConditionInvalid(mutations, timestamp);
        } catch (TransactionFailedException ex) {
            metrics.markPreCommitCheckFailed();
            throw ex;
        }
    }

    @Override
    public void throwIfPreCommitRequirementsNotMet(LockToken commitLocksToken, long timestamp) {
        throwIfImmutableTsOrCommitLocksExpired(commitLocksToken);
        throwIfPreCommitConditionInvalid(timestamp);
    }

    @Override
    public void throwIfImmutableTsOrCommitLocksExpired(LockToken commitLocksToken) {
        Optional<ExpiredLocks> expiredLocks = immutableTimestampLockManager.getExpiredImmutableTimestampAndCommitLocks(
                Optional.ofNullable(commitLocksToken));
        if (expiredLocks.isPresent()) {
            throw createDefaultTransactionLockTimeoutException(expiredLocks.get());
        }
    }

    private RuntimeException createDefaultTransactionLockTimeoutException(ExpiredLocks expiredLocks) {
        String baseMsg = "Locks acquired as part of the transaction protocol are no longer valid. ";
        TransactionLockTimeoutException exception = new TransactionLockTimeoutException(baseMsg + expiredLocks.errorDescription());
        log.warn(baseMsg + "{}", UnsafeArg.of("expiredLocksErrorString", expiredLocks.errorDescription()), exception);
        metrics.markLocksExpired();
        throw exception;
    }
}
