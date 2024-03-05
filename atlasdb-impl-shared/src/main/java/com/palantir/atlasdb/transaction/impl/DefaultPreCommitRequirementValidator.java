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

package com.palantir.atlasdb.transaction.impl;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.transaction.TransactionConfig;
import com.palantir.atlasdb.transaction.api.PreCommitCondition;
import com.palantir.atlasdb.transaction.api.TransactionFailedException;
import com.palantir.atlasdb.transaction.api.TransactionLockTimeoutException;
import com.palantir.atlasdb.transaction.impl.metrics.TransactionOutcomeMetrics;
import com.palantir.lock.v2.LockToken;
import com.palantir.logsafe.UnsafeArg;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;

// Transaction scoped. One instance should live for the duration of a transaction.
public class DefaultPreCommitRequirementValidator implements PreCommitRequirementValidator {
    private static final SafeLogger log = SafeLoggerFactory.get(DefaultPreCommitRequirementValidator.class);

    private final PreCommitCondition userPreCommitCondition;
    private final SweepStrategyManager sweepStrategyManager;
    private final Supplier<TransactionConfig> transactionConfig;
    private final LockRefresher lockRefresher;
    private final Optional<LockToken> immutableTimestampLock;
    private final boolean validateLocksOnReads;
    private final TransactionOutcomeMetrics transactionOutcomeMetrics;

    public DefaultPreCommitRequirementValidator(
            PreCommitCondition userPreCommitCondition,
            SweepStrategyManager sweepStrategyManager,
            Supplier<TransactionConfig> transactionConfig,
            LockRefresher lockRefresher,
            Optional<LockToken> immutableTimestampLock,
            boolean validateLocksOnReads,
            TransactionOutcomeMetrics transactionOutcomeMetrics) {
        this.userPreCommitCondition = userPreCommitCondition;
        this.sweepStrategyManager = sweepStrategyManager;
        this.transactionConfig = transactionConfig;
        this.lockRefresher = lockRefresher;
        this.immutableTimestampLock = immutableTimestampLock;
        this.validateLocksOnReads = validateLocksOnReads;
        this.transactionOutcomeMetrics = transactionOutcomeMetrics;
    }

    // TODO (jkong): The boolean means "are there unvalidated reads"?
    @Override
    public boolean throwIfPreCommitRequirementsNotMetOnRead(
            TableReference tableRef, long timestamp, boolean allPossibleCellsReadAndPresent) {
        if (isValidationNecessaryOnReads(tableRef, allPossibleCellsReadAndPresent)) {
            throwIfPreCommitRequirementsNotMet(Optional.empty(), timestamp);
        }
        return !allPossibleCellsReadAndPresent;
    }

    @Override
    public void throwIfPreCommitConditionInvalidAtCommitOnWriteTransaction(
            Map<TableReference, ? extends Map<Cell, byte[]>> mutations, long timestamp) {
        try {
            userPreCommitCondition.throwIfConditionInvalid(mutations, timestamp);
        } catch (TransactionFailedException ex) {
            transactionOutcomeMetrics.markPreCommitCheckFailed();
            throw ex;
        }
    }

    @Override
    public void throwIfImmutableTsOrCommitLocksExpired(Optional<LockToken> commitLocksToken) {
        Set<LockToken> expiredLocks = refreshCommitAndImmutableTsLocks(commitLocksToken);
        if (!expiredLocks.isEmpty()) {
            final String baseMsg = "Locks acquired as part of the transaction protocol are no longer valid. ";
            String expiredLocksErrorString = getExpiredLocksErrorString(commitLocksToken, expiredLocks);
            TransactionLockTimeoutException ex = new TransactionLockTimeoutException(baseMsg + expiredLocksErrorString);
            log.warn(baseMsg + "{}", UnsafeArg.of("expiredLocksErrorString", expiredLocksErrorString), ex);
            transactionOutcomeMetrics.markLocksExpired();
            throw ex;
        }
    }

    @Override
    public void throwIfPreCommitRequirementsNotMet(Optional<LockToken> commitLocksToken, long timestamp) {
        throwIfImmutableTsOrCommitLocksExpired(commitLocksToken);
        throwIfPreCommitConditionInvalid(timestamp);
    }

    @Override
    public void throwIfPreCommitConditionInvalid(long timestamp) {
        try {
            userPreCommitCondition.throwIfConditionInvalid(timestamp);
        } catch (TransactionFailedException ex) {
            transactionOutcomeMetrics.markPreCommitCheckFailed();
            throw ex;
        }
    }

    private boolean isValidationNecessaryOnReads(TableReference tableRef, boolean allPossibleCellsReadAndPresent) {
        return validateLocksOnReads && requiresImmutableTimestampLocking(tableRef, allPossibleCellsReadAndPresent);
    }

    private boolean requiresImmutableTimestampLocking(TableReference tableRef, boolean allPossibleCellsReadAndPresent) {
        return sweepStrategyManager.get(tableRef).mustCheckImmutableLock(allPossibleCellsReadAndPresent)
                || transactionConfig.get().lockImmutableTsOnReadOnlyTransactions();
    }

    private String getExpiredLocksErrorString(Optional<LockToken> commitLocksToken, Set<LockToken> expiredLocks) {
        return "The following immutable timestamp lock was required: " + immutableTimestampLock
                + "; the following commit locks were required: " + commitLocksToken
                + "; the following locks are no longer valid: " + expiredLocks;
    }

    /**
     * Refreshes external and commit locks.
     *
     * @return set of locks that could not be refreshed
     */
    private Set<LockToken> refreshCommitAndImmutableTsLocks(Optional<LockToken> commitLocksToken) {
        Set<LockToken> toRefresh = new HashSet<>();
        commitLocksToken.ifPresent(toRefresh::add);
        immutableTimestampLock.ifPresent(toRefresh::add);

        if (toRefresh.isEmpty()) {
            return ImmutableSet.of();
        }

        return Sets.difference(toRefresh, lockRefresher.refreshLocks(toRefresh)).immutableCopy();
    }
}
