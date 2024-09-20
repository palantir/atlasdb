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

import com.google.common.collect.Sets;
import com.palantir.atlasdb.transaction.impl.precommit.LockValidityChecker;
import com.palantir.lock.v2.LockToken;
import com.palantir.logsafe.Preconditions;
import com.palantir.logsafe.SafeArg;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import org.immutables.value.Value;

public final class ImmutableTimestampLockManager {
    private final Optional<LockToken> immutableTimestampLock;
    private final LockValidityChecker lockValidityChecker;

    public ImmutableTimestampLockManager(
            Optional<LockToken> immutableTimestampLock, LockValidityChecker lockValidityChecker) {
        this.immutableTimestampLock = immutableTimestampLock;
        this.lockValidityChecker = lockValidityChecker;
    }

    public Optional<ExpiredLocks> getExpiredImmutableTimestampAndCommitLocks(Set<LockToken> commitLocksToken) {
        Set<LockToken> toRefresh = new HashSet<>();

        // TODO(jakubk): Handle this upstream
        if (commitLocksToken != null) {
            toRefresh.addAll(commitLocksToken);
        }
        immutableTimestampLock.ifPresent(toRefresh::add);

        if (toRefresh.isEmpty()) {
            return Optional.empty();
        }

        Set<LockToken> expiredLocks =
                Sets.difference(toRefresh, lockValidityChecker.getStillValidLockTokens(toRefresh));

        if (expiredLocks.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(ImmutableExpiredLocks.of(getExpiredLocksErrorString(commitLocksToken, expiredLocks)));
    }

    public SummarizedLockCheckResult getExpiredImmutableTimestampAndCommitLocksWithFullSummary(
            Set<LockToken> commitLocksToken) {
        Preconditions.checkNotNull(
                commitLocksToken,
                "commitLocksToken was null, not expected to be in a call to"
                        + " getExpiredImmutableTimestampAndCommitLocksWithFullSummary",
                SafeArg.of("immutableTimestampLock", immutableTimestampLock));
        Optional<ExpiredLocks> expiredLocks = getExpiredImmutableTimestampAndCommitLocks(commitLocksToken);
        return SummarizedLockCheckResult.builder()
                .expiredLocks(expiredLocks)
                .immutableTimestampLock(immutableTimestampLock)
                .userProvidedLock(commitLocksToken)
                .build();
    }

    private String getExpiredLocksErrorString(Set<LockToken> commitLocksToken, Set<LockToken> expiredLocks) {
        return "The following immutable timestamp lock was required: " + immutableTimestampLock
                + "; the following commit locks were required: " + commitLocksToken
                + "; the following locks are no longer valid: " + expiredLocks;
    }

    @Value.Immutable
    public interface ExpiredLocks {
        // It seems perverse not to include the actual tokens that have expired, but these are not currently actually
        // used by any caller; we can subsequently include them later if necessary. This API is only intended for
        // internal usage.

        @Value.Parameter
        String errorDescription();

        static ExpiredLocks of(String errorDescription) {
            return ImmutableExpiredLocks.of(errorDescription);
        }
    }

    @Value.Immutable
    public interface SummarizedLockCheckResult {
        Optional<ExpiredLocks> expiredLocks();

        Optional<LockToken> immutableTimestampLock();

        Set<LockToken> userProvidedLock();

        static ImmutableSummarizedLockCheckResult.Builder builder() {
            return ImmutableSummarizedLockCheckResult.builder();
        }
    }
}
