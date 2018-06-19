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

package com.palantir.atlasdb.transaction.impl.lock;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.palantir.atlasdb.transaction.api.TransactionLockTimeoutException;
import com.palantir.lock.v2.LockRequest;
import com.palantir.lock.v2.LockResponse;
import com.palantir.lock.v2.LockToken;
import com.palantir.lock.v2.TimelockService;
import java.util.Optional;
import java.util.Set;
import java.util.function.BooleanSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SimpleTransactionLockReleaser implements TransactionLockReleaser {
    private static final Logger log = LoggerFactory.getLogger(SimpleTransactionLockReleaser.class);

    private final TimelockService timelockService;
    private final BooleanSupplier conservative;

    private Optional<LockToken> immutableTimestampToken;
    private Optional<LockToken> transactionRowLockToken;

    private SimpleTransactionLockReleaser(TimelockService timelockService, BooleanSupplier conservative) {
        this.timelockService = timelockService;
        this.conservative = conservative;
    }

    public static TransactionLockReleaser conservative(TimelockService timelockService) {
        return new SimpleTransactionLockReleaser(timelockService, () -> true);
    }

    @Override
    public Optional<LockToken> acquireTransactionLock(LockRequest lockRequest) {
        LockResponse response = timelockService.lock(lockRequest);
        transactionRowLockToken = response.getTokenOrEmpty();
        return transactionRowLockToken;
    }

    @Override
    public void checkAndMaybeReleaseLocksBeforeCommit() {
        Set<LockToken> knownTokens = getKnownLockTokens();
        if (knownTokens.isEmpty()) {
            return;
        }

        Set<LockToken> heldTokens = conservative.getAsBoolean()
                ? timelockService.refreshLockLeases(knownTokens)
                : timelockService.unlock(knownTokens);
        if (!heldTokens.equals(knownTokens)) {
            // we lost something, explode
            final String baseMsg = "Required locks are no longer valid. ";
            String expiredLocksErrorString = getExpiredLocksErrorString(Sets.difference(knownTokens, heldTokens));
            TransactionLockTimeoutException ex = new TransactionLockTimeoutException(baseMsg + expiredLocksErrorString);
            log.error(baseMsg + "{}", expiredLocksErrorString, ex);
            throw ex;
        }
    }

    @Override
    public void releaseLocks() {
        unlockAndGetSuccessfullyUnlockedTokens();
    }

    private Set<LockToken> getKnownLockTokens() {
        Set<LockToken> knownTokens = Sets.newHashSet();
        immutableTimestampToken.ifPresent(knownTokens::add);
        transactionRowLockToken.ifPresent(knownTokens::add);
        return knownTokens;
    }

    private Set<LockToken> unlockAndGetSuccessfullyUnlockedTokens() {
        Set<LockToken> knownTokens = getKnownLockTokens();
        if (knownTokens.isEmpty()) {
            return ImmutableSet.of();
        }
        return timelockService.unlock(knownTokens);
    }

    private String getExpiredLocksErrorString(Set<LockToken> expiredLocks) {
        return "The following immutable timestamp lock was required: " + immutableTimestampToken
                + "; the following commit locks were required: " + transactionRowLockToken
                + "; the following locks are no longer valid: " + expiredLocks;
    }
}
