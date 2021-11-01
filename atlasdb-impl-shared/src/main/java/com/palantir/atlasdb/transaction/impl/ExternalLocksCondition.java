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

import com.google.common.collect.Sets;
import com.palantir.atlasdb.transaction.api.TransactionLockTimeoutNonRetriableException;
import com.palantir.lock.HeldLocksToken;
import com.palantir.lock.LockRefreshToken;
import com.palantir.lock.LockService;
import com.palantir.logsafe.UnsafeArg;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class ExternalLocksCondition implements AdvisoryLocksCondition {

    private static final SafeLogger log = SafeLoggerFactory.get(ExternalLocksCondition.class);

    private final LockService lockService;
    private final Set<HeldLocksToken> lockTokens;

    public ExternalLocksCondition(LockService lockService, Set<HeldLocksToken> lockTokens) {
        this.lockService = lockService;
        this.lockTokens = lockTokens;
    }

    @Override
    public void throwIfConditionInvalid(long _timestamp) {
        if (lockTokens.isEmpty()) {
            return;
        }

        Set<LockRefreshToken> refreshTokens =
                lockTokens.stream().map(HeldLocksToken::getLockRefreshToken).collect(Collectors.toSet());
        Set<LockRefreshToken> refreshedLocks = lockService.refreshLockRefreshTokens(refreshTokens);
        Set<LockRefreshToken> expiredLocks = Sets.difference(refreshTokens, refreshedLocks);
        if (!expiredLocks.isEmpty()) {
            List<HeldLocksToken> expiredHeldLocks = lockTokens.stream()
                    .filter(token -> expiredLocks.contains(token.getLockRefreshToken()))
                    .collect(Collectors.toList());
            log.warn(
                    "External lock service locks were no longer valid", UnsafeArg.of("invalidLocks", expiredHeldLocks));
            throw new TransactionLockTimeoutNonRetriableException(
                    "Provided external lock tokens expired. " + "Retry is not possible. Locks: " + expiredHeldLocks);
        }
    }

    @Override
    public void cleanup() {}

    @Override
    public Iterable<HeldLocksToken> getLocks() {
        return lockTokens;
    }
}
