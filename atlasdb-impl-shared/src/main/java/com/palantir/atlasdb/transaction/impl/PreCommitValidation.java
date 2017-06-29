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

import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.palantir.atlasdb.transaction.api.TransactionLockTimeoutException;
import com.palantir.lock.LockRefreshToken;
import com.palantir.lock.RemoteLockService;
import com.palantir.logsafe.UnsafeArg;

/**
 * A validation to be run immediately before committing a transaction.
 */
public interface PreCommitValidation {

    Logger log = LoggerFactory.getLogger(PreCommitValidation.class);

    /**
     * Checks that any required conditions for committing still hold.
     */
    void check();

    PreCommitValidation NO_OP = () -> { };

    static PreCommitValidation forLockServiceLocks(Iterable<LockRefreshToken> tokens, RemoteLockService lockService) {
        Set<LockRefreshToken> toRefresh = ImmutableSet.copyOf(tokens);
        if (toRefresh.isEmpty()) {
            return NO_OP;
        }

        return () -> {
            Set<LockRefreshToken> refreshed = lockService.refreshLockRefreshTokens(toRefresh);
            Set<LockRefreshToken> notRefreshed = Sets.difference(toRefresh, refreshed).immutableCopy();
            if (!notRefreshed.isEmpty()) {
                log.warn("Lock service locks were no longer valid at commit time",
                        UnsafeArg.of("invalidTokens", notRefreshed));
                throw new TransactionLockTimeoutException(
                        "Lock service locks were no longer valid at commit time: " + notRefreshed);
            }
        };
    };

}
