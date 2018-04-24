/*
 * (c) Copyright 2017 Palantir Technologies Inc. All rights reserved.
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

import java.util.Collections;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.palantir.atlasdb.transaction.api.TransactionLockTimeoutException;
import com.palantir.lock.HeldLocksToken;
import com.palantir.lock.LockService;
import com.palantir.logsafe.UnsafeArg;

public class TransactionLocksCondition implements AdvisoryLocksCondition {

    private static final Logger log = LoggerFactory.getLogger(TransactionLocksCondition.class);

    private final LockService lockService;
    private final HeldLocksToken heldLock;

    public TransactionLocksCondition(LockService lockService, HeldLocksToken heldLock) {
        this.lockService = lockService;
        this.heldLock = heldLock;
    }

    @Override
    public void throwIfConditionInvalid(long timestamp) {
        if (lockService.refreshLockRefreshTokens(Collections.singleton(heldLock.getLockRefreshToken())).isEmpty()) {
            log.warn("Lock service locks were no longer valid",
                    UnsafeArg.of("invalidToken", heldLock.getLockRefreshToken()));
            throw new TransactionLockTimeoutException("Provided transaction lock expired. Token: "
                    + heldLock.getLockRefreshToken());
        }
    }

    @Override
    public void cleanup() {
        PreCommitConditions.runCleanupTask(() -> lockService.unlock(heldLock.getLockRefreshToken()));
    }

    @Override
    public Iterable<HeldLocksToken> getLocks() {
        return Collections.singleton(heldLock);
    }
}
