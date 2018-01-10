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

import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.transaction.api.LockAcquisitionException;
import com.palantir.lock.HeldLocksToken;
import com.palantir.lock.LockClient;
import com.palantir.lock.LockRequest;
import com.palantir.lock.LockService;

public class AdvisoryLockConditionSuppliers {

    private static final Logger log = LoggerFactory.getLogger(AdvisoryLockConditionSuppliers.class);

    private static final int NUM_RETRIES = 10;

    public static Supplier<AdvisoryLocksCondition> get(LockService lockService, Iterable<HeldLocksToken> lockTokens,
            Supplier<LockRequest> lockSupplier) {
        return () -> {
            Set<HeldLocksToken> externalLocks = ImmutableSet.copyOf(lockTokens);
            ExternalLocksCondition externalCondition = externalLocks.isEmpty()
                    ? null
                    : new ExternalLocksCondition(lockService, externalLocks);

            LockRequest lockRequest = lockSupplier.get();
            if (lockRequest != null) {
                Validate.isTrue(lockRequest.getVersionId() == null, "Using a version id is not allowed");
                HeldLocksToken newToken = acquireLock(lockService, lockRequest);
                TransactionLocksCondition transactionCondition = new TransactionLocksCondition(lockService, newToken);

                return externalCondition == null
                        ? transactionCondition
                        : new CombinedLocksCondition(externalCondition, transactionCondition);
            }

            return externalCondition == null
                    ? NO_LOCKS_CONDITION
                    : externalCondition;
        };
    }

    private static HeldLocksToken acquireLock(LockService lockService, LockRequest lockRequest) {
        int failureCount = 0;
        while (true) {
            HeldLocksToken response;
            try {
                response = lockService.lockAndGetHeldLocks(LockClient.ANONYMOUS.getClientId(), lockRequest);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
            if (response == null) {
                RuntimeException ex = new LockAcquisitionException(
                        "Failed to lock using the provided lock request: " + lockRequest);
                log.warn("Count not lock successfully", ex);
                ++failureCount;
                if (failureCount >= NUM_RETRIES) {
                    log.warn("Failing after {} tries", failureCount, ex);
                    throw ex;
                }
                AbstractTransactionManager.sleepForBackoff(failureCount);
                continue;
            } else {
                return response;
            }
        }
    }

    private static final AdvisoryLocksCondition NO_LOCKS_CONDITION = new AdvisoryLocksCondition() {
        @Override
        public Iterable<HeldLocksToken> getLocks() {
            return ImmutableSet.of();
        }

        @Override
        public void throwIfConditionInvalid(long timestamp) {}

        @Override
        public void cleanup() {}
    };
}
