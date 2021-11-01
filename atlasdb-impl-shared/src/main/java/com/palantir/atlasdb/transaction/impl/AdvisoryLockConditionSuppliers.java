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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.transaction.api.LockAcquisitionException;
import com.palantir.lock.HeldLocksToken;
import com.palantir.lock.LockClient;
import com.palantir.lock.LockRequest;
import com.palantir.lock.LockService;
import com.palantir.logsafe.Preconditions;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import java.util.Set;
import java.util.function.Supplier;

public final class AdvisoryLockConditionSuppliers {

    private static final SafeLogger log = SafeLoggerFactory.get(AdvisoryLockConditionSuppliers.class);

    private static final int NUM_RETRIES = 10;

    private AdvisoryLockConditionSuppliers() {}

    public static Supplier<AdvisoryLocksCondition> get(
            LockService lockService, Iterable<HeldLocksToken> lockTokens, Supplier<LockRequest> lockSupplier) {
        return () -> {
            Set<HeldLocksToken> externalLocks = ImmutableSet.copyOf(lockTokens);
            ExternalLocksCondition externalCondition =
                    externalLocks.isEmpty() ? null : new ExternalLocksCondition(lockService, externalLocks);

            LockRequest lockRequest = lockSupplier.get();
            if (lockRequest != null) {
                Preconditions.checkArgument(lockRequest.getVersionId() == null, "Using a version id is not allowed");
                HeldLocksToken newToken = acquireLock(lockService, lockRequest);
                TransactionLocksCondition transactionCondition = new TransactionLocksCondition(lockService, newToken);

                return externalCondition == null
                        ? transactionCondition
                        : new CombinedLocksCondition(externalCondition, transactionCondition);
            }

            return externalCondition == null ? NO_LOCKS_CONDITION : externalCondition;
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
                RuntimeException ex =
                        new LockAcquisitionException("Failed to lock using the provided lock request: " + lockRequest);
                switch (lockRequest.getBlockingMode()) {
                    case DO_NOT_BLOCK:
                        log.debug("Could not lock successfully", ex);
                        throw ex;
                    case BLOCK_UNTIL_TIMEOUT:
                    case BLOCK_INDEFINITELY:
                    case BLOCK_INDEFINITELY_THEN_RELEASE:
                        log.warn("Could not lock successfully", ex);
                        ++failureCount;
                        if (failureCount >= NUM_RETRIES) {
                            log.warn("Failing after {} tries", SafeArg.of("failureCount", failureCount), ex);
                            throw ex;
                        }
                        break;
                }
            } else {
                return response;
            }
        }
    }

    @VisibleForTesting
    static final AdvisoryLocksCondition NO_LOCKS_CONDITION = new AdvisoryLocksCondition() {
        @Override
        public Iterable<HeldLocksToken> getLocks() {
            return ImmutableSet.of();
        }

        @Override
        public void throwIfConditionInvalid(long _timestamp) {}

        @Override
        public void cleanup() {}
    };
}
