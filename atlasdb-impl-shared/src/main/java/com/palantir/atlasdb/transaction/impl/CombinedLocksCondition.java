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

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.palantir.lock.HeldLocksToken;
import com.palantir.lock.LockService;

public class CombinedLocksCondition implements AdvisoryLocksCondition {
    private final ExternalLocksCondition externalLocksCondition;
    private final TransactionLocksCondition transactionLocksCondition;

    public CombinedLocksCondition(
            LockService lockService, Iterable<HeldLocksToken> externalLockTokens, HeldLocksToken transactionLockToken) {
        this.externalLocksCondition = new ExternalLocksCondition(lockService, ImmutableSet.copyOf(externalLockTokens));
        this.transactionLocksCondition = new TransactionLocksCondition(lockService, transactionLockToken);
    }

    public CombinedLocksCondition(
            ExternalLocksCondition externalLocksCondition, TransactionLocksCondition transactionLocksCondition) {
        this.externalLocksCondition = externalLocksCondition;
        this.transactionLocksCondition = transactionLocksCondition;
    }

    @Override
    public void throwIfConditionInvalid(long timestamp) {
        externalLocksCondition.throwIfConditionInvalid(timestamp);
        transactionLocksCondition.throwIfConditionInvalid(timestamp);
    }

    @Override
    public void cleanup() {
        // Both of these conditions don't throw internally
        externalLocksCondition.cleanup();
        transactionLocksCondition.cleanup();
    }

    @Override
    public Iterable<HeldLocksToken> getLocks() {
        return Iterables.concat(externalLocksCondition.getLocks(), transactionLocksCondition.getLocks());
    }
}
