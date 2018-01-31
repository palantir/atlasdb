/*
 * Copyright 2018 Palantir Technologies, Inc. All rights reserved.
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

import com.google.common.collect.ForwardingObject;
import com.palantir.atlasdb.transaction.api.TransactionTaskCondition;
import com.palantir.atlasdb.transaction.api.ConditionAwareTransactionTask;
import com.palantir.atlasdb.transaction.api.LockAwareTransactionTask;
import com.palantir.atlasdb.transaction.api.PreCommitCondition;
import com.palantir.atlasdb.transaction.api.Transaction;
import com.palantir.atlasdb.transaction.api.TransactionTask;
import com.palantir.lock.HeldLocksToken;

public class RefreshableTransactionTaskCondition extends ForwardingObject implements TransactionTaskCondition {
    private TransactionTaskCondition condition;

    public RefreshableTransactionTaskCondition(TransactionTaskCondition condition) {
        refresh(condition);
    }

    @Override
    protected TransactionTaskCondition delegate() {
        return condition;
    }

    public void refresh(TransactionTaskCondition condition) {
        this.condition = condition;
    }

    @Override
    public <T, E extends Exception> boolean test(Transaction transaction, TransactionTask<T, E> task) {
        return delegate().test(transaction, task);
    }

    @Override
    public <T, E extends Exception> boolean test(Transaction transaction, Iterable<HeldLocksToken> locks,
            LockAwareTransactionTask<T, E> task) {
        return delegate().test(transaction, locks, task);
    }

    @Override
    public <T, C extends PreCommitCondition, E extends Exception> boolean test(Transaction transaction,
            C preCommitCondition, ConditionAwareTransactionTask<T, C, E> task) {
        return delegate().test(transaction, preCommitCondition, task);
    }
}
