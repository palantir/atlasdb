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

package com.palantir.atlasdb.transaction.api;

import com.palantir.lock.HeldLocksToken;

public interface TransactionCondition extends TransactionTaskCondition {
    boolean test(Transaction transaction);

    @Override
    default <T, E extends Exception> boolean test(Transaction transaction, TransactionTask<T, E> task) {
        return test(transaction);
    }
    @Override
    default <T, E extends Exception> boolean test(Transaction transaction,
            Iterable<HeldLocksToken> locks, LockAwareTransactionTask<T, E> task) {
        return test(transaction);
    }
    @Override
    default <T, C extends PreCommitCondition, E extends Exception> boolean test(Transaction transaction,
            C preCommitCondition, ConditionAwareTransactionTask<T, C, E> task) {
        return test(transaction);
    }
}
