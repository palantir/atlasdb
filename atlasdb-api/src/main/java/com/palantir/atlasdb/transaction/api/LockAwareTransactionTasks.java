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
package com.palantir.atlasdb.transaction.api;

import com.google.common.collect.ImmutableSet;
import com.palantir.lock.HeldLocksToken;

public final class LockAwareTransactionTasks {

    private LockAwareTransactionTasks() {
        // cannot instantiate
    }

    public static <T, E extends Exception> LockAwareTransactionTask<T, E> asLockAware(
            final TransactionTask<T, E> task) {
        return new LockAwareTransactionTask<T, E>() {
            @Override
            public T execute(Transaction tx, Iterable<HeldLocksToken> _heldLocks) throws E {
                return task.execute(tx);
            }

            @Override
            public String toString() {
                return task.toString();
            }
        };
    }

    public static <T, E extends Exception> TransactionTask<T, E> asLockUnaware(
            final LockAwareTransactionTask<T, E> task) {
        return asLockUnaware(task, ImmutableSet.<HeldLocksToken>of());
    }

    public static <T, E extends Exception> TransactionTask<T, E> asLockUnaware(
            final LockAwareTransactionTask<T, E> task, final Iterable<HeldLocksToken> locks) {
        return tx -> task.execute(tx, locks);
    }
}
