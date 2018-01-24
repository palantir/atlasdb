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

import java.util.List;
import java.util.function.Consumer;
import java.util.function.Supplier;

import com.palantir.atlasdb.transaction.api.PreCommitCondition;
import com.palantir.atlasdb.transaction.api.Transaction;
import com.palantir.atlasdb.transaction.api.TransactionFailedRetriableException;
import com.palantir.atlasdb.transaction.api.TransactionTask;

public class InvocationCapturingTransactionManager extends AutoDelegate_SerializableTransactionManager {
    private final SerializableTransactionManager manager;
    private final Consumer<List<Consumer<Transaction>>> invocationConsumer;
    private final Supplier<Boolean> shouldCaptureFilter;

    public InvocationCapturingTransactionManager(SerializableTransactionManager manager,
            Consumer<List<Consumer<Transaction>>> invocationConsumer,
            Supplier<Boolean> shouldCaptureFilter) {
        this.manager = manager;
        this.invocationConsumer = invocationConsumer;
        this.shouldCaptureFilter = shouldCaptureFilter;
    }

    @Override
    public SerializableTransactionManager delegate() {
        return manager;
    }

    @Override
    public RawTransaction setupRunTaskWithConditionThrowOnConflict(PreCommitCondition condition) {
        RawTransaction raw = super.setupRunTaskWithConditionThrowOnConflict(condition);
        if (shouldCaptureFilter.get()) {
            log.warn("wrapping transaction.");
            return new InvocationCapturingTransaction(raw);
        }
        return raw;
    }

    @Override
    public <T, E extends Exception> T finishRunTaskWithLockThrowOnConflict(RawTransaction tx,
            TransactionTask<T, E> task) throws E, TransactionFailedRetriableException {
        try {
            return delegate().finishRunTaskWithLockThrowOnConflict(tx, task);
        } finally {
            if (tx instanceof InvocationCapturingTransaction) {
                log.warn("passing through captured invocations.");
                invocationConsumer.accept(((InvocationCapturingTransaction) tx).invocations());
            }
        }
    }
}
