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
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.palantir.atlasdb.transaction.api.ConditionAwareTransactionTask;
import com.palantir.atlasdb.transaction.api.PreCommitCondition;
import com.palantir.atlasdb.transaction.api.Transaction;
import com.palantir.atlasdb.transaction.api.TransactionFailedNonRetriableException;

public class InvocationReplayer implements Consumer<List<Consumer<Transaction>>> {
    private final static Logger log = LoggerFactory.getLogger(InvocationReplayer.class);

    private final ExecutorService executor;
    private final SerializableTransactionManager manager;
    private final int repetitions;

    public InvocationReplayer(ExecutorService executor, SerializableTransactionManager manager, int repetitions) {
        this.executor = executor;
        this.manager = manager;
        this.repetitions = repetitions;
    }

    @Override
    public void accept(List<Consumer<Transaction>> invocations) {
        accept(invocations, repetitions);
    }

    protected void accept(List<Consumer<Transaction>> invocations, int repetitions) {
        if (repetitions > 0) {
            log.warn("submitting task");
            executor.submit(() -> manager.runTaskWithConditionThrowOnConflict(new PreCommitCondition() {
                @Override
                public void throwIfConditionInvalid(long timestamp) {
                    log.warn("failing task");
                    throw new TransactionFailedNonRetriableException("failing to commit replayed transaction");
                }

                @Override
                public void cleanup() {}
            }, createTransactionTask(invocations, repetitions)));
        }
    }

    private ConditionAwareTransactionTask<Void, PreCommitCondition, Exception> createTransactionTask(
            List<Consumer<Transaction>> invocations, int repetitions) {
        return (transaction, condition) -> {
            try {
                log.warn("running another replay transaction");
                invocations.forEach(invocation -> invocation.accept(transaction));
                return null;
            } finally {
                accept(invocations, repetitions - 1);
            }
        };
    }
}
