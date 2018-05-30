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

import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.palantir.atlasdb.transaction.api.CapturedTransaction;
import com.palantir.atlasdb.transaction.api.CapturingTransaction;
import com.palantir.atlasdb.transaction.api.ConditionAwareTransactionTask;
import com.palantir.atlasdb.transaction.api.LockAwareTransactionTask;
import com.palantir.atlasdb.transaction.api.PreCommitCondition;
import com.palantir.atlasdb.transaction.api.Transaction;
import com.palantir.atlasdb.transaction.api.TransactionTask;
import com.palantir.atlasdb.transaction.api.TransactionTaskCondition;
import com.palantir.atlasdb.transaction.api.TransactionTaskWrapper;

public class SuccessfulTaskInvocationCapture implements TransactionTaskWrapper {
    private static final Logger log = LoggerFactory.getLogger(SuccessfulTaskInvocationCapture.class);

    private final TransactionTaskCondition transactionTaskCondition;
    private final Consumer<CapturedTransaction> consumer;

    public SuccessfulTaskInvocationCapture(TransactionTaskCondition transactionTaskCondition,
            Consumer<CapturedTransaction> consumer) {
        this.transactionTaskCondition = transactionTaskCondition;
        this.consumer = consumer;
    }

    @Override
    public <E extends Exception, T> TransactionTask<T, E> wrap(TransactionTask<T, E> task) {
        return transaction -> wrapInternal(transaction, transactionTaskCondition.test(transaction, task), task);
    }

    @Override
    public <T, E extends Exception> LockAwareTransactionTask<T, E> wrap(LockAwareTransactionTask<T, E> task) {
        return (transaction, heldLocks) ->
                wrapInternal(transaction, transactionTaskCondition.test(transaction, heldLocks, task),
                        wrapped -> task.execute(wrapped, heldLocks));
    }

    @Override
    public <T, C extends PreCommitCondition, E extends Exception> ConditionAwareTransactionTask<T, C, E> wrap(
            ConditionAwareTransactionTask<T, C, E> task) {
        return (transaction, condition) ->
                wrapInternal(transaction, transactionTaskCondition.test(transaction, condition, task),
                        wrapped -> task.execute(wrapped, condition));
    }

    private <T, E extends Exception> T wrapInternal(Transaction transaction, boolean shouldCapture,
            TransactionTask<T, E> task) throws E {
        if (!shouldCapture) {
            return task.execute(transaction);
        }

        long timestamp = transaction.getTimestamp();

        log.debug("capturing transaction with timestamp {}.", timestamp);
        CapturingTransaction capturing = new ForwardingCapturingTransaction(transaction);
        T result;
        try {
            result = task.execute(capturing);
        } catch (Exception exception) {
            log.debug("exception thrown whilst executing task");
            throw exception;
        }

        log.debug("transaction with timestamp {} successfully executed, passing to consumer.", timestamp);
        consumer.accept(capturing.captured());
        return result;
    }
}
