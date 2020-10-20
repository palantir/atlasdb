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

import com.palantir.atlasdb.transaction.api.Transaction;
import com.palantir.atlasdb.transaction.api.TransactionManager;
import com.palantir.atlasdb.transaction.api.TransactionWrapper;
import com.palantir.common.concurrent.ExecutorInheritableThreadLocal;

public class ThreadLocalWrappingTransactionManager extends WrappingTransactionManager {
    private final ExecutorInheritableThreadLocal<TransactionWrapper> wrapper;

    public ThreadLocalWrappingTransactionManager(
            TransactionManager delegate, ExecutorInheritableThreadLocal<TransactionWrapper> wrapper) {
        super(delegate);
        this.wrapper = wrapper;
    }

    @Override
    protected Transaction wrap(Transaction transaction) {
        TransactionWrapper transactionWrapper = this.wrapper.get();
        if (transactionWrapper == null) {
            return transaction;
        }
        return transactionWrapper.wrap(transaction);
    }

    @Override
    public void close() {
        super.close();
        wrapper.remove();
    }
}
