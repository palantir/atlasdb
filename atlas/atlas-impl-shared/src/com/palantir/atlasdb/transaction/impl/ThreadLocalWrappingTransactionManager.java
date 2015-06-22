// Copyright 2015 Palantir Technologies
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.palantir.atlasdb.transaction.impl;

import com.palantir.atlasdb.transaction.api.LockAwareTransactionManager;
import com.palantir.atlasdb.transaction.api.Transaction;
import com.palantir.atlasdb.transaction.api.TransactionWrapper;
import com.palantir.common.concurrent.ExecutorInheritableThreadLocal;

public class ThreadLocalWrappingTransactionManager extends WrappingTransactionManager {
    private final ExecutorInheritableThreadLocal<TransactionWrapper> wrapper;

    public ThreadLocalWrappingTransactionManager(LockAwareTransactionManager delegate,
                                                 ExecutorInheritableThreadLocal<TransactionWrapper> wrapper) {
        super(delegate);
        this.wrapper = wrapper;
    }

    @Override
    protected Transaction wrap(Transaction t) {
        TransactionWrapper w = wrapper.get();
        if (w == null) {
            return t;
        }
        return w.wrap(t);
    }
}
