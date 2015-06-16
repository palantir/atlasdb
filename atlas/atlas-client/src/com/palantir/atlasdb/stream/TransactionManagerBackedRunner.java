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

package com.palantir.atlasdb.stream;

import com.palantir.atlasdb.transaction.api.TransactionManager;
import com.palantir.atlasdb.transaction.api.TransactionTask;

public class TransactionManagerBackedRunner implements TransactionRunner {

    private final TransactionManager txManager;

    public TransactionManagerBackedRunner(TransactionManager txManager) {
        this.txManager = txManager;
    }

    @Override
    public <T, E extends Exception> T run(TransactionTask<T, E> task) throws E {
        return txManager.runTaskThrowOnConflict(task);
    }
}
