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

import com.palantir.atlasdb.transaction.api.TransactionConflictException;
import com.palantir.atlasdb.transaction.api.TransactionTask;

/**
 * This interface and its implementing classes WrappedTransactionManager and WrappedTransaction
 * were made specifically for StreamStoreImpl. It allows the StreamStore to run either with multiple transactions
 * (using the WrappedTransactionManager class) for large streams or with a single transaction (using WrappedTransaction)
 * for small streams.
 *
 * @author kfang
 */
public interface TransactionRunner {
    public <T, E extends Exception> T run(TransactionTask<T, E> task) throws E, TransactionConflictException;
}
