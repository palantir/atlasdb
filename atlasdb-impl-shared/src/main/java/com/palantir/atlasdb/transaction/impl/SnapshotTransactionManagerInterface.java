/*
 * Copyright 2017 Palantir Technologies, Inc. All rights reserved.
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

import com.palantir.atlasdb.cleaner.Cleaner;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.transaction.api.KeyValueServiceStatus;
import com.palantir.atlasdb.transaction.api.LockAwareTransactionManager;
import com.palantir.atlasdb.transaction.api.TransactionFailedRetriableException;
import com.palantir.atlasdb.transaction.api.TransactionTask;
import com.palantir.lock.LockRefreshToken;
import com.palantir.timestamp.TimestampService;

public interface SnapshotTransactionManagerInterface extends LockAwareTransactionManager {
    RawTransaction setupRunTaskWithLocksThrowOnConflict(Iterable<LockRefreshToken> lockTokens);
    <T, E extends Exception> T finishRunTaskWithLockThrowOnConflict(RawTransaction tx,
            TransactionTask<T, E> task)
            throws E, TransactionFailedRetriableException;
    void registerClosingCallback(Runnable closingCallback);
    Cleaner getCleaner();
    KeyValueService getKeyValueService();
    TimestampService getTimestampService();
    KeyValueServiceStatus getKeyValueServiceStatus();
}
