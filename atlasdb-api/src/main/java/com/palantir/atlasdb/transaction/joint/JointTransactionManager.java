/*
 * (c) Copyright 2021 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.transaction.joint;

import com.palantir.atlasdb.transaction.api.TransactionFailedRetriableException;

public interface JointTransactionManager {
    /**
     * Runs the given {@link JointTransactionTask}.
     *
     * TODO (jkong): relax this?
     * This task must be an autocommit task - that is, users are not allowed to manually commit or abort the
     * constituent transactions that users will receive in their map.
     *
     * This task may be run multiple times, though it is guaranteed that the task operates atomically (i.e. all or
     * none of its writes will be persisted to the database). It is worth noting that even in the event of failure,
     * it is possible that the transaction was actually committed.
     */
    <T, E extends Exception> T runTaskThrowOnConflict(JointTransactionTask<T, E> task)
            throws E, TransactionFailedRetriableException;
}
