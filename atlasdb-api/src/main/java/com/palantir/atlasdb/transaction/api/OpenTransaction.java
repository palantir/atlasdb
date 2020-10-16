/*
 * (c) Copyright 2020 Palantir Technologies Inc. All rights reserved.
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

import com.palantir.atlasdb.metrics.Timed;

public interface OpenTransaction extends Transaction {

    /**
     * Runs a provided task, commits the transaction, and performs cleanup. If no further work needs to be done with the
     * transaction, a no-op task can be passed in.
     *
     * @return value returned by the task
     */
    @Timed
    <T, E extends Exception> T finish(TransactionTask<T, E> task) throws E, TransactionFailedRetriableException;
}
