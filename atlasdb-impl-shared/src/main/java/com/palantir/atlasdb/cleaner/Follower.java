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
package com.palantir.atlasdb.cleaner;

import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.transaction.api.Transaction;
import com.palantir.atlasdb.transaction.api.TransactionManager;
import java.util.Set;

/**
 * A callback that is called as part of scrubbing and sweeping.
 */
public interface Follower {
    /**
     * Run the follower over the cells in a transaction.
     *
     * @param tableRef the table being scrubbed / swept
     * @param cells ALL the actual cells being deleted (not just the ones passed in to scrub()!)
     * @param transactionType regular or aggressive hard delete
     */
    // TODO (ejin): Is passing a TransactionType really the cleanest approach here?
    void run(
            TransactionManager txManager,
            TableReference tableRef,
            Set<Cell> cells,
            Transaction.TransactionType transactionType);
}
