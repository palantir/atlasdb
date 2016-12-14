/**
 * Copyright 2016 Palantir Technologies
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
package com.palantir.atlasdb.transaction.service;

import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.transaction.impl.TransactionConstants;

public abstract class AbstractTransactionService implements TransactionService {
    protected KeyValueService kvs;

    AbstractTransactionService(KeyValueService kvs) {
        this.kvs = kvs;
    }

    // The maximum key-value store timestamp (exclusive) at which data is stored
    // in transaction table.
    // All entries in transaction table are stored with timestamp 0
    protected static final long MAX_TIMESTAMP = 1L;

    protected static Cell getTransactionCell(long startTimestamp) {
        return Cell.create(TransactionConstants.getValueForTimestamp(startTimestamp),
                TransactionConstants.COMMIT_TS_COLUMN);
    }

    /**
     *    only call if you get a triple miss
     *    (local good-before ts cache, local transactions cache, DB _transactions table)
     */
    protected long getCurrentGoodBeforeTimestamp() {

    }
}
