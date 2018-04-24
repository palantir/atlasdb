/*
 * (c) Copyright 2015 Palantir Technologies Inc. All rights reserved.
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
package com.palantir.atlasdb.cleaner;

import com.google.common.collect.Multimap;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.transaction.api.TransactionManager;

public final class NoOpCleaner implements Cleaner {
    public static final NoOpCleaner INSTANCE = new NoOpCleaner();

    @Override
    public boolean isInitialized() {
        return true;
    }

    @Override
    public void queueCellsForScrubbing(Multimap<Cell, TableReference> cellToTableRefs,
                                       long scrubTimestamp) {
        throw new UnsupportedOperationException("This cleaner does not support scrubbing");
    }

    @Override
    public void scrubImmediately(TransactionManager txManager,
                                 Multimap<TableReference, Cell> tableRefToCell,
                                 long scrubTimestamp,
                                 long commitTs) {
        throw new UnsupportedOperationException("This cleaner does not support scrubbing");
    }

    @Override
    public void punch(long timestamp) {
        // Do nothing
    }

    @Override
    public long getTransactionReadTimeoutMillis() {
        return Long.MAX_VALUE;
    }

    @Override
    public void close() {
        // Do nothing
    }

    @Override
    public long getUnreadableTimestamp() {
        return Long.MIN_VALUE;
    }

    @Override
    public void start(TransactionManager txManager) {
        // Do nothing
    }
}
