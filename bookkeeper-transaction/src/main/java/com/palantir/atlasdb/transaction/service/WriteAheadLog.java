/**
 * Copyright 2015 Palantir Technologies
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

import java.util.Iterator;

/**
 * Interface for transaction service storing logs.
 * It can be in states - open, when only writing is allowed and closed, when only reading is allowed.
 */
public interface WriteAheadLog extends Iterable<TransactionLogEntry> {
    /**
     * Appends to the log. The log need to be open.
     */
    void append(long startTs, long commitTs);

    /**
     * Changes the state from open to closed.
     */
    void close();

    boolean isClosed();

    long getId();

    /**
     * Returns an iterator that goes through all entries. The log need to be closed.
     */
    @Override
    Iterator<TransactionLogEntry> iterator();
}
