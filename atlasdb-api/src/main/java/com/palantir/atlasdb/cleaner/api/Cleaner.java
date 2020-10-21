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
package com.palantir.atlasdb.cleaner.api;

import com.google.common.collect.Multimap;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.transaction.api.TransactionManager;
import java.io.Closeable;

/**
 * A {@link Cleaner} is good for two things: it scrubs and it punches. Scrubbing is an on-demand
 * operation, deleting things you particularly badly want gone, and it lets you configure just how
 * badly you want them gone. Punching is associating atlasdb timestamps with wallclock time.
 *
 * @author jweel
 */
public interface Cleaner extends Closeable {

    /**
     * Used for Cleaners that can be initialized asynchronously (i.e. those extending
     * {@link com.palantir.async.initializer.AsyncInitializer}; other Cleaners can keep the default implementation,
     * and return true (they're trivially fully initialized).
     *
     * @return true if and only if the Cleaner has been fully initialized
     */
    default boolean isInitialized() {
        return true;
    }

    /**
     * @param cellToTableRefs Cells that were touched as part of the hard delete transaction
     * @param scrubTimestamp The start timestamp of the hard delete transaction whose
     *        cells need to be scrubbed; at the time queueCellsForScrubbing is called,
     *        the hard delete transaction will be in the process of committing  @throws
     *        exceptions are simply propagated up if something goes wrong.
     */
    void queueCellsForScrubbing(Multimap<Cell, TableReference> cellToTableRefs, long scrubTimestamp);

    /**
     * @param tableRefToCell Cells to be scrubbed immediately
     * @param scrubTimestamp The start timestamp of the hard delete transaction whose
     *        cells need to be scrubbed; at the time scrubImmediately is called, the
     *        hard delete transaction will have just committed
     * @throws RuntimeException are simply propagated up if something goes wrong.
     */
    void scrubImmediately(
            TransactionManager txManager,
            Multimap<TableReference, Cell> tableRefToCell,
            long scrubTimestamp,
            long commitTimestamp);

    /**
     * Indicate that the given timestamp has just been created. This must be called frequently
     * (preferably on each transaction commit) so that the Cleaner can keep track of the
     * wall-clock/timestamp mapping. If it is never called, semantically nothing goes wrong, but the
     * sweeper won't sweep, since it cannot know what things are old enough to be swept.
     *
     * @param timestamp Timestamp that has just been committed.
     */
    void punch(long timestamp);

    /**
     * @return The timeout for transactions reads in milliseconds.  SnapshotTransaction enforces that
     *         transactions that have been open for longer than this timeout can no longer perform reads;
     *         waiting for the timeout to elapse after cleaning allows us to avoid causing currently
     *         open read transactions to abort
     */
    long getTransactionReadTimeoutMillis();

    /**
     * Returns the timestamp that is before any open start timestamps. This is different from the immutable
     * timestamp, because it takes into account open read-only transactions. There is likely to be NO
     * running transactions open at a timestamp before the unreadable timestamp, however this cannot be guaranteed.
     * <p>
     * When using the unreadable timestamp for cleanup it is important to leave a sentinel value behind at a negative
     * timestamp so any transaction that is open will fail out if reading a value that is cleaned up instead of just
     * getting back no data. This is needed to ensure that all transactions either produce correct values or fail.
     * It is not an option to return incorrect data.
     */
    long getUnreadableTimestamp();

    /**
     * Release resources.
     */
    @Override
    void close();

    /**
     * Starts the background scrubber.
     */
    void start(TransactionManager txManager);
}
