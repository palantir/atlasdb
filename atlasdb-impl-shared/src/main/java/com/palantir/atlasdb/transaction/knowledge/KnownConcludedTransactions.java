/*
 * (c) Copyright 2022 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.transaction.knowledge;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Range;
import java.util.Set;

/**
 * Represents a set of start timestamps that belong to transactions that are known to have concluded.
 * This means that transactions with these start timestamps have either been committed, been aborted, or have not
 * committed and will not be able to ever commit (though it is not known which of these is actually the case).
 */
public interface KnownConcludedTransactions {
    /**
     * Returns whether the transaction that started at the provided timestamp is known to have concluded.
     *
     * @param startTimestamp start timestamp associated with the value we are checking for
     * @param consistency consistency level to use when answering this query
     * @return whether the transaction that started at the provided timestamp is known to have concluded.
     */
    boolean isKnownConcluded(long startTimestamp, Consistency consistency);

    /**
     * Registers the fact that any transactions that had started in the provided ranges have concluded, including
     * writing this to the database. This endpoint is costly, and must not be called with a high level of concurrency.
     *
     * Whether the endpoint is coordination aware or not depends on the underlying implementation.
     *
     * @param knownConcludedInterval range of timestamps in which all transactions must have concluded
     */
    default void addConcludedTimestamps(Range<Long> knownConcludedInterval) {
        addConcludedTimestamps(ImmutableSet.of(knownConcludedInterval));
    }

    void addConcludedTimestamps(Set<Range<Long>> knownConcludedIntervals);

    /**
     * Sets the minimum timestamp required for any range to be concluded.
     *
     * The minimum timestamp is used to ensure we do not conclude ranges that are below this timestamp [-∞, ts).
     * This is necessary for backup/restore, as we must not conclude any ranges post-restore that are before
     * the fast-forward timestamp. This is to prevent concluding transactions which have written to the KVS,
     * but had not committed.
     *
     * @param timestamp minimum timestamp
     */
    void setMinimumConcludableTimestamp(Long timestamp);

    /**
     * Returns the greatest known concluded timestamp for which transaction is known to have concluded. This call
     * relies on local cache. Hence, it is possible for the view to be out of date.
     *
     * @return the greatest known concluded timestamp for which transaction is known to have concluded.
     */
    long lastLocallyKnownConcludedTimestamp();

    enum Consistency {
        /**
         * Only perform a read from a local cache. This is eventually consistent and the set of known committed
         * timestamps for a given namespace only grows, so a 'true' answer to
         * {@link #isKnownConcluded(long, Consistency)} at this level can be trusted, but a 'false' answer might
         * actually be knowably committed if one looks in the database.
         */
        LOCAL_READ,
        /**
         * Perform a remote read against the underlying database if necessary.
         */
        REMOTE_READ;
    }
}
