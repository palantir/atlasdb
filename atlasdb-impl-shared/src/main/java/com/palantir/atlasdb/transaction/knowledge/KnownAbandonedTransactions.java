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

import java.util.Set;

/**
 * Represents a set of start timestamps that belong to transactions which will never commit.
 */
public interface KnownAbandonedTransactions {

    /**
     * For a concluded transaction with the given start timestamp, returns true if it is known that the transaction
     * is abandoned. Calling this method for a transaction that has not been concluded is undefined.
     *
     * @param startTimestamp start timestamp associated with the value we are checking for
     * @return whether the transaction that started at the provided timestamp can never be committed.
     */
    boolean isKnownAbandoned(long startTimestamp);

    /**
     * Registers the fact that any transactions that had startTimestamp in set of timestamps provided are marked as
     * abandoned.
     *
     * @param abandonedTimestamps set of timestamps for which all transactions have been abandoned.
     */
    void addAbandonedTimestamps(Set<Long> abandonedTimestamps);
}
