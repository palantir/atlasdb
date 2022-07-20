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
 * Represents a set of start timestamps that belong to transactions that are known to have aborted.
 */
public interface KnownAbortedTransactions {

    /**
     * Returns whether the transaction that started at the provided timestamp is known to have aborted.
     * This method should only be called for transactions that are known to have concluded.
     *
     * @param startTimestamp start timestamp associated with the value we are checking for
     * @return whether the transaction that started at the provided timestamp is known to have aborted.
     */
    boolean isKnownAborted(long startTimestamp);

    /**
     * Registers the fact that any transactions that had startTimestamp in set of timestamps provided have been
     * aborted. This endpoint is only meant to be called internally by a background task.
     *
     * @param abortedTimestamps set of timestamps for which all transactions have been aborted.
     */
    void addAbortedTimestamps(Set<Long> abortedTimestamps);
}
