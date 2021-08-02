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

package com.palantir.atlasdb.transaction.service;

import com.palantir.atlasdb.keyvalue.api.KeyAlreadyExistsException;
import com.palantir.atlasdb.transaction.api.TransactionCommittedState;
import java.util.Map;
import java.util.Optional;

/**
 * Like {@link com.palantir.atlasdb.transaction.service.TransactionService}, but supports partial states.
 */
public interface CombinedTransactionService extends AutoCloseable {
    /**
     * Gets the state from the database for this transaction.
     * Not committed -> empty.
     */
    Optional<TransactionCommittedState> getImmediateState(long startTimestamp);

    /**
     * Should be at least as good as {@link this#getImmediateState(long)} on each of the timestamps.
     * Not committed -> not present in map.
     */
    Map<Long, TransactionCommittedState> get(Iterable<Long> startTimestamps);

    /**
     * Records that a transaction has committed, rolled back, or is dependent.
     * @throws KeyAlreadyExistsException if this value was already set (possibly by ourselves on failed attempts!)
     */
    void putUnlessExists(long startTimestamp, TransactionCommittedState state) throws KeyAlreadyExistsException;

    void checkAndSet(long startTimestamp, TransactionCommittedState expected, TransactionCommittedState newProposal)
            throws KeyAlreadyExistsException;

    /**
     * Frees up resources associated with the transaction service.
     */
    @Override
    void close();
}
