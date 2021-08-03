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
package com.palantir.atlasdb.transaction.service;

import com.palantir.atlasdb.keyvalue.api.KeyAlreadyExistsException;
import com.palantir.atlasdb.metrics.Timed;
import java.util.Map;
import javax.annotation.CheckForNull;

/**
 * Transaction service is used by the atlas protocol to determine is a given transaction has been
 * committed or aborted. Transaction services may assume that data tables are written at timestamps greater than
 * or equal to AtlasDbConstants.STARTING_TS (1).
 *
 * A given startTimestamp will only ever have one non-null value.  This means that non-null values
 * returned from this service can be aggressively cached.  Caching negative look ups should not be
 * done for performance reasons.  If a null value is returned, that startTimestamp will likely be
 * rolled back and set to TransactionConstants.FAILED_COMMIT_TS (-1).
 *
 * @author carrino
 */
public interface TransactionService extends AutoCloseable, AsyncTransactionService {
    /**
     * Gets the commit timestamp associated with a given start timestamp.
     * Non-null responses may be cached on the client-side. Null responses must not be cached, as they could
     * subsequently be updated.
     *
     * This function may return null, which means that the transaction in question had not been committed, at
     * least at some point between the request being made and it returning.
     *
     * @param startTimestamp start timestamp of the transaction being looked up
     * @return timestamp which the transaction committed at, or null if the transaction had not committed yet
     */
    @CheckForNull
    @Timed
    Long get(long startTimestamp);

    @Timed
    Map<Long, Long> get(Iterable<Long> startTimestamps);

    /**
     * This operation is guaranteed to be atomic and only set the value if it hasn't already been
     * set.
     * @throws KeyAlreadyExistsException If this value was already set, but {@link #get(long)} should
     * be called to check what the value was set to.  This may throw spuriously due to retry.
     * @throws RuntimeException If a runtime exception is thrown, this operation may or may
     * not have ran.
     */
    @Timed
    void putUnlessExists(long startTimestamp, long commitTimestamp) throws KeyAlreadyExistsException;

    /**
     * This operation seeks to commit multiple transactions; implementations may override it if this can be
     * done more efficiently than performing individual {@link TransactionService#putUnlessExists(long, long)}
     * operations.
     *
     * This operation is NOT atomic. On success, it is guaranteed that all start/commit timestamp pairs have been
     * successfully stored. However, on failure, start/commit timestamp pairs may or may not have been stored;
     * users should check the state of the transaction service with {@link TransactionService#get(Iterable)} if
     * they need to know what has happened.
     *
     * @param startTimestampToCommitTimestamp map of start timestamps to corresponding commit timestamps
     * @throws KeyAlreadyExistsException if the value corresponding to some start timestamp in the map already existed.
     * @throws RuntimeException if an error occurred; in this case, the operation may or may not have ran.
     */
    default void putUnlessExistsMultiple(Map<Long, Long> startTimestampToCommitTimestamp) {
        startTimestampToCommitTimestamp.forEach(this::putUnlessExists);
    }

    /**
     * Frees up resources associated with the transaction service.
     */
    @Override
    void close();

    /**
     * TODO (jkong): Bizarre nonsense.
     */
    void putDependentInformation(
            long localStart, long localCommit, String foreignDependentName, long foreignDependentStart)
            throws KeyAlreadyExistsException;

    void confirmDependentInformation(long localStart, long localCommit, String foreignCommitIdentity, long foreignStart)
            throws KeyAlreadyExistsException;
}
