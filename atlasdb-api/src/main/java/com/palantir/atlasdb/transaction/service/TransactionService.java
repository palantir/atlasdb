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

import java.util.Map;

import javax.annotation.CheckForNull;

import com.palantir.atlasdb.keyvalue.api.KeyAlreadyExistsException;

/**
 * Transaction service is used by the atlas protocol to determine is a given transaction has been
 * committed or aborted. Service behaviour is only defined for timestamps that are at least
 * AtlasDbConstants.STARTING_TS (1); services may throw or otherwise exhibit undefined behaviour for
 * zero or negative timestamps.
 *
 * A given startTimestamp will only ever have one non-null value.  This means that non-null values
 * returned from this service can be aggressively cached.  Caching negative look ups should not be
 * done for performance reasons.  If a null value is returned, that startTimestamp will likely be
 * rolled back and set to TransactionConstants.FAILED_COMMIT_TS (-1).
 *
 * @author carrino
 */
public interface TransactionService {
    @CheckForNull
    /**
     * Gets the commit timestamp associated with a given start timestamp.
     * This may be cached on the client-side, if desired.
     *
     * This function may return null, which means that the transaction in question had not been committed, at
     * least at some point between the request being made and it returning.
     *
     * @param startTimestamp start timestamp of the transaction being looked up
     * @return timestamp which the transaction committed at, or null if the transaction had not committed yet
     */
    Long get(long startTimestamp);

    Map<Long, Long> get(Iterable<Long> startTimestamps);

    /**
     * This operation is guaranteed to be atomic and only set the value if it hasn't already been
     * set.
     * @throws KeyAlreadyExistsException If this value was already set, but {@link #get(long)} should
     * be called to check what the value was set to.  This may throw spuriously due to retry.
     * @throws RuntimeException If a runtime exception is thrown, this operation may or may
     * not have ran.
     */
    void putUnlessExists(long startTimestamp, long commitTimestamp)
            throws KeyAlreadyExistsException;
}
