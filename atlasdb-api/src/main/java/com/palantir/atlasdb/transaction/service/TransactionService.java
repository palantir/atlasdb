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
package com.palantir.atlasdb.transaction.service;

import java.util.Map;

import javax.annotation.CheckForNull;

import com.palantir.atlasdb.keyvalue.api.KeyAlreadyExistsException;

/**
 * Transaction service is used by the atlas protocol to determine is a given transaction has been
 * committed or aborted.
 *
 * A given startTimestamp will only ever have one non-null value.  This means that anything
 * returned from this service can be aggressively cached.  Caching negative look ups should not be
 * done for performance reasons.  If a null value is returned, that startTimestamp will likely be
 * rolled back and set to TransactionConstants.FAILED_COMMIT_TS (-1).
 *
 * @author carrino
 */
public interface TransactionService {
    @CheckForNull
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
