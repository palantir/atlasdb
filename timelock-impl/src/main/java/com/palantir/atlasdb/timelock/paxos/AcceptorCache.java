/*
 * (c) Copyright 2019 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.timelock.paxos;

import java.util.Optional;
import java.util.Set;

/**
 * An {@link AcceptorCache} tracks the latest sequence numbers for each client it has seen so far.
 * Each {@code prepare} or {@code accept} call should call through to {@link AcceptorCache#updateSequenceNumbers}.
 * <p>
 * To retrieve latest sequence for the clients that the cache has seen so far, you can return everything via
 * {@link AcceptorCache#getAllUpdates}, or by passing in a {@link AcceptorCacheKey} to
 * {@link AcceptorCache#updatesSinceCacheKey} which returns all updates received after the given cache key was issued.
 * <p>
 * Implementations <b>must</b> be thread safe.
 */
public interface AcceptorCache {

    /**
     * Update the cache with the given set of {@code (client, sequence number)} pairs.
     * <p>
     * Implementations <b><em>SHOULD</em></b> ignore any pairs where the given sequence number is lower than what has already
     * been seen by this cache.
     */
    void updateSequenceNumbers(Set<WithSeq<Client>> clientsAndSeqs);

    /**
     * Get all latest sequence numbers for each client this cache has seen so far.
     * <p>
     * A {@link AcceptorCacheDigest digest} is returned which also contains a {@link AcceptorCacheKey cacheKey} that
     * should be used with {@link AcceptorCache#updatesSinceCacheKey}.
     *
     * @return digest containing all latest sequence numbers for clients seen by this cache.
     */
    AcceptorCacheDigest getAllUpdates();

    /**
     * Get all sequence number updates received by the cache after {@code cacheKey} was issued.
     * <p>
     * The behaviour of this cache is as follows:
     * <ul>
     *     <li>
     *         If there have been no updates to the latest sequence numbers since {@code cacheKey} was issued, an
     *         {@link Optional#empty} is returned.
     *     </li>
     *     <li>
     *         If there <em>have</em> been updates since {@code cacheKey} was issued, an {@link Optional} is returned
     *         with a {@link AcceptorCacheDigest} containing the respective updates and also a new
     *         {@link AcceptorCacheKey cache key} to use on the next invocation.
     *     </li>
     *     <li>
     *         If the {@code cacheKey} is invalid either because it was not issued by the cache or it has expired for
     *         example, the implementation must throw a {@link InvalidAcceptorCacheKeyException}.
     *     </li>
     * </ul>
     *
     * @param cacheKey effective point in time where updates are requested from
     * @return a {@link AcceptorCacheDigest digest} containing the updates and a new cache key if any
     * @throws InvalidAcceptorCacheKeyException when {@code cacheKey} is missing or invalid
     */
    Optional<AcceptorCacheDigest> updatesSinceCacheKey(AcceptorCacheKey cacheKey) throws
            InvalidAcceptorCacheKeyException;

}
