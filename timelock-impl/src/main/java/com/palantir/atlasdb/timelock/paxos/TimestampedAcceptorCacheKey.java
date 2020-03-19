/*
 * (c) Copyright 2020 Palantir Technologies Inc. All rights reserved.
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

import org.immutables.value.Value;

@Value.Immutable
interface TimestampedAcceptorCacheKey {
    /**
     * Represents snapshot of the {@link AcceptorCache}.
     *
     * @see AcceptorCacheKey
     */
    @Value.Parameter
    AcceptorCacheKey cacheKey();

    /**
     * A logical clock that is, numerically, independent of Paxos sequence numbers and AtlasDB timestamps. It exists to
     * give us an ordering to the cache keys when they are processed concurrently.
     * <p>
     * This is incremented together with the current cache state advancing whenever a
     * {@link BatchPaxosAcceptor#prepare prepare} or a {@link BatchPaxosAcceptor#accept accept} occur.
     * <p>
     * This is also reset to zero, together with the current cache state in the presence of leader election.
     */
    @Value.Parameter
    long timestamp();

    static TimestampedAcceptorCacheKey of(AcceptorCacheDigest digest) {
        return of(digest.newCacheKey(), digest.cacheTimestamp());
    }

    static TimestampedAcceptorCacheKey of(AcceptorCacheKey cacheKey, long timestamp) {
        return ImmutableTimestampedAcceptorCacheKey.of(cacheKey, timestamp);
    }
}
