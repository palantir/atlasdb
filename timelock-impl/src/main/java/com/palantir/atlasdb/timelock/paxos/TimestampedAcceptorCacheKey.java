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
    @Value.Parameter
    AcceptorCacheKey cacheKey();

    // this exists to give ordering to cache keys especially when they're coming back concurrently.
    @Value.Parameter
    long timestamp();

    static TimestampedAcceptorCacheKey of(AcceptorCacheDigest digest) {
        return of(digest.newCacheKey(), digest.cacheTimestamp());
    }

    static TimestampedAcceptorCacheKey of(AcceptorCacheKey cacheKey, long timestamp) {
        return ImmutableTimestampedAcceptorCacheKey.of(cacheKey, timestamp);
    }
}
