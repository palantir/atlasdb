/**
 * Copyright 2016 Palantir Technologies
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
package com.palantir.atlasdb.cache;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

/**
 * This class just here for readability and not directly leaking / tying us down to a Guava class in our API
 */
public class TimestampCache {
    final Cache<Long, Long> timestampCache;

    public TimestampCache() {
        timestampCache = CacheBuilder.newBuilder()
                .maximumSize(1_000_000) // up to ~72MB with java Long object bloat
                .build();
    }

    /**
     * Returns null if not present
     */
    public Long getCommitTimestampIfPresent(Long startTimestamp) {
        return timestampCache.getIfPresent(startTimestamp);
    }


    /**
     * Be very careful to only insert timestamps here that are already present in the backing store,
     * effectively using the timestamp table as existing concurrency control for who wins a commit
     */
    public void putAlreadyCommittedTransaction(Long startTimestamp, Long commitTimestamp) {
        timestampCache.put(startTimestamp, commitTimestamp);
    }
}
