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

package com.palantir.atlasdb.cache;

import com.palantir.atlasdb.metrics.Timed;
import javax.annotation.Nullable;

public interface TimestampCache {
    /**
     * Clear all values from the cache.
     */
    @Timed
    void clear();
    /**
     * Be very careful to only insert timestamps here that are already present in the backing store,
     * effectively using the timestamp table as existing concurrency control for who wins a commit.
     *
     * @param startTimestamp transaction start timestamp
     * @param commitTimestamp transaction commit timestamp
     */
    @Timed
    void putAlreadyCommittedTransaction(Long startTimestamp, Long commitTimestamp);
    /**
     * Returns null if not present.
     *
     * @param startTimestamp transaction start timestamp
     * @return commit timestamp for the specified transaction start timestamp if present in cache, otherwise null
     */
    @Nullable
    @Timed
    Long getCommitTimestampIfPresent(Long startTimestamp);
}
