/*
 * (c) Copyright 2024 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.timelock.lock;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import java.util.Optional;

final class NamedMinTimestampLockCollection {
    private static final String IMMUTABLE_TIMESTAMP_NAME = "ImmutableTimestamp";

    private final LoadingCache<String, NamedMinTimestampTracker> namedMinTimestampTrackers =
            Caffeine.newBuilder().build(NamedMinTimestampTracker::new);

    AsyncLock getImmutableTimestampLock(long timestamp) {
        return getNamedMinTimestampLockInternal(IMMUTABLE_TIMESTAMP_NAME, timestamp);
    }

    Optional<Long> getImmutableTimestamp() {
        return getNamedMinTimestampInternal(IMMUTABLE_TIMESTAMP_NAME);
    }

    private AsyncLock getNamedMinTimestampLockInternal(String name, long timestamp) {
        return NamedMinTimestampLock.create(getNamedMinTimestampTracker(name), timestamp);
    }

    private Optional<Long> getNamedMinTimestampInternal(String name) {
        return getNamedMinTimestampTracker(name).getMinimumTimestamp();
    }

    private NamedMinTimestampTracker getNamedMinTimestampTracker(String name) {
        return namedMinTimestampTrackers.get(name);
    }
}
