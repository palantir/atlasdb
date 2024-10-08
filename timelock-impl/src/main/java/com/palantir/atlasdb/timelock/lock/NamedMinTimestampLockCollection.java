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
import com.palantir.logsafe.Preconditions;
import com.palantir.logsafe.SafeArg;
import java.util.Optional;
import java.util.Set;

final class NamedMinTimestampLockCollection {
    private static final Set<String> ALLOWED_MIN_NAMED_TIMESTAMPS = Set.of("commitImmutable");
    private static final String IMMUTABLE_TIMESTAMP_NAME = "immutable";

    private final LoadingCache<String, NamedMinTimestampTracker> namedMinTimestampTrackers =
            Caffeine.newBuilder().build(NamedMinTimestampTracker::new);

    OrderedLocks getImmutableTimestampLock(long timestamp) {
        return getNamedMinTimestampLockInternal(IMMUTABLE_TIMESTAMP_NAME, timestamp);
    }

    Optional<Long> getImmutableTimestamp() {
        return getNamedMinTimestampInternal(IMMUTABLE_TIMESTAMP_NAME);
    }

    OrderedLocks getNamedMinTimestampLock(String name, long timestamp) {
        validateTimestampName(name);
        return getNamedMinTimestampLockInternal(name, timestamp);
    }

    Optional<Long> getNamedMinTimestamp(String name) {
        validateTimestampName(name);
        return getNamedMinTimestampInternal(name);
    }

    private OrderedLocks getNamedMinTimestampLockInternal(String name, long timestamp) {
        validateTimestampName(name);
        return OrderedLocks.fromSingleLock(NamedMinTimestampLock.create(getNamedMinTimestampTracker(name), timestamp));
    }

    private Optional<Long> getNamedMinTimestampInternal(String name) {
        validateTimestampName(name);
        return getNamedMinTimestampTracker(name).getMinimumTimestamp();
    }

    private NamedMinTimestampTracker getNamedMinTimestampTracker(String name) {
        return namedMinTimestampTrackers.get(name);
    }

    private static void validateTimestampName(String name) {
        Preconditions.checkArgument(
                ALLOWED_MIN_NAMED_TIMESTAMPS.contains(name), "Unknown named timestamp", SafeArg.of("name", name));
    }
}
