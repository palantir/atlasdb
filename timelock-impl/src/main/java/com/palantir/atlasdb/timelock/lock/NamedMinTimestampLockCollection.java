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
    private static final String IMMUTABLE_TIMESTAMP_NAME = "ImmutableTimestamp";

    // Interactions with the immutable timestamp collection is separated from interactions
    // with any other named timestamp. As such, "ImmutableTimestamp" should never be added
    // to the allowed timestamp names.
    private static final Set<String> ALLOWED_TIMESTAMP_NAMES = Set.of("CommitImmutableTimestamp");

    private final LoadingCache<String, NamedMinTimestampTracker> namedMinTimestampTrackers =
            Caffeine.newBuilder().build(NamedMinTimestampTracker::new);

    AsyncLock getImmutableTimestampLock(long timestamp) {
        return getNamedMinTimestampLockInternal(IMMUTABLE_TIMESTAMP_NAME, timestamp);
    }

    Optional<Long> getImmutableTimestamp() {
        return getNamedMinTimestampInternal(IMMUTABLE_TIMESTAMP_NAME);
    }

    AsyncLock getNamedMinTimestampLock(String timestampName, long timestamp) {
        validateTimestampName(timestampName);
        return getNamedMinTimestampLockInternal(timestampName, timestamp);
    }

    Optional<Long> getNamedMinTimestamp(String timestampName) {
        validateTimestampName(timestampName);
        return getNamedMinTimestampInternal(timestampName);
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

    private static void validateTimestampName(String name) {
        Preconditions.checkArgument(
                ALLOWED_TIMESTAMP_NAMES.contains(name), "Unknown named timestamp", SafeArg.of("name", name));
    }
}
