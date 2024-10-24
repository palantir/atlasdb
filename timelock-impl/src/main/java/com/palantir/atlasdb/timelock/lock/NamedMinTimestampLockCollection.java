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
import com.palantir.atlasdb.common.api.timelock.TimestampLeaseName;
import java.util.Optional;

final class NamedMinTimestampLockCollection {
    private final LoadingCache<String, NamedMinTimestampTracker> namedMinTimestampTrackers =
            Caffeine.newBuilder().build(NamedMinTimestampTrackerImpl::new);

    AsyncLock getImmutableTimestampLock(long timestamp) {
        return getNamedTimestampLockInternal(TimestampLeaseName.RESERVED_NAME_FOR_IMMUTABLE_TIMESTAMP, timestamp);
    }

    Optional<Long> getImmutableTimestamp() {
        return getNamedMinTimestampInternal(TimestampLeaseName.RESERVED_NAME_FOR_IMMUTABLE_TIMESTAMP);
    }

    AsyncLock getNamedTimestampLock(TimestampLeaseName timestampName, long timestamp) {
        return getNamedTimestampLockInternal(timestampName.name(), timestamp);
    }

    Optional<Long> getNamedMinTimestamp(TimestampLeaseName timestampName) {
        return getNamedMinTimestampInternal(timestampName.name());
    }

    private AsyncLock getNamedTimestampLockInternal(String name, long timestamp) {
        return NamedMinTimestampLock.create(getNamedMinTimestampTracker(name), timestamp);
    }

    private Optional<Long> getNamedMinTimestampInternal(String name) {
        return getNamedMinTimestampTracker(name).getMinimumTimestamp();
    }

    private NamedMinTimestampTracker getNamedMinTimestampTracker(String name) {
        return namedMinTimestampTrackers.get(name);
    }
}
