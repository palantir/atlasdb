/*
 * (c) Copyright 2021 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.keyvalue.api.cache;

import org.immutables.value.Value;

/**
 * Represents either:
 *  1. A value that cannot be currently cached due to a lock being taken out. This has an empty cache value (which
 *     should never actually need to be read), and a status of LOCKED.
 *  2. A value that is cached because the last seen event for it was an unlock event (or there was never a lock event in
 *     the first place). This has a status of UNLOCKED and a value which may be present or empty.
 */
@Value.Immutable
public interface CacheEntry {
    Status status();

    CacheValue value();

    static CacheEntry locked() {
        return ImmutableCacheEntry.builder()
                .status(Status.LOCKED)
                .value(CacheValue.empty())
                .build();
    }

    static CacheEntry unlocked(CacheValue value) {
        return ImmutableCacheEntry.builder()
                .status(Status.UNLOCKED)
                .value(value)
                .build();
    }

    default boolean isUnlocked() {
        return status().isUnlocked();
    }

    enum Status {
        LOCKED,
        UNLOCKED;

        boolean isUnlocked() {
            return this == Status.UNLOCKED;
        }
    }
}
