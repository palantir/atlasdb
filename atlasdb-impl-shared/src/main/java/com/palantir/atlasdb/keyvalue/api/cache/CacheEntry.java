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

import java.util.Optional;
import org.immutables.value.Value;

@Value.Immutable
public interface CacheEntry {
    Status status();

    Optional<CacheValue> value();

    static CacheEntry locked() {
        return ImmutableCacheEntry.builder().status(Status.LOCKED).build();
    }

    static CacheEntry unlocked(CacheValue value) {
        return ImmutableCacheEntry.builder()
                .status(Status.UNLOCKED)
                .value(value)
                .build();
    }

    enum Status {
        LOCKED,
        UNLOCKED;

        public boolean isUnlocked() {
            return this == Status.UNLOCKED;
        }
    }
}
