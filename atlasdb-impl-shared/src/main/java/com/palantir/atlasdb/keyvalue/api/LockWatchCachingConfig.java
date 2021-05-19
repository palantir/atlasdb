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

package com.palantir.atlasdb.keyvalue.api;

import org.immutables.value.Value;

@Value.Immutable
public interface LockWatchCachingConfig {
    /**
     * The maximum size, in bytes, for the lock watch value cache. Note that this is in truth an approximate maximum:
     * it is possible for the cache to start evicting slightly before it reaches this size, or slightly after, but
     * should be correct within an order of magnitude.
     */
    @Value.Default
    default long cacheSize() {
        return 20_000_000;
    }

    /**
     * Determines how frequently to validate reads from the cache. This involves a read to the KVS, and thus negates
     * the performance gain from the cache. A value of 1.0 will always read from the remote; a value of 0.0 will
     * never read from the remote when it does not have to. Once there is confidence in the cache's correctness, this
     * should probably be set to 0.01.
     */
    @Value.Default
    default double validationProbability() {
        return 1.0;
    }

    @Value.Default
    default boolean enabled() {
        return false;
    }

    static ImmutableLockWatchCachingConfig.Builder builder() {
        return ImmutableLockWatchCachingConfig.builder();
    }
}
