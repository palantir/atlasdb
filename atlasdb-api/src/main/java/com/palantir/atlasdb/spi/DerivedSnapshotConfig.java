/*
 * (c) Copyright 2022 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.spi;

import java.util.Optional;
import org.immutables.value.Value;

@Value.Immutable
public abstract class DerivedSnapshotConfig {
    /**
     * The size of the thread pool used for concurrently running range requests.
     */
    public abstract int concurrentGetRangesThreadPoolSize();

    abstract Optional<Integer> defaultGetRangesConcurrencyOverride();
    /**
     * The maximum number of threads from the pool of {@link #concurrentGetRangesThreadPoolSize()} to use
     * for a single getRanges request when the user does not explicitly provide a value.
     */
    @Value.Derived
    public int defaultGetRangesConcurrency() {
        return defaultGetRangesConcurrencyOverride()
                .orElseGet(() -> Math.min(8, concurrentGetRangesThreadPoolSize() / 2));
    }

    public static ImmutableDerivedSnapshotConfig.Builder builder() {
        return ImmutableDerivedSnapshotConfig.builder();
    }
}
