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

/**
 * Cassandra derives the default concurrentGetRangesThreadPoolSize from
 * the pool size and the number of thrift hosts.
 *
 * The pool size is configured in {@link com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfig}, whereas the
 * initial server list is now present in {@link com.palantir.atlasdb.cassandra.CassandraKeyValueServiceRuntimeConfig},
 * and thus the derivation requires information from both configs.
 *
 * This immutable represents the minimal set of properties where the values <i>may</i> be derived from different
 * configs, to avoid library consumers from depending on the idea of a totally merged config (which may exist
 * separately, e.g for the purpose of moving config keys from install to runtime config)
 *
 * Despite being derived from the runtime config, there is no guarantees that the derived values are in turn
 * live-reloaded.
 *
 * See {@link AtlasDbFactory} for information on how this is constructed for the various KVSs.
 *
 */
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
