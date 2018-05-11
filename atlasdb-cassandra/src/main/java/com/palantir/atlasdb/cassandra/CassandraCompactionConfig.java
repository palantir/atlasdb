/*
 * Copyright 2018 Palantir Technologies, Inc. All rights reserved.
 *
 * Licensed under the BSD-3 License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://opensource.org/licenses/BSD-3-Clause
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.palantir.atlasdb.cassandra;

import java.util.Optional;

import org.immutables.value.Value;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.base.Preconditions;

/**
 * Configuration for how AtlasDB tables undergo compaction in Cassandra.
 */
@JsonSerialize(as = ImmutableCassandraCompactionConfig.class)
@JsonDeserialize(as = ImmutableCassandraCompactionConfig.class)
@Value.Immutable
public abstract class CassandraCompactionConfig {
    public static CassandraCompactionConfig defaultConfig() {
        return ImmutableCassandraCompactionConfig.builder().build();
    }

    /**
     * Ratio of droppable tombstones to (Cassandra) columns contained in a single SSTable, before Cassandra's
     * background compactor considers compacting the SSTable (by itself).
     *
     * Cassandra's default is 0.2. However, for stream store value tables specifically there is an argument that this
     * parameter could reasonably be decreased, for several reasons:
     *
     * 1. These tables use SizeTieredCompactionStrategy, which only compacts SSTables when we have (by default)
     *    4 or more of about the same size - hence compaction may not happen frequently, especially for stale values
     *    in the largest tier of SSTables.
     * 2. When a block in the stream store value table is swept, we write a deletion sentinel and an Atlas tombstone.
     *    Thus values are written three times but only tombstoned once, and stale values still occupy 2 columns.
     *    Furthermore, tombstoned values (not unreasonable to assume 1 MB on average) are substantially larger in size
     *    than 'live' values, thus the ratio may significantly underestimate the space that may be recovered.
     *
     * If this value is changed, then the next time an Atlas client is started up it will change the column-family
     * metadata in Cassandra for all stream store value tables to reflect the new tombstone threshold.
     *
     * Note that reducing this value may cause Cassandra to internally compact stream store SSTables more aggressively,
     * thus taking resources away from other compactions that may be going on.
     */
    public abstract Optional<Double> streamStoreValueTableTombstoneThreshold();

    @Value.Check
    public void check() {
        streamStoreValueTableTombstoneThreshold().ifPresent(threshold ->
                Preconditions.checkState(threshold >= 0, "Cannot have negative tombstone threshold."));
    }
}
