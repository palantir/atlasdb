/*
 * (c) Copyright 2019 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.keyvalue.cassandra;

import com.palantir.atlasdb.table.description.TableMetadata;

/**
 * Utilities for translating between AtlasDB {@link TableMetadata}-level
 * abstractions and corresponding Cassandra table options.
 */
final class CassandraTableOptions {
    private CassandraTableOptions() {
        // Utility class
    }

    static double bloomFilterFpChance(TableMetadata tableMetadata) {
        if (tableMetadata.hasDenselyAccessedWideRows()) {
            return CassandraConstants.DENSELY_ACCESSED_WIDE_ROWS_BLOOM_FILTER_FP_CHANCE;
        }
        if (tableMetadata.isAppendHeavyAndReadLight()) {
            return tableMetadata.hasNegativeLookups()
                    ? CassandraConstants.NEGATIVE_LOOKUPS_SIZE_TIERED_BLOOM_FILTER_FP_CHANCE
                    : CassandraConstants.DEFAULT_SIZE_TIERED_COMPACTION_BLOOM_FILTER_FP_CHANCE;
        }
        return tableMetadata.hasNegativeLookups()
                ? CassandraConstants.NEGATIVE_LOOKUPS_BLOOM_FILTER_FP_CHANCE
                : CassandraConstants.DEFAULT_LEVELED_COMPACTION_BLOOM_FILTER_FP_CHANCE;
    }

    static int minIndexInterval(TableMetadata tableMetadata) {
        return tableMetadata.hasDenselyAccessedWideRows()
                ? CassandraConstants.DENSELY_ACCESSED_WIDE_ROWS_INDEX_INTERVAL
                : CassandraConstants.DEFAULT_MIN_INDEX_INTERVAL;
    }

    static int maxIndexInterval(TableMetadata tableMetadata) {
        return tableMetadata.hasDenselyAccessedWideRows()
                ? CassandraConstants.DENSELY_ACCESSED_WIDE_ROWS_INDEX_INTERVAL
                : CassandraConstants.DEFAULT_MAX_INDEX_INTERVAL;
    }
}
