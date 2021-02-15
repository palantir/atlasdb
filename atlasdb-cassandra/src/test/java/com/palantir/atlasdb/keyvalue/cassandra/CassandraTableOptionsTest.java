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

import static org.assertj.core.api.Assertions.assertThat;

import com.palantir.atlasdb.table.description.TableMetadata;
import org.junit.Test;

public class CassandraTableOptionsTest {
    private static final TableMetadata DENSELY_ACCESSED_WIDE_ROWS_METADATA =
            TableMetadata.internal().denselyAccessedWideRows(true).build();
    private static final TableMetadata DENSELY_ACCESSED_WIDE_ROWS_APPEND_HEAVY_READ_LIGHT_METADATA =
            TableMetadata.internal()
                    .denselyAccessedWideRows(true)
                    .appendHeavyAndReadLight(true)
                    .build();
    private static final TableMetadata APPEND_HEAVY_READ_LIGHT_METADATA =
            TableMetadata.internal().appendHeavyAndReadLight(true).build();
    private static final TableMetadata NEGATIVE_LOOKUPS_METADATA =
            TableMetadata.internal().negativeLookups(true).build();
    private static final TableMetadata APPEND_HEAVY_READ_LIGHT_NEGATIVE_LOOKUPS_METADATA = TableMetadata.internal()
            .appendHeavyAndReadLight(true)
            .negativeLookups(true)
            .build();

    @Test
    public void tablesWithDenselyAccessedWideRowsAlwaysHaveLowBloomFilterFpChance() {
        assertThat(CassandraTableOptions.bloomFilterFpChance(DENSELY_ACCESSED_WIDE_ROWS_METADATA))
                .isEqualTo(CassandraConstants.DENSELY_ACCESSED_WIDE_ROWS_BLOOM_FILTER_FP_CHANCE);
        assertThat(CassandraTableOptions.bloomFilterFpChance(
                        DENSELY_ACCESSED_WIDE_ROWS_APPEND_HEAVY_READ_LIGHT_METADATA))
                .isEqualTo(CassandraConstants.DENSELY_ACCESSED_WIDE_ROWS_BLOOM_FILTER_FP_CHANCE);
    }

    @Test
    public void tablesWithAppendHeavyReadLightHaveSizeTieredBasedBloomFilterFpChance() {
        assertThat(CassandraTableOptions.bloomFilterFpChance(APPEND_HEAVY_READ_LIGHT_METADATA))
                .isEqualTo(CassandraConstants.DEFAULT_SIZE_TIERED_COMPACTION_BLOOM_FILTER_FP_CHANCE);
        assertThat(CassandraTableOptions.bloomFilterFpChance(APPEND_HEAVY_READ_LIGHT_NEGATIVE_LOOKUPS_METADATA))
                .isEqualTo(CassandraConstants.NEGATIVE_LOOKUPS_SIZE_TIERED_BLOOM_FILTER_FP_CHANCE);
    }

    @Test
    public void tablesWithNegativeLookupsHaveDifferentBloomFilterFpChance() {
        assertThat(CassandraTableOptions.bloomFilterFpChance(NEGATIVE_LOOKUPS_METADATA))
                .isEqualTo(CassandraConstants.NEGATIVE_LOOKUPS_BLOOM_FILTER_FP_CHANCE);
        assertThat(CassandraTableOptions.bloomFilterFpChance(TableMetadata.allDefault()))
                .isEqualTo(CassandraConstants.DEFAULT_LEVELED_COMPACTION_BLOOM_FILTER_FP_CHANCE);
    }

    @Test
    public void tablesWithDenselyAccessedWideRowsHaveReducedIndexIntervals() {
        assertThat(CassandraTableOptions.minIndexInterval(DENSELY_ACCESSED_WIDE_ROWS_METADATA))
                .isEqualTo(CassandraConstants.DENSELY_ACCESSED_WIDE_ROWS_INDEX_INTERVAL)
                .isLessThan(CassandraConstants.DEFAULT_MIN_INDEX_INTERVAL);
        assertThat(CassandraTableOptions.maxIndexInterval(DENSELY_ACCESSED_WIDE_ROWS_METADATA))
                .isEqualTo(CassandraConstants.DENSELY_ACCESSED_WIDE_ROWS_INDEX_INTERVAL)
                .isLessThan(CassandraConstants.DEFAULT_MAX_INDEX_INTERVAL);
    }
}
