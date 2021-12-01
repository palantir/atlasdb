/*
 * (c) Copyright 2018 Palantir Technologies Inc. All rights reserved.
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

import com.google.common.collect.ImmutableSet;

public final class CassandraConstants {
    static final String DEFAULT_COMPRESSION_TYPE = "LZ4Compressor";
    static final double DEFAULT_LEVELED_COMPACTION_BLOOM_FILTER_FP_CHANCE = 0.1;
    static final double DEFAULT_SIZE_TIERED_COMPACTION_BLOOM_FILTER_FP_CHANCE = 0.01;
    static final double NEGATIVE_LOOKUPS_BLOOM_FILTER_FP_CHANCE = 0.01;
    static final double NEGATIVE_LOOKUPS_SIZE_TIERED_BLOOM_FILTER_FP_CHANCE = 0.0001;
    static final double DENSELY_ACCESSED_WIDE_ROWS_BLOOM_FILTER_FP_CHANCE = 0.0001;
    static final String SIMPLE_STRATEGY = "org.apache.cassandra.locator.SimpleStrategy";
    static final String NETWORK_STRATEGY = "org.apache.cassandra.locator.NetworkTopologyStrategy";

    // They're both ordered, we just had to change the name to accommodate datastax client-side driver handling
    static final ImmutableSet<String> ALLOWED_PARTITIONERS = ImmutableSet.of(
            "com.palantir.atlasdb.keyvalue.cassandra.dht.AtlasDbPartitioner",
            "com.palantir.atlasdb.keyvalue.cassandra.dht.AtlasDbOrderedPartitioner",
            "org.apache.cassandra.dht.ByteOrderedPartitioner");

    static final String DEFAULT_DC = "datacenter1";
    static final String DEFAULT_RACK = "rack1";
    static final String SIMPLE_RF_TEST_KEYSPACE = "__simple_rf_test_keyspace__";
    static final String REPLICATION_FACTOR_OPTION = "replication_factor";
    // 1 hour; AtlasDB only performs deletes with consistency ALL, so there is no need to ensure repairs
    // complete within gc_grace_seconds.
    public static final int DEFAULT_GC_GRACE_SECONDS = 60 * 60;

    // this is only used to sanity check reads from a TFramedTransport;
    // writes are sanity checked with server side frame size limits and are user-configurable,
    // so I'm okay with just bypassing the check for reads and having this check only in one place, server side.
    static final int CLIENT_MAX_THRIFT_FRAME_SIZE_BYTES = Integer.MAX_VALUE;

    static final String CFDEF_COMPRESSION_TYPE_KEY = "sstable_compression";
    static final String CFDEF_COMPRESSION_CHUNK_LENGTH_KEY = "chunk_length_kb";

    static final String LEVELED_COMPACTION_STRATEGY = "org.apache.cassandra.db.compaction.LeveledCompactionStrategy";
    static final String SIZE_TIERED_COMPACTION_STRATEGY =
            "org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy";

    public static final int DEFAULT_FETCH_BATCH_COUNT = 5000;
    public static final int DEFAULT_MUTATION_BATCH_SIZE_BYTES = 4 * 1024 * 1024;
    public static final int DEFAULT_MUTATION_BATCH_COUNT = 5000;
    public static final int DEFAULT_UNRESPONSIVE_HOST_BACKOFF_TIME_SECONDS = 30;

    public static final int DEFAULT_CROSS_COLUMN_LOAD_BATCH_LIMIT = 200;
    // TODO (jkong): Review this limit, it seems like we are making very big requests to Cassandra even at this value
    public static final int DEFAULT_SINGLE_QUERY_LOAD_BATCH_LIMIT = 50_000;

    // TODO(Sudiksha): This is used to be compatible with past behaviour
    public static final int DEFAULT_READ_LIMIT_PER_ROW = Integer.MAX_VALUE;

    static final int DENSELY_ACCESSED_WIDE_ROWS_INDEX_INTERVAL = 1;
    static final int DEFAULT_MIN_INDEX_INTERVAL = 128;
    static final int DEFAULT_MAX_INDEX_INTERVAL = 2048;

    public static final String ROW = "key";
    public static final String COLUMN = "column1";
    public static final String TIMESTAMP = "column2";
    public static final String VALUE = "value";

    public static final long CAS_TABLE_TIMESTAMP = 0L;
    public static final long ENCODED_TAS_TABLE_TIMESTAMP = ~CAS_TABLE_TIMESTAMP;

    private CassandraConstants() {
        // Utility class
    }
}
