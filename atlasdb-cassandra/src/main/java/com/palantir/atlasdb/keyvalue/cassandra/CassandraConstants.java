/*
 * Copyright 2015 Palantir Technologies, Inc. All rights reserved.
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
package com.palantir.atlasdb.keyvalue.cassandra;

import java.util.Set;

import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.keyvalue.api.TableReference;

public final class CassandraConstants {
    static final int LONG_RUNNING_QUERY_SOCKET_TIMEOUT_MILLIS = 62000;
    public static final int DEFAULT_REPLICATION_FACTOR = 3;
    public static final int DEFAULT_THRIFT_PORT = 9160;
    public static final int DEFAULT_CQL_PORT = 9042;
    static final int SECONDS_WAIT_FOR_VERSIONS = 60;
    static final int MAX_TRUNCATION_ATTEMPTS = 3; // tied to an exponential timeout, be careful if you change it

    static final int ABSOLUTE_MINIMUM_NUMBER_OF_TOKENS_PER_NODE = 32;
    static final long TS_SIZE = 4L;

    static final String DEFAULT_COMPRESSION_TYPE = "LZ4Compressor";
    static final String SSTABLE_SIZE_IN_MB = "80";
    static final double DEFAULT_LEVELED_COMPACTION_BLOOM_FILTER_FP_CHANCE = 0.1;
    static final double DEFAULT_SIZE_TIERED_COMPACTION_BLOOM_FILTER_FP_CHANCE = 0.01;
    static final double NEGATIVE_LOOKUPS_BLOOM_FILTER_FP_CHANCE = 0.01;
    static final double NEGATIVE_LOOKUPS_SIZE_TIERED_BLOOM_FILTER_FP_CHANCE = 0.0001;
    static final String SIMPLE_STRATEGY = "org.apache.cassandra.locator.SimpleStrategy";
    static final String NETWORK_STRATEGY = "org.apache.cassandra.locator.NetworkTopologyStrategy";

    // They're both ordered, we just had to change the name to accommodate datastax client-side driver handling
    static final Set<String> ALLOWED_PARTITIONERS = ImmutableSet.of(
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
    static final float TOMBSTONE_THRESHOLD_RATIO = 0.2f;

    // JMX compaction related
    public static final String JMX_RMI = "service:jmx:rmi:///jndi/rmi://[%s]:%d/jmxrmi";
    public static final String STORAGE_SERVICE_OBJECT_NAME = "org.apache.cassandra.db:type=StorageService";
    public static final String COMPACTION_MANAGER_OBJECT_NAME = "org.apache.cassandra.db:type=CompactionManager";
    public static final String HINTED_HANDOFF_MANAGER_OBJECT_NAME = "org.apache.cassandra.db:type=HintedHandoffManager";

    // this is only used to sanity check reads from a TFramedTransport;
    // writes are sanity checked with server side frame size limits and are user-configurable,
    // so I'm okay with just bypassing the check for reads and having this check only in one place, server side.
    static final int CLIENT_MAX_THRIFT_FRAME_SIZE_BYTES = Integer.MAX_VALUE;

    static final String CFDEF_COMPRESSION_TYPE_KEY = "sstable_compression";
    static final String CFDEF_COMPRESSION_CHUNK_LENGTH_KEY = "chunk_length_kb";

    public static final TableReference NO_TABLE = TableReference.createWithEmptyNamespace("SYSTEM");
    public static final int NO_TTL = -1;

    static final String LEVELED_COMPACTION_STRATEGY = "org.apache.cassandra.db.compaction.LeveledCompactionStrategy";
    static final String SIZE_TIERED_COMPACTION_STRATEGY =
            "org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy";

    public static final String GLOBAL_DDL_LOCK_ROW_NAME = "Global DDL lock";
    public static final String GLOBAL_DDL_LOCK_COLUMN_NAME = "id_with_lock";

    static final int SCHEMA_MUTATION_LOCK_TIMEOUT_MULTIPLIER = 10;

    public static final int DEFAULT_FETCH_BATCH_COUNT = 5000;
    public static final int DEFAULT_MUTATION_BATCH_SIZE_BYTES = 4 * 1024 * 1024;
    public static final int DEFAULT_MUTATION_BATCH_COUNT = 5000;
    public static final int DEFAULT_UNRESPONSIVE_HOST_BACKOFF_TIME_SECONDS = 30;

    private CassandraConstants() {
        // Utility class
    }

}
