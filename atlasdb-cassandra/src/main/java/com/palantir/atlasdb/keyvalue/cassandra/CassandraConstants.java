/**
 * Copyright 2015 Palantir Technologies
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
import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.cassandra.thrift.CfDef;
import org.apache.cassandra.thrift.ColumnDef;
import org.apache.cassandra.thrift.TriggerDef;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;


public class CassandraConstants {
    /**
     * Socket timeout is a java side concept.  This the maximum time we will block on a network
     * read without the server sending us any bytes.  After this time a {@link SocketTimeoutException}
     * will be thrown.  All cassandra reads time out at less than this value so we shouldn't see
     * it very much (10s by default).
     */
    static final int SOCKET_TIMEOUT_MILLIS = 22000;
    /**
     * This is how long we will wait when we first open a socket to the cassandra server.
     * This should be long enough to enough to handle cross data center latency, but short enough
     * that it will fail out quickly if it is clear we can't reach that server.
     */
    static final int CONNECTION_TIMEOUT_MILLIS = 2000;
    static final String METADATA_TABLE = "_metadata";
    public static final int DEFAULT_REPLICATION_FACTOR = 3;
    static final int SECONDS_BETWEEN_GETTING_HOST_LIST = 600; // 10 min
    static final int SECONDS_WAIT_FOR_VERSIONS = 20;

    static final int INDEX_COMPRESSION_KB = 4;
    static final int TABLE_COMPRESSION_KB = 64;
    static final String DEFAULT_COMPRESSION_TYPE = "LZ4Compressor";
    static final int PUT_BATCH_SIZE = 100;
    static final String SSTABLE_SIZE_IN_MB = "80";
    static final double DEFAULT_LEVELED_COMPACTION_BLOOM_FILTER_FP_CHANCE = 0.1;
    static final double NEGATIVE_LOOKUPS_BLOOM_FILTER_FP_CHANCE = 0.01;
    static final String SIMPLE_STRATEGY = "org.apache.cassandra.locator.SimpleStrategy";
    static final String NETWORK_STRATEGY = "org.apache.cassandra.locator.NetworkTopologyStrategy";
    static final String PARTITIONER = "com.palantir.atlasdb.keyvalue.cassandra.dht.AtlasDbPartitioner";
    static final String PARTITIONER2 = "org.apache.cassandra.dht.ByteOrderedPartitioner";
    static final String DEFAULT_DC = "datacenter1";
    static final String DEFAULT_RACK = "rack1";
    static final String SIMPLE_RF_TEST_KEYSPACE = "__simple_rf_test_keyspace__";
    static final String REPLICATION_FACTOR_OPTION = "replication_factor";
    static final long SECONDS_TO_WAIT_FOR_SCHEMA_MUTATION_LOCK = 60;
    static final int GC_GRACE_SECONDS = 4 * 24 * 60 * 60; // 4 days; Hinted-Handoffs MUST expire well within this period for delete correctness (I believe we will be expiring hints in half this period)

    // this is only used to sanity check reads from a TFramedTransport;
    // writes are sanity checked with server side frame size limits and are user-configurable,
    // so I'm okay with just bypassing the check for reads and having this check only in one place, server side.
    static final int CLIENT_MAX_THRIFT_FRAME_SIZE_BYTES = Integer.MAX_VALUE;

    static final String CFDEF_COMPRESSION_TYPE_KEY = "sstable_compression";
    static final String CFDEF_COMPRESSION_CHUNK_LENGTH_KEY = "chunk_length_kb";

    // update CKVS.isMatchingCf if you update this method
    static CfDef getStandardCfDef(String keyspace, String internalTableName) {
        CfDef cf = new CfDef(keyspace, internalTableName);
        cf.setComparator_type("CompositeType(BytesType,LongType)");
        cf.setCompaction_strategy("LeveledCompactionStrategy");
        cf.setCompaction_strategy_options(ImmutableMap.of("sstable_size_in_mb", CassandraConstants.SSTABLE_SIZE_IN_MB));
        cf.setCompression_options(Maps.<String, String>newHashMap());
        cf.setGc_grace_seconds(GC_GRACE_SECONDS);

        // explicitly set fields to default values
        cf.setCaching("KEYS_ONLY");
        cf.setDclocal_read_repair_chance(0.1);
        cf.setTriggers(new ArrayList<TriggerDef>());
        cf.setCells_per_row_to_cache("0");
        cf.setMin_index_interval(128);
        cf.setMax_index_interval(2048);
        cf.setComment("");
        cf.setColumn_metadata(new ArrayList<ColumnDef>());
        cf.setMin_compaction_threshold(4);
        cf.setMax_compaction_threshold(32);
        cf.setKey_validation_class("org.apache.cassandra.db.marshal.BytesType");
        cf.setCompaction_strategy_options(new HashMap<String, String>());
        cf.setDefault_validation_class("org.apache.cassandra.db.marshal.BytesType");

        return cf;
    }
}
