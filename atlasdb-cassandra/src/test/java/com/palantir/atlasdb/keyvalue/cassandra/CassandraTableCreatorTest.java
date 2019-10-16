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
import static org.mockito.Mockito.mock;

import java.net.InetSocketAddress;

import org.junit.Test;

import com.palantir.atlasdb.cassandra.ImmutableCassandraCredentialsConfig;
import com.palantir.atlasdb.cassandra.ImmutableCassandraKeyValueServiceConfig;
import com.palantir.atlasdb.cassandra.ImmutableDefaultConfig;
import com.palantir.atlasdb.table.description.ImmutableTableMetadata;

public class CassandraTableCreatorTest {
    private CassandraTableCreator cassandraTableCreator =
            new CassandraTableCreator(
                    mock(CassandraClientPool.class),
                    ImmutableCassandraKeyValueServiceConfig.builder()
                            .servers(ImmutableDefaultConfig.builder()
                                    .addThriftHosts(new InetSocketAddress("localhost", 9160))
                                    .build())
                            .credentials(ImmutableCassandraCredentialsConfig.builder()
                                    .username("username")
                                    .password("password")
                                    .build())
                            .replicationFactor(1)
                            .keyspace("test")
                            .build());

    private static final String ALL_DEFAULT_CREATING_STRING = "\n"
            + "\tCREATE TABLE IF NOT EXISTS \"test\".test(\n"
            + "\t\tkey blob,\n"
            + "\t\tcolumn1 blob,\n"
            + "\t\tcolumn2 bigint,\n"
            + "\t\tvalue blob,\n"
            + "\t\tPRIMARY KEY(key, column1, column2))\n"
            + "\tWITH caching = 'keys_only' AND bloom_filter_fp_chance = 0.1 AND "
            + "compression = {'sstable_compression' : 'LZ4Compressor', 'chunk_length_kb' : 4} "
            + "AND compaction = {'class' : 'LeveledCompactionStrategy'} "
            + "AND dclocal_read_repair_chance = 0.1 AND gc_grace_seconds = 3600 AND min_index_interval = 128 AND "
            + "max_index_interval = 2048 AND populate_io_cache_on_flush = false AND speculative_retry = 'NONE' AND "
            + "CLUSTERING ORDER BY(column1 ASC, column2 ASC) AND COMPACT STORAGE";

    @Test
    public void testTableCreationString() {
        assertThat(cassandraTableCreator.createTableQueryString(ImmutableTableMetadata.allDefault(), "test"))
                .isEqualTo(ALL_DEFAULT_CREATING_STRING);
    }
}
