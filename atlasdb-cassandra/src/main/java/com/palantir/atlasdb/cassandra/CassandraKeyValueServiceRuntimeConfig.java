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

import org.immutables.value.Value;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.auto.service.AutoService;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.keyvalue.cassandra.CassandraConstants;
import com.palantir.atlasdb.spi.KeyValueServiceRuntimeConfig;

@AutoService(KeyValueServiceRuntimeConfig.class)
@JsonDeserialize(as = ImmutableCassandraKeyValueServiceRuntimeConfig.class)
@JsonSerialize(as = ImmutableCassandraKeyValueServiceRuntimeConfig.class)
@Value.Immutable
public abstract class CassandraKeyValueServiceRuntimeConfig implements KeyValueServiceRuntimeConfig {

    @Override
    public String type() {
        return "cassandra";
    }

    /**
     * The minimal period we wait to check if a Cassandra node is healthy after it's been blacklisted.
     */
    @Value.Default
    public int unresponsiveHostBackoffTimeSeconds() {
        return CassandraConstants.DEFAULT_UNRESPONSIVE_HOST_BACKOFF_TIME_SECONDS;
    }

    /**
     * The maximum amount of cells that each thread writes to Cassandra at a time.
     */
    @Value.Default
    public int mutationBatchCount() {
        return CassandraConstants.DEFAULT_MUTATION_BATCH_COUNT;
    }

    /**
     * The maximum amount of bytes that each thread writes to Cassandra at a time.
     */
    @Value.Default
    public int mutationBatchSizeBytes() {
        return CassandraConstants.DEFAULT_MUTATION_BATCH_SIZE_BYTES;
    }

    @Value.Default
    public int fetchBatchCount() {
        return CassandraConstants.DEFAULT_FETCH_BATCH_COUNT;
    }

    /**
     * The number of threads Sweep uses to read values from Cassandra.
     * Each thread fetches values from a distinct row.
     */
    @Value.Default
    public Integer sweepReadThreads() {
        return AtlasDbConstants.DEFAULT_SWEEP_CASSANDRA_READ_THREADS;
    }

    /**
     * The number of times a call to Cassandra retries a single host.
     */
    @Value.Default
    public int numberOfRetriesOnSameHost() {
        return 3;
    }

    /**
     * The maximum number of times a call to Cassandra retries.
     */
    @Value.Default
    public int numberOfRetriesOnAllHosts() {
        return 6;
    }

    public static CassandraKeyValueServiceRuntimeConfig getDefault() {
        return ImmutableCassandraKeyValueServiceRuntimeConfig.builder().build();
    }
}
