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
package com.palantir.atlasdb.cassandra;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.auto.service.AutoService;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.cassandra.CassandraServersConfigs.CassandraServersConfig;
import com.palantir.atlasdb.keyvalue.cassandra.CassandraConstants;
import com.palantir.atlasdb.spi.KeyValueServiceRuntimeConfig;
import org.immutables.value.Value;

@AutoService(KeyValueServiceRuntimeConfig.class)
@JsonDeserialize(as = ImmutableCassandraKeyValueServiceRuntimeConfig.class)
@JsonSerialize(as = ImmutableCassandraKeyValueServiceRuntimeConfig.class)
@JsonSubTypes({
    @JsonSubTypes.Type(
            value = CassandraKeyValueServiceRuntimeConfig.class,
            name = "CassandraKeyValueServiceRuntimeConfig"), // Deprecated, kept for backwards compatibility
    @JsonSubTypes.Type(
            value = CassandraKeyValueServiceRuntimeConfig.class,
            name = CassandraKeyValueServiceRuntimeConfig.TYPE)
})
@Value.Immutable
public abstract class CassandraKeyValueServiceRuntimeConfig implements KeyValueServiceRuntimeConfig {

    public static final String TYPE = "cassandra";

    @Override
    public String type() {
        return TYPE;
    }

    @Value.Default
    public CassandraServersConfig servers() {
        return ImmutableDefaultConfig.of();
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

    /**
     * The maximum number of rows to query for in a single call to the database when loading entire rows.
     */
    @Value.Default
    public int fetchBatchCount() {
        return CassandraConstants.DEFAULT_FETCH_BATCH_COUNT;
    }

    /**
     * Limits on query sizes when loading cells from an underlying Cassandra key-value service.
     */
    @Value.Default
    public CassandraCellLoadingConfig cellLoadingConfig() {
        return CassandraCellLoadingConfig.defaultConfig();
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

    @Value.Default
    public int fetchReadLimitPerRow() {
        return CassandraConstants.DEFAULT_READ_LIMIT_PER_ROW;
    }

    /**
     * Setting this value to true will cause us to take a more conservative approach to retrying requests on exceptions.
     */
    @Value.Default
    public boolean conservativeRequestExceptionHandler() {
        return true;
    }

    /**
     * Config that controls which cassandra queries will be traced. The default is nothing is traced.
     */
    @Value.Default
    public CassandraTracingConfig tracing() {
        return ImmutableCassandraTracingConfig.builder().build();
    }

    public static CassandraKeyValueServiceRuntimeConfig getDefault() {
        return ImmutableCassandraKeyValueServiceRuntimeConfig.builder().build();
    }
}
