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

package com.palantir.atlasdb.cassandra;

import com.palantir.atlasdb.cassandra.CassandraServersConfigs.CassandraServersConfig;
import com.palantir.atlasdb.spi.KeyValueServiceRuntimeConfig;

public interface CassandraKeyValueServiceRuntimeConfig extends KeyValueServiceRuntimeConfig {
    String TYPE = "cassandra";

    String type();

    CassandraServersConfig servers();

    int replicationFactor();
    /**
     * The minimal period we wait to check if a Cassandra node is healthy after it's been blacklisted.
     */
    int unresponsiveHostBackoffTimeSeconds();

    /**
     * The maximum amount of cells that each thread writes to Cassandra at a time.
     */
    int mutationBatchCount();

    /**
     * The maximum amount of bytes that each thread writes to Cassandra at a time.
     */
    int mutationBatchSizeBytes();

    /**
     * The maximum number of rows to query for in a single call to the database when loading entire rows.
     */
    int fetchBatchCount();

    /**
     * Limits on query sizes when loading cells from an underlying Cassandra key-value service.
     */
    CassandraCellLoadingConfig cellLoadingConfig();

    /**
     * The number of threads Sweep uses to read values from Cassandra.
     * Each thread fetches values from a distinct row.
     */
    Integer sweepReadThreads();

    /**
     * The number of times a call to Cassandra retries a single host.
     */
    int numberOfRetriesOnSameHost();

    /**
     * The maximum number of times a call to Cassandra retries.
     */
    int numberOfRetriesOnAllHosts();

    int fetchReadLimitPerRow();

    /**
     * Setting this value to true will cause us to take a more conservative approach to retrying requests on exceptions.
     */
    boolean conservativeRequestExceptionHandler();

    /**
     * Config that controls which cassandra queries will be traced. The default is nothing is traced.
     */
    CassandraTracingConfig tracing();
}
