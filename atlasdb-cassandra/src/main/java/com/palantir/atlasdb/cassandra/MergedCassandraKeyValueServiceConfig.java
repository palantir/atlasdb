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
import com.palantir.atlasdb.keyvalue.cassandra.async.CassandraAsyncKeyValueServiceFactory;
import com.palantir.atlasdb.keyvalue.cassandra.pool.HostLocation;
import com.palantir.atlasdb.spi.SharedResourcesConfig;
import com.palantir.conjure.java.api.config.service.HumanReadableDuration;
import com.palantir.conjure.java.api.config.ssl.SslConfiguration;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.function.Supplier;

public interface MergedCassandraKeyValueServiceConfig {
    CassandraServersConfig servers();
    Map<String, InetSocketAddress> addressTranslation();
    Optional<String> namespace();
    int poolSize();
    int maxConnectionBurstSize();
    double proportionConnectionsToCheckPerEvictionRun();
    int idleConnectionTimeoutSeconds();
    int timeBetweenConnectionEvictionRunsSeconds();
    int poolRefreshIntervalSeconds();
    int unresponsiveHostBackoffTimeSeconds();
    int gcGraceSeconds();
    double localHostWeighting();
    int consecutiveAbsencesBeforePoolRemoval();
    Optional<HostLocation> overrideHostLocation();
    Duration timeoutOnConnectionClose();
    HumanReadableDuration timeoutOnConnectionBorrow();
    String getKeyspaceOrThrow();
    Optional<String> keyspace();
    CassandraCredentialsConfig credentials();
    Optional<SslConfiguration> sslConfiguration();
    CassandraAsyncKeyValueServiceFactory asyncKeyValueServiceFactory();
    Optional<Supplier<ExecutorService>> thriftExecutorServiceFactory();
    int replicationFactor();
    int mutationBatchCount();
    int mutationBatchSizeBytes();
    int fetchBatchCount();
    boolean ignoreNodeTopologyChecks();
    boolean ignoreInconsistentRingChecks();
    boolean ignoreDatacenterConfigurationChecks();
    boolean ignorePartitionerChecks();
    boolean autoRefreshNodes();
    boolean clusterMeetsNormalConsistencyGuarantees();
    int socketTimeoutMillis();
    int socketQueryTimeoutMillis();
    int initialSocketQueryTimeoutMillis();
    int cqlPoolTimeoutMillis();
    int schemaMutationTimeoutMillis();
    int rangesConcurrency();
    int sweepReadThreads();
    Optional<CassandraJmxCompactionConfig> jmx();
    String type();
    Integer timestampsGetterBatchSize();
    int concurrentGetRangesThreadPoolSize();
    int defaultGetRangesConcurrency();
    Optional<SharedResourcesConfig> sharedResourcesConfig();
    CassandraCellLoadingConfig cellLoadingConfig();
    int numberOfRetriesOnSameHost();
    int numberOfRetriesOnAllHosts();
    int fetchReadLimitPerRow();
    boolean conservativeRequestExceptionHandler();
    CassandraTracingConfig tracing();
}
