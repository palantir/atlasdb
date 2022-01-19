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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.auto.service.AutoService;
import com.google.common.collect.ImmutableMap;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.cassandra.CassandraServersConfigs.CassandraServersConfig;
import com.palantir.atlasdb.keyvalue.cassandra.CassandraConstants;
import com.palantir.atlasdb.keyvalue.cassandra.async.CassandraAsyncKeyValueServiceFactory;
import com.palantir.atlasdb.keyvalue.cassandra.async.DefaultCassandraAsyncKeyValueServiceFactory;
import com.palantir.atlasdb.keyvalue.cassandra.pool.HostLocation;
import com.palantir.atlasdb.spi.KeyValueServiceConfig;
import com.palantir.conjure.java.api.config.service.HumanReadableDuration;
import com.palantir.conjure.java.api.config.ssl.SslConfiguration;
import com.palantir.logsafe.Preconditions;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;
import java.net.InetSocketAddress;
import java.net.SocketTimeoutException;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.function.Supplier;
import org.immutables.value.Value;

@AutoService(KeyValueServiceConfig.class)
@JsonDeserialize(as = ImmutableCassandraKeyValueServiceConfig.class)
@JsonSerialize(as = ImmutableCassandraKeyValueServiceConfig.class)
@JsonTypeName(CassandraKeyValueServiceConfig.TYPE)
@Value.Immutable
public interface CassandraKeyValueServiceConfig extends KeyValueServiceConfig {

    String TYPE = "cassandra";

    /**
     * These are only the initial 'contact points' that will be used in connecting with the cluster. AtlasDB will
     * subsequently discover additional hosts in the cluster. (This is true for both Thrift and CQL endpoints.)
     *
     * This value, or values derived from it (e.g. the number of Thrift hosts) must ONLY be used on KVS initialization
     * to generate the initial connection(s) to the cluster, or as part of startup checks.
     */
    @Value.Default
    default CassandraServersConfig servers() {
        return ImmutableDefaultConfig.of();
    }

    @Value.Default
    default Map<String, InetSocketAddress> addressTranslation() {
        return ImmutableMap.of();
    }

    @Override
    @JsonIgnore
    @Value.Derived
    default Optional<String> namespace() {
        return keyspace();
    }

    @Value.Default
    default int poolSize() {
        return 30;
    }

    /**
     * The cap at which the connection pool is able to grow over the {@link #poolSize()} given high request load. When
     * load is depressed, the pool will shrink back to its idle {@link #poolSize()} value.
     */
    @Value.Default
    default int maxConnectionBurstSize() {
        return 100;
    }

    /**
     * The proportion of {@link #poolSize()} connections that are checked approximately every {@link
     * #timeBetweenConnectionEvictionRunsSeconds()} seconds to see if has been idle at least {@link
     * #idleConnectionTimeoutSeconds()} seconds and evicts it from the pool if so. For example, given the the default
     * values, 0.1 * 30 = 3 connections will be checked approximately every 20 seconds and will be evicted from the pool
     * if it has been idle for at least 10 minutes.
     */
    @Value.Default
    default double proportionConnectionsToCheckPerEvictionRun() {
        return 0.1;
    }

    @Value.Default
    default int idleConnectionTimeoutSeconds() {
        return 10 * 60;
    }

    @Value.Default
    default int timeBetweenConnectionEvictionRunsSeconds() {
        return 20;
    }

    /**
     * The period between refreshing the Cassandra client pools. At every refresh, we check the health of the current
     * blacklisted nodes — if they're healthy, we whitelist them.
     */
    @Value.Default
    default int poolRefreshIntervalSeconds() {
        return 2 * 60;
    }

    /**
     * The minimal period we wait to check if a Cassandra node is healthy after it's been blacklisted.
     *
     * @deprecated Use {@link CassandraKeyValueServiceRuntimeConfig#unresponsiveHostBackoffTimeSeconds()} to make this
     * value live-reloadable.
     */
    @SuppressWarnings("DeprecatedIsStillUsed")
    @Value.Default
    @Deprecated
    default int unresponsiveHostBackoffTimeSeconds() {
        return CassandraConstants.DEFAULT_UNRESPONSIVE_HOST_BACKOFF_TIME_SECONDS;
    }

    /**
     * The gc_grace_seconds for all tables(column families). This is the maximum TTL for tombstones in Cassandra as data
     * marked with a tombstone is removed during the normal compaction process every gc_grace_seconds.
     */
    @Value.Default
    default int gcGraceSeconds() {
        return CassandraConstants.DEFAULT_GC_GRACE_SECONDS;
    }

    /**
     * This increases the likelihood of selecting an instance that is hosted in the same rack as the process.
     * Weighting is a ratio from 0 to 1, where 0 disables the feature and 1 forces the same rack if possible.
     */
    @Value.Default
    default double localHostWeighting() {
        return 1.0;
    }

    /**
     * This sets the number of times a node needs to be detected as absent from the Cassandra ring before its client
     * pool is removed. Configuring this may be useful for nodes operating in environments where IPs change frequently.
     */
    @Value.Default
    default int consecutiveAbsencesBeforePoolRemoval() {
        return 10;
    }

    /**
     * STOPSHIP I am for RC only, this would make me a lot more expensive if just rolled out
     */
    @Value.Default
    default boolean tryToDetermineMyLocation() {
        return false;
    }

    /**
     * Overrides the behaviour of the host location supplier.
     */
    Optional<HostLocation> overrideHostLocation();

    /**
     * Times out after the provided duration when attempting to close evicted connections from the Cassandra
     * threadpool. Note that this is potentially unsafe in the general case, as unclosed connections can eventually leak
     * memory.
     */
    @Value.Default
    default Duration timeoutOnConnectionClose() {
        return Duration.ofSeconds(10);
    }

    /**
     * Times out after the provided duration when borrowing objects from the Cassandra client pool.
     */
    @Value.Default
    default HumanReadableDuration timeoutOnConnectionBorrow() {
        return HumanReadableDuration.minutes(60);
    }

    @JsonIgnore
    @Value.Lazy
    default String getKeyspaceOrThrow() {
        return keyspace()
                .orElseThrow(() -> new SafeIllegalStateException(
                        "Tried to read the keyspace from a CassandraConfig when it hadn't been set!"));
    }

    /**
     * Note that when the keyspace is read, this field must be present.
     *
     * @deprecated Use the AtlasDbConfig#namespace to specify it instead.
     */
    @Deprecated
    Optional<String> keyspace();

    CassandraCredentialsConfig credentials();

    /**
     * A boolean declaring whether or not to use ssl to communicate with cassandra. This configuration value is
     * deprecated in favor of using sslConfiguration.  If true, read in trust and key store information from system
     * properties unless the sslConfiguration object is specified.
     *
     * @deprecated Use {@link #sslConfiguration()} instead.
     */
    @Deprecated
    Optional<Boolean> ssl();

    /**
     * An object for specifying ssl configuration details.  The lack of existence of this object implies ssl is not to
     * be used to connect to cassandra.
     * <p>
     * The existence of this object overrides any configuration made via the ssl config value.
     */
    Optional<SslConfiguration> sslConfiguration();

    /**
     * An object which implements the logic behind CQL communication resource management. Default factory object creates
     * a new {@link com.palantir.atlasdb.keyvalue.api.AsyncKeyValueService} with new session and thread pool every time.
     * For smarter resource management this option should be programmatically set for client specific resource
     * management.
     */
    @Value.Default
    @JsonIgnore
    default CassandraAsyncKeyValueServiceFactory asyncKeyValueServiceFactory() {
        return DefaultCassandraAsyncKeyValueServiceFactory.DEFAULT;
    }

    /**
     * If provided, will be used to create executor service used to perform some blocking calls to Cassandra.
     * Otherwise, a new executor service will be created with default configuration.
     */
    @JsonIgnore
    Optional<Supplier<ExecutorService>> thriftExecutorServiceFactory();

    int replicationFactor();

    /**
     * @deprecated Use {@link CassandraKeyValueServiceRuntimeConfig#mutationBatchCount()} to make this value
     * live-reloadable.
     */
    @SuppressWarnings("DeprecatedIsStillUsed")
    @Value.Default
    @Deprecated
    default int mutationBatchCount() {
        return CassandraConstants.DEFAULT_MUTATION_BATCH_COUNT;
    }

    /**
     * @deprecated Use {@link CassandraKeyValueServiceRuntimeConfig#mutationBatchSizeBytes()} to make this value
     * live-reloadable.
     */
    @SuppressWarnings("DeprecatedIsStillUsed")
    @Value.Default
    @Deprecated
    default int mutationBatchSizeBytes() {
        return CassandraConstants.DEFAULT_MUTATION_BATCH_SIZE_BYTES;
    }

    /**
     * @deprecated Use {@link CassandraKeyValueServiceRuntimeConfig#fetchBatchCount()} to make this value
     * live-reloadable.
     */
    @SuppressWarnings("DeprecatedIsStillUsed")
    @Value.Default
    @Deprecated
    default int fetchBatchCount() {
        return CassandraConstants.DEFAULT_FETCH_BATCH_COUNT;
    }

    @Value.Default
    default boolean ignoreNodeTopologyChecks() {
        return false;
    }

    @Value.Default
    default boolean ignoreInconsistentRingChecks() {
        return false;
    }

    @Value.Default
    default boolean ignoreDatacenterConfigurationChecks() {
        return false;
    }

    @Value.Default
    default boolean ignorePartitionerChecks() {
        return false;
    }

    @Value.Default
    default boolean autoRefreshNodes() {
        return true;
    }

    @Value.Default
    default boolean clusterMeetsNormalConsistencyGuarantees() {
        return true;
    }

    /**
     * This is how long we will wait when we first open a socket to the cassandra server. This should be long enough to
     * enough to handle cross data center latency, but short enough that it will fail out quickly if it is clear we
     * can't reach that server.
     */
    @Value.Default
    default int socketTimeoutMillis() {
        return 2 * 1000;
    }

    /**
     * Socket timeout is a java side concept.  This the maximum time we will block on a network read without the server
     * sending us any bytes.  After this time a {@link SocketTimeoutException} will be thrown.  All cassandra reads time
     * out at less than this value so we shouldn't see it very much (10s by default).
     */
    @Value.Default
    default int socketQueryTimeoutMillis() {
        return 62 * 1000;
    }

    @Value.Default
    default int cqlPoolTimeoutMillis() {
        return 20 * 1000;
    }

    @Value.Default
    default int schemaMutationTimeoutMillis() {
        return 120 * 1000;
    }

    @Value.Default
    default int rangesConcurrency() {
        return 32;
    }

    /**
     * Obsolete value, replaced by {@link SweepConfig#readLimit}.
     *
     * @deprecated this parameter is unused and should be removed from the configuration
     */
    @SuppressWarnings("DeprecatedIsStillUsed") // Used by immutable copy of this file
    @Value.Default
    @Deprecated
    default Integer timestampsGetterBatchSize() {
        return 1_000;
    }

    /**
     * @deprecated Use {@link CassandraKeyValueServiceRuntimeConfig#sweepReadThreads()} to make this value
     * live-reloadable.
     */
    @SuppressWarnings("DeprecatedIsStillUsed")
    @Value.Default
    @Deprecated
    default Integer sweepReadThreads() {
        return AtlasDbConstants.DEFAULT_SWEEP_CASSANDRA_READ_THREADS;
    }

    Optional<CassandraJmxCompactionConfig> jmx();

    @Override
    default String type() {
        return TYPE;
    }

    @Override
    @Value.Default
    default int concurrentGetRangesThreadPoolSize() {
        return poolSize() * servers().numberOfThriftHosts();
    }

    @JsonIgnore
    @Value.Derived
    default boolean usingSsl() {
        return ssl().orElseGet(sslConfiguration()::isPresent);
    }

    @Value.Check
    default void check() {
        double evictionCheckProportion = proportionConnectionsToCheckPerEvictionRun();
        Preconditions.checkArgument(
                evictionCheckProportion > 0.01 && evictionCheckProportion <= 1,
                "'proportionConnectionsToCheckPerEvictionRun' must be between 0.01 and 1");

        Preconditions.checkArgument(
                localHostWeighting() >= 0.0 && localHostWeighting() <= 1.0,
                "'localHostWeighting' must be between 0 and 1 inclusive");
    }
}
