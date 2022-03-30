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

import com.palantir.atlasdb.keyvalue.cassandra.async.CassandraAsyncKeyValueServiceFactory;
import com.palantir.atlasdb.keyvalue.cassandra.pool.HostLocation;
import com.palantir.atlasdb.spi.KeyValueServiceConfig;
import com.palantir.conjure.java.api.config.service.HumanReadableDuration;
import com.palantir.conjure.java.api.config.ssl.SslConfiguration;
import java.net.InetSocketAddress;
import java.net.SocketTimeoutException;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.function.Supplier;

public interface CassandraKeyValueServiceConfig extends KeyValueServiceConfig {

    String TYPE = "cassandra";

    Map<String, InetSocketAddress> addressTranslation();

    int poolSize();

    /**
     * The cap at which the connection pool is able to grow over the {@link #poolSize()} given high request load. When
     * load is depressed, the pool will shrink back to its idle {@link #poolSize()} value.
     */
    int maxConnectionBurstSize();

    /**
     * The proportion of {@link #poolSize()} connections that are checked approximately every {@link
     * #timeBetweenConnectionEvictionRunsSeconds()} seconds to see if has been idle at least {@link
     * #idleConnectionTimeoutSeconds()} seconds and evicts it from the pool if so. For example, given the the default
     * values, 0.1 * 30 = 3 connections will be checked approximately every 20 seconds and will be evicted from the pool
     * if it has been idle for at least 10 minutes.
     */
    double proportionConnectionsToCheckPerEvictionRun();

    int idleConnectionTimeoutSeconds();

    int timeBetweenConnectionEvictionRunsSeconds();

    /**
     * The period between refreshing the Cassandra client pools. At every refresh, we check the health of the current
     * blacklisted nodes — if they're healthy, we whitelist them.
     */
    int poolRefreshIntervalSeconds();

    /**
     * The minimal period we wait to check if a Cassandra node is healthy after it's been blacklisted.
     *
     * @deprecated Use {@link CassandraKeyValueServiceRuntimeConfig#unresponsiveHostBackoffTimeSeconds()} to make this
     * value live-reloadable.
     */
    @SuppressWarnings("DeprecatedIsStillUsed")
    @Deprecated
    int unresponsiveHostBackoffTimeSeconds();

    /**
     * The gc_grace_seconds for all tables(column families). This is the maximum TTL for tombstones in Cassandra as data
     * marked with a tombstone is removed during the normal compaction process every gc_grace_seconds.
     */
    int gcGraceSeconds();

    /**
     * This increases the likelihood of selecting an instance that is hosted in the same rack as the process.
     * Weighting is a ratio from 0 to 1, where 0 disables the feature and 1 forces the same rack if possible.
     */
    double localHostWeighting();

    /**
     * This sets the number of times a node needs to be detected as absent from the Cassandra ring before its client
     * pool is removed. Configuring this may be useful for nodes operating in environments where IPs change frequently.
     */
    int consecutiveAbsencesBeforePoolRemoval();

    /**
     * Overrides the behaviour of the host location supplier.
     */
    Optional<HostLocation> overrideHostLocation();

    /**
     * Times out after the provided duration when attempting to close evicted connections from the Cassandra
     * threadpool. Note that this is potentially unsafe in the general case, as unclosed connections can eventually leak
     * memory.
     */
    Duration timeoutOnConnectionClose();

    /**
     * Times out after the provided duration when borrowing objects from the Cassandra client pool.
     */
    HumanReadableDuration timeoutOnConnectionBorrow();

    String getKeyspaceOrThrow();

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
    CassandraAsyncKeyValueServiceFactory asyncKeyValueServiceFactory();

    /**
     * If provided, will be used to create executor service used to perform some blocking calls to Cassandra.
     * Otherwise, a new executor service will be created with default configuration.
     */
    Optional<Supplier<ExecutorService>> thriftExecutorServiceFactory();

    Optional<Integer> replicationFactor();

    /**
     * @deprecated Use {@link CassandraKeyValueServiceRuntimeConfig#mutationBatchCount()} to make this value
     * live-reloadable.
     */
    @SuppressWarnings("DeprecatedIsStillUsed")
    @Deprecated
    int mutationBatchCount();

    /**
     * @deprecated Use {@link CassandraKeyValueServiceRuntimeConfig#mutationBatchSizeBytes()} to make this value
     * live-reloadable.
     */
    @SuppressWarnings("DeprecatedIsStillUsed")
    @Deprecated
    int mutationBatchSizeBytes();

    /**
     * @deprecated Use {@link CassandraKeyValueServiceRuntimeConfig#fetchBatchCount()} to make this value
     * live-reloadable.
     */
    @SuppressWarnings("DeprecatedIsStillUsed")
    @Deprecated
    int fetchBatchCount();

    boolean ignoreNodeTopologyChecks();

    boolean ignoreInconsistentRingChecks();

    boolean ignoreDatacenterConfigurationChecks();

    boolean ignorePartitionerChecks();

    boolean autoRefreshNodes();

    boolean clusterMeetsNormalConsistencyGuarantees();

    /**
     * This is how long we will wait when we first open a socket to the cassandra server. This should be long enough to
     * enough to handle cross data center latency, but short enough that it will fail out quickly if it is clear we
     * can't reach that server.
     */
    int socketTimeoutMillis();

    /**
     * Socket timeout is a java side concept.  This the maximum time we will block on a network read without the server
     * sending us any bytes.  After this time a {@link SocketTimeoutException} will be thrown.  All cassandra reads time
     * out at less than this value so we shouldn't see it very much (10s by default).
     */
    int socketQueryTimeoutMillis();

    /**
     * When a Cassandra node is down or acting malignantly, it is plausible that we succeed in creating a socket but
     * subsequently do not read anything, and thus are at the mercy of the {@link #socketQueryTimeoutMillis()}. This is
     * particularly problematic on the creation of transaction managers and client pools in general: we can end up in
     * a state where a query to a bad host blocks us for up to 186 seconds with default settings (due to retrying three
     * times on the first node).
     *
     * This initial timeout actually affects the creation of the clients themselves, and is only set for the call to
     * login on Cassandra. When that is successful, the query timeout is set to the regular setting above.
     */
    int initialSocketQueryTimeoutMillis();

    int cqlPoolTimeoutMillis();

    int schemaMutationTimeoutMillis();

    int rangesConcurrency();

    /**
     * Obsolete value, replaced by {@link SweepConfig#readLimit}.
     *
     * @deprecated this parameter is unused and should be removed from the configuration
     */
    @SuppressWarnings("DeprecatedIsStillUsed") // Used by immutable copy of this file
    @Deprecated
    Integer timestampsGetterBatchSize();

    /**
     * @deprecated Use {@link CassandraKeyValueServiceRuntimeConfig#sweepReadThreads()} to make this value
     * live-reloadable.
     */
    @SuppressWarnings("DeprecatedIsStillUsed")
    @Deprecated
    Integer sweepReadThreads();

    Optional<CassandraJmxCompactionConfig> jmx();

    String type();

    int concurrentGetRangesThreadPoolSize();

    boolean usingSsl();

    /**
     * Number of threads to be used across ALL transaction managers to refresh the client pool. It is suggested to use
     * one thread per approximately 10 transaction managers that are expected to be used.
     */
    int numPoolRefreshingThreads();

}
