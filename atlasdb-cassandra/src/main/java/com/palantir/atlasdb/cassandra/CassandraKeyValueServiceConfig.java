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
package com.palantir.atlasdb.cassandra;

import java.net.InetSocketAddress;
import java.net.SocketTimeoutException;
import java.util.Optional;
import java.util.Set;

import org.immutables.value.Value;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.auto.service.AutoService;
import com.google.common.base.Preconditions;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.keyvalue.cassandra.CassandraConstants;
import com.palantir.atlasdb.spi.KeyValueServiceConfig;
import com.palantir.remoting.api.config.ssl.SslConfiguration;

@AutoService(KeyValueServiceConfig.class)
@JsonDeserialize(as = ImmutableCassandraKeyValueServiceConfig.class)
@JsonSerialize(as = ImmutableCassandraKeyValueServiceConfig.class)
@JsonTypeName(CassandraKeyValueServiceConfig.TYPE)
@Value.Immutable
public abstract class CassandraKeyValueServiceConfig implements KeyValueServiceConfig {

    public static final String TYPE = "cassandra";

    public abstract Set<InetSocketAddress> servers();

    @Override
    @JsonIgnore
    @Value.Derived
    public Optional<String> namespace() {
        return keyspace();
    }

    @Value.Default
    public int poolSize() {
        return 30;
    }

    /**
     * The cap at which the connection pool is able to grow over the {@link #poolSize()}
     * given high request load. When load is depressed, the pool will shrink back to its
     * idle {@link #poolSize()} value.
     */
    @Value.Default
    public int maxConnectionBurstSize() {
        return 100;
    }

    /**
     * The proportion of {@link #poolSize()} connections that are checked approximately
     * every {@link #timeBetweenConnectionEvictionRunsSeconds()} seconds to see if has been idle at least
     * {@link #idleConnectionTimeoutSeconds()} seconds and evicts it from the pool if so. For example, given the
     * the default values, 0.1 * 30 = 3 connections will be checked approximately every 20 seconds and will
     * be evicted from the pool if it has been idle for at least 10 minutes.
     */
    @Value.Default
    public double proportionConnectionsToCheckPerEvictionRun() {
        return 0.1;
    }

    @Value.Default
    public int idleConnectionTimeoutSeconds() {
        return 10 * 60;
    }

    @Value.Default
    public int timeBetweenConnectionEvictionRunsSeconds() {
        return 20;
    }

    /**
     * The period between refreshing the Cassandra client pools.
     * At every refresh, we check the health of the current blacklisted nodes — if they're healthy, we whitelist them.
     */
    @Value.Default
    public int poolRefreshIntervalSeconds() {
        return 2 * 60;
    }

    /**
     * The minimal period we wait to check if a Cassandra node is healthy after it's been blacklisted.
     *
     * @deprecated Use {@link CassandraKeyValueServiceRuntimeConfig#mutationBatchCount()} to make this value
     * live-reloadable.
     */
    @Value.Default
    @Deprecated
    public int unresponsiveHostBackoffTimeSeconds() {
        return 30;
    }

    /**
     * The gc_grace_seconds for all tables(column families). This is the maximum TTL for tombstones in Cassandra
     * as data marked with a tombstone is removed during the normal compaction process every gc_grace_seconds.
     */
    @Value.Default
    public int gcGraceSeconds() {
        return CassandraConstants.DEFAULT_GC_GRACE_SECONDS;
    }

    @JsonIgnore
    @Value.Lazy
    public String getKeyspaceOrThrow() {
        return keyspace().orElseThrow(() -> new IllegalStateException(
                "Tried to read the keyspace from a CassandraConfig when it hadn't been set!"));
    }

    /**
     * Note that when the keyspace is read, this field must be present.
     * @deprecated Use the AtlasDbConfig#namespace to specify it instead.
     */
    @Deprecated
    public abstract Optional<String> keyspace();

    public abstract Optional<CassandraCredentialsConfig> credentials();

    /**
     * A boolean declaring whether or not to use ssl to communicate with cassandra.
     * This configuration value is deprecated in favor of using sslConfiguration.  If
     * true, read in trust and key store information from system properties unless
     * the sslConfiguration object is specified.
     *
     * @deprecated Use {@link #sslConfiguration()} instead.
     */
    @Deprecated
    public abstract Optional<Boolean> ssl();

    /**
     * An object for specifying ssl configuration details.  The lack of existence of this
     * object implies ssl is not to be used to connect to cassandra.
     *
     * The existence of this object overrides any configuration made via the ssl config value.
     */
    public abstract Optional<SslConfiguration> sslConfiguration();

    public abstract int replicationFactor();

    /**
     * @deprecated Use {@link CassandraKeyValueServiceRuntimeConfig#mutationBatchCount()} to make this value
     * live-reloadable.
     */
    @SuppressWarnings("DeprecatedIsStillUsed")
    @Value.Default
    @Deprecated
    public int mutationBatchCount() {
        return 5000;
    }

    /**
     * @deprecated Use {@link CassandraKeyValueServiceRuntimeConfig#mutationBatchSizeBytes()} to make this value
     * live-reloadable.
     */
    @SuppressWarnings("DeprecatedIsStillUsed")
    @Value.Default
    @Deprecated
    public int mutationBatchSizeBytes() {
        return 4 * 1024 * 1024;
    }

    /**
     * @deprecated Use {@link CassandraKeyValueServiceRuntimeConfig#fetchBatchCount()} to make this value
     * live-reloadable.
     */
    @SuppressWarnings("DeprecatedIsStillUsed")
    @Value.Default
    @Deprecated
    public int fetchBatchCount() {
        return 5000;
    }

    @Value.Default
    public boolean ignoreNodeTopologyChecks() {
        return false;
    }

    @Value.Default
    public boolean ignoreInconsistentRingChecks() {
        return false;
    }

    @Value.Default
    public boolean ignoreDatacenterConfigurationChecks() {
        return false;
    }

    @Value.Default
    public boolean ignorePartitionerChecks() {
        return false;
    }

    @Value.Default
    public boolean autoRefreshNodes() {
        return true;
    }

    @Value.Default
    public boolean clusterMeetsNormalConsistencyGuarantees() {
        return true;
    }

    /**
     * This is how long we will wait when we first open a socket to the cassandra server.
     * This should be long enough to enough to handle cross data center latency, but short enough
     * that it will fail out quickly if it is clear we can't reach that server.
     */
    @Value.Default
    public int socketTimeoutMillis() {
        return 2 * 1000;
    }

    /**
     * Socket timeout is a java side concept.  This the maximum time we will block on a network
     * read without the server sending us any bytes.  After this time a {@link SocketTimeoutException}
     * will be thrown.  All cassandra reads time out at less than this value so we shouldn't see
     * it very much (10s by default).
     */
    @Value.Default
    public int socketQueryTimeoutMillis() {
        return 62 * 1000;
    }

    @Value.Default
    public int cqlPoolTimeoutMillis() {
        return 20 * 1000;
    }

    @Value.Default
    public int schemaMutationTimeoutMillis() {
        return 120 * 1000;
    }

    @Value.Default
    public int rangesConcurrency() {
        return 32;
    }

    @Value.Default
    public boolean scyllaDb() {
        return false;
    }

    /**
     * Obsolete value, replaced by {@link SweepConfig#readLimit}.
     *
     * @deprecated this parameter is unused and should be removed from the configuration
     */
    @SuppressWarnings("DeprecatedIsStillUsed") // Used by immutable copy of this file
    @Value.Default
    @Deprecated
    public Integer timestampsGetterBatchSize() {
        return 1_000;
    }

    /**
     * @deprecated Use {@link CassandraKeyValueServiceRuntimeConfig#sweepReadThreads()} to make this value
     * live-reloadable.
     */
    @Value.Default
    @Deprecated
    public Integer sweepReadThreads() {
        return AtlasDbConstants.DEFAULT_SWEEP_CASSANDRA_READ_THREADS;
    }

    public abstract Optional<CassandraJmxCompactionConfig> jmx();

    @Override
    public final String type() {
        return TYPE;
    }

    @Override
    @Value.Default
    public int concurrentGetRangesThreadPoolSize() {
        return poolSize() * servers().size();
    }

    @JsonIgnore
    @Value.Derived
    public boolean usingSsl() {
        return ssl().orElse(sslConfiguration().isPresent());
    }

    @Value.Check
    protected final void check() {
        Preconditions.checkState(!servers().isEmpty(), "'servers' must have at least one entry");
        for (InetSocketAddress addr : servers()) {
            Preconditions.checkState(addr.getPort() > 0, "each server must specify a port ([host]:[port])");
        }
        double evictionCheckProportion = proportionConnectionsToCheckPerEvictionRun();
        Preconditions.checkArgument(evictionCheckProportion > 0.01 && evictionCheckProportion <= 1,
                "'proportionConnectionsToCheckPerEvictionRun' must be between 0.01 and 1");
    }
}
