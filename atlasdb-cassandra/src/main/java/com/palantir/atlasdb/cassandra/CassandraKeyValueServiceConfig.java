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
package com.palantir.atlasdb.cassandra;

import java.net.InetSocketAddress;
import java.net.SocketTimeoutException;
import java.util.Set;

import org.immutables.value.Value;

import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.palantir.atlasdb.spi.KeyValueServiceConfig;

@JsonDeserialize(as = ImmutableCassandraKeyValueServiceConfig.class)
@JsonSerialize(as = ImmutableCassandraKeyValueServiceConfig.class)
@JsonTypeName(CassandraKeyValueServiceConfig.TYPE)
@Value.Immutable
public abstract class CassandraKeyValueServiceConfig implements KeyValueServiceConfig {

    public static final String TYPE = "cassandra";

    public abstract Set<InetSocketAddress> servers();

    @Value.Default
    public int poolSize() {
        return 20;
    }

    public abstract String keyspace();

    public abstract Optional<CassandraCredentialsConfig> credentials();

    public abstract boolean ssl();

    public abstract int replicationFactor();

    @Value.Default
    public int mutationBatchCount() {
        return 5000;
    }

    @Value.Default
    public int mutationBatchSizeBytes() {
        return 4 * 1024 * 1024;
    }

    @Value.Default
    public int fetchBatchCount() { return 5000; }

    @Value.Default
    public boolean safetyDisabled() {
        return false;
    }

    @Value.Default
    public boolean autoRefreshNodes() {
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
        return 5 * 1000;
    }

    @Value.Default
    public int schemaMutationTimeoutMillis() { return 60 * 1000; }

    @Value.Default
    public int rangesConcurrency() {
        return 64;
    }

    public abstract Optional<CassandraJmxCompactionConfig> jmx();

    @Override
    public final String type() {
        return TYPE;
    }

    @Value.Check
    protected final void check() {
        Preconditions.checkState(!servers().isEmpty(), "'servers' must have at least one entry");
        for (InetSocketAddress addr : servers()) {
            Preconditions.checkState(addr.getPort() > 0, "each server must specify a port ([host]:[port])");
        }
        Preconditions.checkNotNull(keyspace(), "'keyspace' must be specified");
    }
}
