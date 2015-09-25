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
package com.palantir.atlasdb.cassandra.spi;

import java.util.Set;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.spi.KeyValueServiceConfig;

@JsonTypeName(CassandraKeyValueServiceConfig.TYPE)
public final class CassandraKeyValueServiceConfig implements KeyValueServiceConfig {
    
    public static final String TYPE = "cassandra";

    private final Set<String> servers;
    private final int port;
    private final int poolSize;
    private final String keyspace;
    private final boolean ssl;
    private final int replicationFactor;
    private final int mutationBatchCount;
    private final int mutationBatchSizeBytes;
    private final int fetchBatchCount;
    private final boolean safetyDisabled;
    private final boolean autoRefreshNodes;

    public CassandraKeyValueServiceConfig(
            @JsonProperty("servers") Set<String> servers,
            @JsonProperty("port") int port,
            @JsonProperty("poolSize") Optional<Integer> poolSize,
            @JsonProperty("keyspace") Optional<String> keyspace,
            @JsonProperty("ssl") boolean ssl,
            @JsonProperty("replicationFactor") int replicationFactor,
            @JsonProperty("mutationBatchCount") Optional<Integer> mutationBatchCount,
            @JsonProperty("mutationBatchSizeBytes") Optional<Integer> mutationBatchSizeBytes,
            @JsonProperty("fetchBatchCount") Optional<Integer> fetchBatchCount,
            @JsonProperty("safetyDisabled") Optional<Boolean> safetyDisabled,
            @JsonProperty("autoRefreshNodes") Optional<Boolean> autoRefreshNodes) {
        this.servers = ImmutableSet.copyOf(servers);
        this.port = port;
        this.poolSize = poolSize.or(20);
        this.keyspace = keyspace.or("atlasdb");
        this.ssl = ssl;
        this.replicationFactor = replicationFactor;
        this.mutationBatchCount = mutationBatchCount.or(5000);
        this.mutationBatchSizeBytes = mutationBatchSizeBytes.or(4 * 1024 * 1024);
        this.fetchBatchCount = fetchBatchCount.or(5000);
        this.safetyDisabled = safetyDisabled.or(false);
        this.autoRefreshNodes = autoRefreshNodes.or(true);
    }

    public Set<String> getServers() {
        return servers;
    }

    public int getPort() {
        return port;
    }

    public int getPoolSize() {
        return poolSize;
    }

    public String getKeyspace() {
        return keyspace;
    }

    public boolean isSsl() {
        return ssl;
    }

    public int getReplicationFactor() {
        return replicationFactor;
    }

    public int getMutationBatchCount() {
        return mutationBatchCount;
    }

    public int getMutationBatchSizeBytes() {
        return mutationBatchSizeBytes;
    }

    public int getFetchBatchCount() {
        return fetchBatchCount;
    }

    public boolean getSafetyDisabled() {
        return safetyDisabled;
    }

    public boolean getAutoRefreshNodes() {
        return autoRefreshNodes;
    }
    
    @Override
    public String getType() {
        return TYPE;
    }

}
