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

import java.util.Set;

import org.immutables.value.Value;

import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.base.Preconditions;
import com.palantir.atlasdb.spi.KeyValueServiceConfig;

@JsonDeserialize(as = ImmutableCassandraKeyValueServiceConfig.class)
@JsonSerialize(as = ImmutableCassandraKeyValueServiceConfig.class)
@JsonTypeName(CassandraKeyValueServiceConfig.TYPE)
@Value.Immutable
public abstract class CassandraKeyValueServiceConfig implements KeyValueServiceConfig {
    
    public static final String TYPE = "cassandra";

    public abstract Set<String> servers();
    
    public abstract int port();
    
    @Value.Default
    public int poolSize() {
        return 20;
    }
    
    @Value.Default
    public String keyspace() {
        return "atlasdb";
    }
    
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
    public int fetchBatchCount() {
        return 5000;
    }
    
    @Value.Default
    public boolean safetyDisabled() {
        return false;
    }
    
    @Value.Default
    public boolean autoRefreshNodes() {
        return true;
    }

    @Override
    public final String type() {
        return TYPE;
    }
    
    @Value.Check
    protected final void check() {
        Preconditions.checkState(!servers().isEmpty(), "'servers' must have at least one entry");
    }

}
