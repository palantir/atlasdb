/*
 * (c) Copyright 2019 Palantir Technologies Inc. All rights reserved.
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


import java.net.InetSocketAddress;
import java.util.Set;

import org.immutables.value.Value;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.annotation.JsonValue;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.collect.ImmutableSet;

public final class CassandraServersConfigs {
    protected CassandraServersConfigs() {

    }

    @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type",
            defaultImpl = DefaultCassandraServersCqlDisabledConfig.class)
    @JsonSubTypes(
            {
                    @JsonSubTypes.Type(value = DefaultCassandraServersCqlDisabledConfig.class,
                            name = DefaultCassandraServersCqlDisabledConfig.TYPE),
                    @JsonSubTypes.Type(value = ImmutableExplicitCassandraServersCqlDisabledConfig.class,
                            name = ExplicitCassandraServersCqlDisabledConfig.TYPE),
                    @JsonSubTypes.Type(value = ImmutableCassandraServersCqlEnabledConfig.class,
                            name = CassandraServersCqlEnabledConfig.TYPE)
            }
    )
    public interface CassandraServersConfig {
        Set<InetSocketAddress> thrift();
        Set<InetSocketAddress> cql();
        String type();
    }

    @JsonDeserialize(as = DefaultCassandraServersCqlDisabledConfig.class)
    @JsonTypeName(DefaultCassandraServersCqlDisabledConfig.TYPE)
    public static class DefaultCassandraServersCqlDisabledConfig implements CassandraServersConfig {
        public static final String TYPE = "default";

        private Set<InetSocketAddress> thriftServers;

        @JsonCreator
        public DefaultCassandraServersCqlDisabledConfig(InetSocketAddress... thriftServers) {
            this.thriftServers = ImmutableSet.copyOf(thriftServers);
        }

        @JsonCreator
        public DefaultCassandraServersCqlDisabledConfig(Iterable<InetSocketAddress> thriftServers) {
            this.thriftServers = ImmutableSet.copyOf(thriftServers);
        }

        @Override
        @JsonValue
        public Set<InetSocketAddress> thrift() {
            return thriftServers;
        }

        @Override
        public Set<InetSocketAddress> cql() {
            return ImmutableSet.of();
        }

        @Override
        public String type() {
            return TYPE;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj instanceof DefaultCassandraServersCqlDisabledConfig) {
                return thriftServers.equals(((DefaultCassandraServersCqlDisabledConfig) obj).thriftServers);
            }
            return false;
        }

        @Override
        public int hashCode() {
            return super.hashCode();
        }
    }


    @JsonDeserialize(as = ImmutableExplicitCassandraServersCqlDisabledConfig.class)
    @JsonSerialize(as = ImmutableExplicitCassandraServersCqlDisabledConfig.class)
    @JsonTypeName(ExplicitCassandraServersCqlDisabledConfig.TYPE)
    @Value.Immutable
    public abstract static class ExplicitCassandraServersCqlDisabledConfig implements CassandraServersConfig {
        public static final String TYPE = "cqlDisabled";

        @Override
        public Set<InetSocketAddress> cql() {
            return ImmutableSet.of();
        }

        @JsonProperty
        @Override
        public String type() {
            return TYPE;
        }
    }

    @JsonDeserialize(as = ImmutableCassandraServersCqlEnabledConfig.class)
    @JsonSerialize(as = ImmutableCassandraServersCqlEnabledConfig.class)
    @JsonTypeName(CassandraServersCqlEnabledConfig.TYPE)
    @Value.Immutable
    public abstract static class CassandraServersCqlEnabledConfig implements CassandraServersConfig {
        public static final String TYPE = "cqlEnabled";


        @JsonProperty
        @Override
        public String type() {
            return TYPE;
        }
    }
}
