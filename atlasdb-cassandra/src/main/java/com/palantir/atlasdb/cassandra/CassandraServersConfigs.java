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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.annotation.JsonValue;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.collect.ImmutableSet;

public final class CassandraServersConfigs {
    private CassandraServersConfigs() {

    }

    @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type",
            defaultImpl = ImmutableDefaultCassandraServersCqlDisabledConfig.class)
    @JsonSubTypes(
            {
                    @JsonSubTypes.Type(value = ImmutableDefaultCassandraServersCqlDisabledConfig.class,
                            name = DefaultCassandraServersCqlDisabledConfig.TYPE),
                    @JsonSubTypes.Type(value = ImmutableThriftOnlyCassandraServersConfig.class,
                            name = ThriftOnlyCassandraServersConfig.TYPE),
                    @JsonSubTypes.Type(value = ImmutableCqlCapableCassandraServersConfig.class,
                            name = CqlCapableCassandraServersConfig.TYPE)
            }
    )
    public interface CassandraServersConfig {
        Set<InetSocketAddress> thrift();
        Set<InetSocketAddress> cql();
        String type();
    }

    @Value.Immutable
    @JsonDeserialize(as = ImmutableDefaultCassandraServersCqlDisabledConfig.class)
    @JsonSerialize(as = ImmutableDefaultCassandraServersCqlDisabledConfig.class)
    @JsonTypeName(DefaultCassandraServersCqlDisabledConfig.TYPE)
    public interface DefaultCassandraServersCqlDisabledConfig extends CassandraServersConfig {
        String TYPE = "default";

        @JsonValue
        @Override
        Set<InetSocketAddress> thrift();

        @Override
        default Set<InetSocketAddress> cql() {
            return null;
        }

        @Override
        default String type() {
            return TYPE;
        }
    }


    @JsonDeserialize(as = ImmutableThriftOnlyCassandraServersConfig.class)
    @JsonSerialize(as = ImmutableThriftOnlyCassandraServersConfig.class)
    @JsonTypeName(ThriftOnlyCassandraServersConfig.TYPE)
    @Value.Immutable
    public interface ThriftOnlyCassandraServersConfig extends CassandraServersConfig {
        String TYPE = "thriftOnly";

        @Override
        default Set<InetSocketAddress> cql() {
            return ImmutableSet.of();
        }

        @JsonProperty
        @Override
        default String type() {
            return TYPE;
        }
    }

    @JsonDeserialize(as = ImmutableCqlCapableCassandraServersConfig.class)
    @JsonSerialize(as = ImmutableCqlCapableCassandraServersConfig.class)
    @JsonTypeName(CqlCapableCassandraServersConfig.TYPE)
    @Value.Immutable
    public interface CqlCapableCassandraServersConfig extends CassandraServersConfig {
        String TYPE = "cqlCapable";

        @JsonProperty
        @Override
        default String type() {
            return TYPE;
        }
    }
}
