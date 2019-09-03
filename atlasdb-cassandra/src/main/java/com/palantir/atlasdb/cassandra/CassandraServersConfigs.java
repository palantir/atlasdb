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
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import org.immutables.value.Value;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.annotation.JsonValue;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.palantir.logsafe.Preconditions;

public final class CassandraServersConfigs {
    private CassandraServersConfigs() {

    }

    public static DefaultConfig defaultConfig(Iterable<InetSocketAddress> thriftServers) {
        return ImmutableDefaultConfig.builder().addAllThrift(thriftServers).build();
    }

    public static DefaultConfig defaultConfig(InetSocketAddress thriftServers) {
        return ImmutableDefaultConfig.builder().addThrift(thriftServers).build();
    }

    public static ThriftOnlyConfig thriftOnlyConfig(Iterable<InetSocketAddress> thriftServers) {
        return ImmutableThriftOnlyConfig.builder().addAllThrift(thriftServers).build();
    }

    public static ThriftOnlyConfig thriftOnlyConfig(InetSocketAddress thriftServers) {
        return ImmutableThriftOnlyConfig.builder().addThrift(thriftServers).build();
    }

    public static CqlCapableConfig cqlCapableConfig(CqlCapableConfig.CqlCapableServer... servers) {
        return ImmutableCqlCapableConfig.builder().addHosts(servers).build();
    }

    public static CqlCapableConfig cqlCapableConfig(Iterable<CqlCapableConfig.CqlCapableServer> servers) {
        return ImmutableCqlCapableConfig.builder().addAllHosts(servers).build();
    }


    public static CqlCapableConfig.CqlCapableServer cqlCapableServer(String hostname, int thriftPort, int cqlPort) {
        Preconditions.checkState(thriftPort > 0, "Thrift port should be a positive number");
        Preconditions.checkState(cqlPort > 0, "CQL port should be a positive number");
        return ImmutableCqlCapableServer
                .builder()
                .hostname(hostname)
                .thriftPort(thriftPort)
                .cqlPort(cqlPort).build();
    }

    public interface Visitor<T> extends BiFunction<Set<InetSocketAddress>, Optional<Set<InetSocketAddress>>, T> {
    }


    @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type",
            defaultImpl = ImmutableDefaultConfig.class)
    @JsonSubTypes(
            {
                    @JsonSubTypes.Type(value = ImmutableDefaultConfig.class,
                            name = DefaultConfig.TYPE),
                    @JsonSubTypes.Type(value = ImmutableThriftOnlyConfig.class,
                            name = ThriftOnlyConfig.TYPE),
                    @JsonSubTypes.Type(value = ImmutableCqlCapableConfig.class,
                            name = CqlCapableConfig.TYPE)
            }
    )
    public interface CassandraServersConfig {
        <T> T visit(Visitor<T> visitor);

        int numberOfHosts();

        void check();
    }

    @Value.Immutable
    @JsonDeserialize(as = ImmutableDefaultConfig.class)
    @JsonSerialize(as = ImmutableDefaultConfig.class)
    @JsonTypeName(DefaultConfig.TYPE)
    public abstract static class DefaultConfig implements CassandraServersConfig {
        public static final String TYPE = "default";

        @Override
        public final <T> T visit(Visitor<T> visitor) {
            return visitor.apply(thrift(), Optional.empty());
        }

        @JsonValue
        public abstract Set<InetSocketAddress> thrift();

        @Override
        public int numberOfHosts() {
            return thrift().size();
        }

        @Override
        public void check() {
            Preconditions.checkState(!thrift().isEmpty(), "'servers' must have at least one entry");
            for (InetSocketAddress addr : thrift()) {
                Preconditions.checkState(addr.getPort() > 0, "each server must specify a port ([host]:[port])");
            }
        }
    }


    @Value.Immutable
    @JsonDeserialize(as = ImmutableThriftOnlyConfig.class)
    @JsonSerialize(as = ImmutableThriftOnlyConfig.class)
    @JsonTypeName(ThriftOnlyConfig.TYPE)
    public abstract static class ThriftOnlyConfig implements CassandraServersConfig {
        public static final String TYPE = "thriftOnly";

        @Override
        public final <T> T visit(Visitor<T> visitor) {
            return visitor.apply(thrift(), Optional.empty());
        }

        @Override
        public final int numberOfHosts() {
            return thrift().size();
        }

        @Override
        public final void check() {
            Preconditions.checkState(!thrift().isEmpty(), "'servers' must have at least one entry");
            for (InetSocketAddress addr : thrift()) {
                Preconditions.checkState(addr.getPort() > 0, "each server must specify a port ([host]:[port])");
            }
        }

        @JsonProperty
        public abstract Set<InetSocketAddress> thrift();

        @JsonProperty
        @Value.Default
        String type() {
            return TYPE;
        }
    }

    @JsonDeserialize(as = ImmutableCqlCapableConfig.class)
    @JsonSerialize(as = ImmutableCqlCapableConfig.class)
    @JsonTypeName(CqlCapableConfig.TYPE)
    @Value.Immutable
    public abstract static class CqlCapableConfig implements CassandraServersConfig {
        public static final String TYPE = "cqlCapable";

        @Value.Immutable
        @JsonDeserialize(as = ImmutableCqlCapableServer.class)
        @JsonSerialize(as = ImmutableCqlCapableServer.class)
        interface CqlCapableServer {

            @JsonProperty
            String hostname();
            @JsonProperty
            Integer thriftPort();

            @JsonProperty
            Integer cqlPort();

            default InetSocketAddress thriftServer() {
                return new InetSocketAddress(hostname(), thriftPort());
            }

            default InetSocketAddress cqlServer() {
                return new InetSocketAddress(hostname(), thriftPort());
            }
        }

        @Override
        public final <T> T visit(Visitor<T> visitor) {
            return visitor.apply(hosts().stream().map(CqlCapableServer::thriftServer).collect(Collectors.toSet()),
                    Optional.of(hosts().stream().map(CqlCapableServer::thriftServer).collect(Collectors.toSet())));
        }


        @Override
        public final int numberOfHosts() {
            return hosts().size();
        }

        @Override
        public final void check() {
            Preconditions.checkState(!hosts().isEmpty(), "there should be at least one thrift capable entry");
        }

        @JsonProperty
        @Value.Default
        String type() {
            return TYPE;
        }

        @JsonProperty
        public abstract List<CqlCapableServer> hosts();
    }
}
