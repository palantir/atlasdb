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
import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import org.immutables.value.Value;

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

    private static final String SERVER_FORMAT_ERROR = "each server must specify a port ([host]:[port])";
    private static final String PORT_NUMBER_ERROR = "%s port number should be a positive number";

    public interface Visitor<T> extends BiFunction<Set<InetSocketAddress>, Optional<Set<InetSocketAddress>>, T> {
        @Override
        T apply(Set<InetSocketAddress> thriftServers, Optional<Set<InetSocketAddress>> maybeCqlServers);
    }

    @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type", defaultImpl = ImmutableDefaultConfig.class)
    @JsonSubTypes({
            @JsonSubTypes.Type(value = ImmutableDefaultConfig.class, name = DefaultConfig.TYPE),
            @JsonSubTypes.Type(value = ImmutableCqlCapableConfig.class, name = CqlCapableConfig.TYPE)
            })
    public interface CassandraServersConfig {
        <T> T visit(Visitor<T> visitor);

        int numberOfHosts();
    }

    @Value.Immutable
    @JsonDeserialize(as = ImmutableDefaultConfig.class)
    @JsonSerialize(as = ImmutableDefaultConfig.class)
    @JsonTypeName(DefaultConfig.TYPE)
    public abstract static class DefaultConfig implements CassandraServersConfig {
        static final String TYPE = "default";

        @JsonValue
        abstract Set<InetSocketAddress> thrift();

        @Override
        public int numberOfHosts() {
            return thrift().size();
        }

        @Value.Check
        final void check() {
            for (InetSocketAddress addr : thrift()) {
                Preconditions.checkState(addr.getPort() > 0, SERVER_FORMAT_ERROR);
            }
        }

        @Override
        public final <T> T visit(Visitor<T> visitor) {
            return visitor.apply(thrift(), Optional.empty());
        }
    }

    @JsonDeserialize(as = ImmutableCqlCapableConfig.class)
    @JsonSerialize(as = ImmutableCqlCapableConfig.class)
    @JsonTypeName(CqlCapableConfig.TYPE)
    @Value.Immutable
    public abstract static class CqlCapableConfig implements CassandraServersConfig {
        static final String TYPE = "cqlCapable";

        abstract Set<String> hosts();

        abstract int thriftPort();

        abstract int cqlPort();

        @Override
        public final int numberOfHosts() {
            return hosts().size();
        }

        @Value.Check
        final void check() {
            Preconditions.checkState(thriftPort() > 0, String.format(PORT_NUMBER_ERROR, "'thriftPort'"));
            Preconditions.checkState(cqlPort() > 0, String.format(PORT_NUMBER_ERROR, "'cqlPort'"));
        }

        @Override
        public final <T> T visit(Visitor<T> visitor) {
            return visitor.apply(
                    hosts().stream()
                            .map(host -> new InetSocketAddress(host, thriftPort()))
                            .collect(Collectors.toSet()),
                    Optional.of(hosts().stream()
                            .map(host -> new InetSocketAddress(host, cqlPort()))
                            .collect(Collectors.toSet())));
        }
    }
}
