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

    //    public static abstract class Visitor<T> {
    //        protected Set<InetSocketAddress> thriftAddresses;
    //        protected Set<InetSocketAddress> maybeCalAddresses;
    //
    //
    //        public final void setThriftPorts(Set<InetSocketAddress> thriftAddresses) {
    //            this.thriftAddresses = thriftAddresses;
    //        }
    //        public final void setCQLPorts(Set<InetSocketAddress> cqlPorts) {
    //            maybeCalAddresses = cqlPorts;
    //        }
    //
    //        public abstract T result();
    //    }

    public interface Visitor<T> extends BiFunction<Set<InetSocketAddress>, Optional<Set<InetSocketAddress>>, T> {

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
        <T> T visit(Visitor<T> visitor);

        int numberOfHosts();

        void check();
    }

    @Value.Immutable
    @JsonDeserialize(as = ImmutableDefaultCassandraServersCqlDisabledConfig.class)
    @JsonSerialize(as = ImmutableDefaultCassandraServersCqlDisabledConfig.class)
    @JsonTypeName(DefaultCassandraServersCqlDisabledConfig.TYPE)
    public abstract static class DefaultCassandraServersCqlDisabledConfig implements CassandraServersConfig {
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
    @JsonDeserialize(as = ImmutableThriftOnlyCassandraServersConfig.class)
    @JsonSerialize(as = ImmutableThriftOnlyCassandraServersConfig.class)
    @JsonTypeName(ThriftOnlyCassandraServersConfig.TYPE)
    public abstract static class ThriftOnlyCassandraServersConfig implements CassandraServersConfig {
        public static final String TYPE = "thriftOnly";

        @Override
        public final <T> T visit(Visitor<T> visitor) {
            return visitor.apply(thrift(), Optional.empty());
        }

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

        @JsonProperty
        public abstract Set<InetSocketAddress> thrift();

        @JsonProperty
        @Value.Default
        String type() {
            return TYPE;
        }
    }

    @JsonDeserialize(as = ImmutableCqlCapableCassandraServersConfig.class)
    @JsonSerialize(as = ImmutableCqlCapableCassandraServersConfig.class)
    @JsonTypeName(CqlCapableCassandraServersConfig.TYPE)
    @Value.Immutable
    public abstract static class CqlCapableCassandraServersConfig implements CassandraServersConfig {
        public static final String TYPE = "cqlCapable";

        @Override
        public final <T> T visit(Visitor<T> visitor) {
            return visitor.apply(thrift(), Optional.of(cql()));
        }

        // TODO (OStevan): this is a temp solution before implementing the full thing
        @Override
        public int numberOfHosts() {
            return thrift().size();
        }

        @Override
        public void check() {
            // TODO (OStevan): still to bea updated with new format
            Preconditions.checkState(!thrift().isEmpty(), "there should be at least one thrift capable entry");
            Preconditions.checkState(thrift().size() == cql().size(),
                    "there should be the same number of CQL and Thrift entries");
            for (InetSocketAddress addr : thrift()) {
                Preconditions.checkState(addr.getPort() > 0, "each server must specify a port ([host]:[port])");
            }
            for (InetSocketAddress addr : cql()) {
                Preconditions.checkState(addr.getPort() > 0, "each server must specify a port ([host]:[port])");
            }
        }

        @JsonProperty
        @Value.Default
        String type() {
            return TYPE;
        }

        @JsonProperty
        public abstract Set<InetSocketAddress> thrift();

        @JsonProperty
        public abstract Set<InetSocketAddress> cql();
    }
}
