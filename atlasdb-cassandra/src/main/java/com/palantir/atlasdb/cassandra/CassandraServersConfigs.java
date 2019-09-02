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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.google.common.collect.ImmutableSet;

public final class CassandraServersConfigs {
    protected CassandraServersConfigs() {

    }

    @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "servers",
            defaultImpl = LegacyCassandraServersConfig.class)
    public interface CassandraServersConfig {
        Set<InetSocketAddress> thrift();
    }

    @JsonDeserialize(as = LegacyCassandraServersConfig.class)
    @JsonTypeName(LegacyCassandraServersConfig.TYPE)
    public static class LegacyCassandraServersConfig implements CassandraServersConfig {
        public static final String TYPE = "deprecated";

        private Set<InetSocketAddress> thriftServers;

        public LegacyCassandraServersConfig(InetSocketAddress thriftAddress) {
            this.thriftServers = ImmutableSet.of(thriftAddress);
        }

        public LegacyCassandraServersConfig(InetSocketAddress... thriftServers) {
            this.thriftServers = ImmutableSet.copyOf(thriftServers);
        }

        @JsonCreator
        public LegacyCassandraServersConfig(Iterable<InetSocketAddress> thriftServers) {
            this.thriftServers = ImmutableSet.copyOf(thriftServers);
        }

        @Override
        public Set<InetSocketAddress> thrift() {
            return thriftServers;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj instanceof LegacyCassandraServersConfig) {
                return thriftServers.equals(((LegacyCassandraServersConfig) obj).thriftServers);
            }
            return false;
        }

        @Override
        public int hashCode() {
            return super.hashCode();
        }
    }
}
