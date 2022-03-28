/*
 * (c) Copyright 2022 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.keyvalue.cassandra.pool;

import com.google.common.collect.ImmutableSet;
import com.palantir.logsafe.Preconditions;
import java.net.InetSocketAddress;
import java.util.Set;
import org.immutables.value.Value;

@Value.Immutable
public interface CassandraServer {
    @Value.Parameter
    String cassandraHostName();

    /**
     * {@code cassandraHostName()} with {@code reachableProxyIps()} form one reachable Cassandra server.
     * We maintain set of all IPs but do not create a client pool for each one of these.
     * */
    @Value.Parameter
    Set<InetSocketAddress> reachableProxyIps();

    /**
     * The only proxy that will be used to reach the Cassandra host.
     * */
    @Value.Lazy
    default InetSocketAddress proxy() {
        // we know the set of proxies contains at least one element
        return reachableProxyIps().stream().findFirst().orElseThrow();
    }

    @Value.Check
    default void check() {
        Preconditions.checkState(reachableProxyIps().size() > 0, "Must have at least one reachable IP.");
    }

    static CassandraServer of(InetSocketAddress addr) {
        return of(addr.getHostString(), addr);
    }

    static CassandraServer of(String hostName, InetSocketAddress addr) {
        return of(hostName, ImmutableSet.of(addr));
    }

    static CassandraServer of(String hostName, Set<InetSocketAddress> reachableProxies) {
        return ImmutableCassandraServer.of(hostName, reachableProxies);
    }

    static ImmutableCassandraServer.Builder builder() {
        return ImmutableCassandraServer.builder();
    }
}
