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

import com.google.common.base.Preconditions;
import java.net.InetSocketAddress;
import java.util.List;
import org.immutables.value.Value;

@Value.Immutable
public interface CassandraServer {
    @Value.Parameter
    InetSocketAddress cassandraHostAddress();

    @Value.Auxiliary
    List<InetSocketAddress> reachableProxyIps();

    @Value.Lazy
    default InetSocketAddress proxy() {
        return reachableProxyIps().get(0);
    }

    @Value.Check
    default void check() {
        Preconditions.checkState(reachableProxyIps().size() > 0, "Must have at least one reachable IP.");
    }

    static CassandraServer from(InetSocketAddress addr) {
        return CassandraServer.builder()
                .cassandraHostAddress(addr)
                .addReachableProxyIps(addr)
                .build();
    }

    static ImmutableCassandraServer.Builder builder() {
        return ImmutableCassandraServer.builder();
    }
}
