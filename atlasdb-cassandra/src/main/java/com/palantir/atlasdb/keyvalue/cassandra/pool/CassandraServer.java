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

import java.net.InetSocketAddress;
import org.immutables.value.Value;

@Value.Immutable
public interface CassandraServer {
    @Value.Parameter
    InetSocketAddress cassandraHostAddress();

    /**
     * The proxy that will be used to reach the Cassandra host.
     * */
    @Value.Parameter
    InetSocketAddress proxy();

    static CassandraServer from(InetSocketAddress addr) {
        return CassandraServer.builder().cassandraHostAddress(addr).proxy(addr).build();
    }

    static ImmutableCassandraServer.Builder builder() {
        return ImmutableCassandraServer.builder();
    }
}
