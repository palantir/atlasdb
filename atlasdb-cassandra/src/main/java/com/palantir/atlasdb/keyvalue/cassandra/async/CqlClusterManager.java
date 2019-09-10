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

package com.palantir.atlasdb.keyvalue.cassandra.async;

import java.io.Closeable;
import java.net.InetSocketAddress;
import java.util.Set;

import org.immutables.value.Value;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfig;

public interface CqlClusterManager {
    @Value.Immutable
    interface UniqueCassandraCluster {
        @Value.Parameter
        Set<InetSocketAddress> servers();
    }

    @Value.Immutable
    interface CassandraClusterSession extends Closeable {
        @Value.Parameter
        Cluster cluster();
        @Value.Parameter
        Session session();
        @Value.Parameter
        String clusterSessionName();

        default void close() {
            session().close();
            session().close();
        }
    }

    CqlClusterClient createCqlClusterClient(CassandraKeyValueServiceConfig config, Set<InetSocketAddress> servers);
}
