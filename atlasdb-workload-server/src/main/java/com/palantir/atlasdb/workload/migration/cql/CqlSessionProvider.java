/*
 * (c) Copyright 2024 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.workload.migration.cql;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfigs;
import com.palantir.atlasdb.cassandra.CassandraServersConfigs;
import com.palantir.atlasdb.keyvalue.cassandra.async.client.creation.ClusterFactory;
import com.palantir.atlasdb.keyvalue.cassandra.async.client.creation.ClusterFactory.CassandraClusterConfig;
import java.io.Closeable;
import java.net.InetSocketAddress;
import java.util.Set;

public final class CqlSessionProvider implements Closeable {
    private final Cluster cluster;

    public CqlSessionProvider(CassandraKeyValueServiceConfigs configs) {
        CassandraClusterConfig cassandraClusterConfig = CassandraClusterConfig.of(
                configs.installConfig(), configs.runtimeConfig().get());
        cluster = CassandraServersConfigs.getCqlCapableConfigIfValid(
                        configs.runtimeConfig().get().servers())
                .map(cqlCapableConfig -> {
                    Set<InetSocketAddress> servers = cqlCapableConfig.cqlHosts();
                    return new ClusterFactory(Cluster::builder).constructCluster(servers, cassandraClusterConfig);
                })
                .orElseThrow();
    }

    public Session getSession() {
        return cluster.connect();
    }

    @Override
    public void close() {
        cluster.close();
    }
}
