/*
 * (c) Copyright 2021 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.cassandra.backup;

import com.datastax.driver.core.Cluster;
import com.google.common.collect.RangeSet;
import com.palantir.atlasdb.cassandra.CassandraServersConfigs.CassandraServersConfig;
import com.palantir.atlasdb.cassandra.backup.transaction.TransactionsTableInteraction;
import com.palantir.atlasdb.keyvalue.cassandra.LightweightOppToken;
import com.palantir.atlasdb.keyvalue.cassandra.async.client.creation.ClusterFactory;
import com.palantir.atlasdb.keyvalue.cassandra.async.client.creation.ClusterFactory.CassandraClusterConfig;
import com.palantir.atlasdb.timelock.api.Namespace;
import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;

public final class CqlCluster implements Closeable {
    private final Cluster cluster;
    private final CassandraServersConfig cassandraServersConfig;

    // VisibleForTesting
    public CqlCluster(Cluster cluster, CassandraServersConfig cassandraServersConfig) {
        this.cluster = cluster;
        this.cassandraServersConfig = cassandraServersConfig;
    }

    public static CqlCluster create(
            CassandraClusterConfig cassandraClusterConfig, CassandraServersConfig cassandraServersConfig) {
        Cluster cluster =
                new ClusterFactory(Cluster::builder).constructCluster(cassandraClusterConfig, cassandraServersConfig);
        return new CqlCluster(cluster, cassandraServersConfig);
    }

    // TODO(gs): method for coord service

    @Override
    public void close() throws IOException {
        cluster.close();
    }

    public Map<InetSocketAddress, RangeSet<LightweightOppToken>> getTokenRanges(Namespace namespace, String tableName) {
        try (CqlSession session = new CqlSession(cluster.connect())) {
            return new TokenRangeFetcher(session, namespace, cassandraServersConfig).getTokenRange(tableName);
        }
    }

    public Map<String, Map<InetSocketAddress, RangeSet<LightweightOppToken>>> getTransactionsTableRangesForRepair(
            Namespace namespace, List<TransactionsTableInteraction> transactionsTableInteractions) {
        try (CqlSession session = new CqlSession(cluster.connect())) {
            return new RepairRangeFetcher(session, namespace, cassandraServersConfig)
                    .getTransactionTableRangesForRepair(transactionsTableInteractions);
        }
    }

    public void abortTransactions(
            Namespace namespace, long timestamp, List<TransactionsTableInteraction> transactionsTableInteractions) {
        try (CqlSession session = new CqlSession(cluster.connect())) {
            new TransactionAborter(session, namespace).abortTransactions(timestamp, transactionsTableInteractions);
        }
    }
}
