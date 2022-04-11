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
import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

public final class CqlCluster implements Closeable {
    private final Cluster cluster;
    private final CassandraClusterConfig cassandraClusterConfig;
    private final Supplier<CassandraServersConfig> cassandraServersConfigSupplier;
    private final String keyspace;

    // VisibleForTesting
    public CqlCluster(
            Cluster cluster,
            CassandraClusterConfig cassandraClusterConfig,
            Supplier<CassandraServersConfig> cassandraServersConfigSupplier,
            String keyspace) {
        this.cluster = cluster;
        this.cassandraClusterConfig = cassandraClusterConfig;
        this.cassandraServersConfigSupplier = cassandraServersConfigSupplier;
        this.keyspace = keyspace;
    }

    public static CqlCluster create(
            CassandraClusterConfig cassandraClusterConfig,
            Supplier<CassandraServersConfig> cassandraServersConfigSupplier,
            String keyspace) {
        Cluster cluster = new ClusterFactory(Cluster::builder)
                .constructCluster(cassandraClusterConfig, cassandraServersConfigSupplier);
        return new CqlCluster(cluster, cassandraClusterConfig, cassandraServersConfigSupplier, keyspace);
    }

    @Override
    public void close() throws IOException {
        cluster.close();
    }

    public Map<InetSocketAddress, RangeSet<LightweightOppToken>> getTokenRanges(String tableName) {
        try (CqlSession session = new CqlSession(cluster.connect())) {
            return new TokenRangeFetcher(session, keyspace, cassandraServersConfigSupplier).getTokenRange(tableName);
        }
    }

    public Map<String, Map<InetSocketAddress, RangeSet<LightweightOppToken>>> getTransactionsTableRangesForRepair(
            List<TransactionsTableInteraction> transactionsTableInteractions) {
        try (CqlSession session = new CqlSession(cluster.connect())) {
            return new RepairRangeFetcher(session, keyspace, cassandraServersConfigSupplier)
                    .getTransactionTableRangesForRepair(transactionsTableInteractions);
        }
    }

    public void abortTransactions(long timestamp, List<TransactionsTableInteraction> transactionsTableInteractions) {
        try (CqlSession session = new CqlSession(cluster.connect())) {
            new TransactionAborter(session, keyspace).abortTransactions(timestamp, transactionsTableInteractions);
        }
    }
}
