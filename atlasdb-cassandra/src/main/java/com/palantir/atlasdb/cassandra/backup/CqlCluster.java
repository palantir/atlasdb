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

import com.google.common.collect.RangeSet;
import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfig;
import com.palantir.atlasdb.cassandra.backup.transaction.TransactionsTableInteraction;
import com.palantir.atlasdb.keyvalue.cassandra.LightweightOppToken;
import com.palantir.atlasdb.keyvalue.cassandra.async.client.creation.ClusterFactory;
import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;

public final class CqlCluster implements Closeable {
    private final Cluster cluster;
    private final CassandraKeyValueServiceConfig config;

    // VisibleForTesting
    public CqlCluster(Cluster cluster, CassandraKeyValueServiceConfig config) {
        this.cluster = cluster;
        this.config = config;
    }

    public static CqlCluster create(CassandraKeyValueServiceConfig config) {
        Cluster cluster = new ClusterFactory(Cluster::builder).constructCluster(config);
        return new CqlCluster(cluster, config);
    }

    @Override
    public void close() throws IOException {
        cluster.close();
    }

    public Map<InetSocketAddress, RangeSet<LightweightOppToken>> getTokenRanges(String tableName) {
        try (CqlSession session = new CqlSession(cluster.connect())) {
            return new TokenRangeFetcher(session, config).getTokenRange(tableName);
        }
    }

    public Map<String, Map<InetSocketAddress, RangeSet<LightweightOppToken>>> getTransactionsTableRangesForRepair(
            List<TransactionsTableInteraction> transactionsTableInteractions) {
        try (CqlSession session = new CqlSession(cluster.connect())) {
            return new RepairRangeFetcher(session, config)
                    .getTransactionTableRangesForRepair(transactionsTableInteractions);
        }
    }

    public void abortTransactions(long timestamp, List<TransactionsTableInteraction> transactionsTableInteractions) {
        try (CqlSession session = new CqlSession(cluster.connect())) {
            new TransactionAborter(session, config).abortTransactions(timestamp, transactionsTableInteractions);
        }
    }
}
