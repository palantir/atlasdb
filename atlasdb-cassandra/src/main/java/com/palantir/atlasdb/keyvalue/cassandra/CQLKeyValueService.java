/**
 * Copyright 2015 Palantir Technologies
 *
 * Licensed under the BSD-3 License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://opensource.org/licenses/BSD-3-Clause
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.palantir.atlasdb.keyvalue.cassandra;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;

import org.apache.commons.lang.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.*;
import com.datastax.driver.core.ProtocolOptions.Compression;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.driver.core.exceptions.UnavailableException;
import com.datastax.driver.core.policies.*;
import com.google.common.base.*;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.*;
import com.google.common.collect.ImmutableMap.Builder;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfig;
import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfigManager;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.*;
import com.palantir.atlasdb.keyvalue.cassandra.CQLKeyValueServices.AllTimestampsCollector;
import com.palantir.atlasdb.keyvalue.cassandra.CQLKeyValueServices.Peer;
import com.palantir.atlasdb.keyvalue.cassandra.CQLKeyValueServices.StartTsResultsCollector;
import com.palantir.atlasdb.keyvalue.cassandra.CQLKeyValueServices.TransactionType;
import com.palantir.atlasdb.keyvalue.impl.AbstractKeyValueService;
import com.palantir.atlasdb.keyvalue.impl.Cells;
import com.palantir.atlasdb.keyvalue.impl.KeyValueServices;
import com.palantir.common.annotation.Idempotent;
import com.palantir.common.base.ClosableIterator;
import com.palantir.common.base.ClosableIterators;
import com.palantir.common.base.Throwables;
import com.palantir.common.concurrent.NamedThreadFactory;
import com.palantir.common.concurrent.PTExecutors;
import com.palantir.util.Visitor;
import com.palantir.util.paging.AbstractPagingIterable;
import com.palantir.util.paging.SimpleTokenBackedResultsPage;
import com.palantir.util.paging.TokenBackedBasicResultsPage;

public class CQLKeyValueService extends AbstractKeyValueService {
    private static final Logger log = LoggerFactory.getLogger(CQLKeyValueService.class);

    private Cluster cluster, longRunningQueryCluster;
    Session session;

    private Session longRunningQuerySession;

    final LoadingCache<String, PreparedStatement> statementCache = CacheBuilder.newBuilder()
            .build(new CacheLoader<String, PreparedStatement>() {
                @Override
                public PreparedStatement load(String query) {
                    return session.prepare(query);
                }
            });

    private final CassandraKeyValueServiceConfigManager configManager;
    private final CassandraJMXCompactionManager compactionManager;

    private ConsistencyLevel readConsistency = ConsistencyLevel.LOCAL_QUORUM;
    private final ConsistencyLevel writeConsistency = ConsistencyLevel.EACH_QUORUM;
    private final ConsistencyLevel deleteConsistency = ConsistencyLevel.ALL;

    public static CQLKeyValueService create(CassandraKeyValueServiceConfigManager configManager) {
        final CassandraKeyValueServiceConfig config = configManager.getConfig();
        Preconditions.checkArgument(!config.servers().isEmpty(), "servers set was empty");
        final CQLKeyValueService ret = new CQLKeyValueService(configManager);
        ret.initializeConnectionPool();
        ret.performInitialSetup();
        return ret;
    }

    private CQLKeyValueService(CassandraKeyValueServiceConfigManager configManager) {
        super(PTExecutors.newFixedThreadPool(configManager.getConfig().poolSize(), new NamedThreadFactory(
                "CQLKeyValueService", false)));
        this.configManager = configManager;
        this.compactionManager = CassandraJMXCompactionManager.newInstance(configManager.getConfig());
    }

    private void initializeConnectionPool() {
        final CassandraKeyValueServiceConfig config = configManager.getConfig();
        HashSet<InetSocketAddress> configuredHosts = Sets.newHashSet(Iterables.transform(config.servers(),
                        new Function<String, InetSocketAddress>() {
                            @Override
                            public InetSocketAddress apply(String server) {
                                return new InetSocketAddress(server, config.port());
                            }
                        })
        );
        Cluster.Builder clusterBuilder = Cluster.builder();
        clusterBuilder.addContactPointsWithPorts(configuredHosts);
        clusterBuilder.withClusterName("atlas_cassandra_cluster_" + config.keyspace()); // for JMX metrics
        clusterBuilder.withCompression(Compression.LZ4);

        if (config.ssl()) {
            clusterBuilder.withSSL();
        }

        PoolingOptions poolingOptions = new PoolingOptions();
        poolingOptions.setMaxRequestsPerConnection(HostDistance.LOCAL, config.poolSize());
        poolingOptions.setMaxRequestsPerConnection(HostDistance.REMOTE, config.poolSize());
        poolingOptions.setPoolTimeoutMillis(config.cqlPoolTimeoutMillis());
        clusterBuilder.withPoolingOptions(poolingOptions);

        // defaults for queries; can override on per-query basis
        QueryOptions queryOptions = new QueryOptions();
        queryOptions.setFetchSize(config.fetchBatchCount());
        clusterBuilder.withQueryOptions(queryOptions);

        // Refuse to talk to nodes twice as (latency-wise) slow as the best one, over a timescale of 100ms,
        // and every 10s try to re-evaluate ignored nodes performance by giving them queries again.
        // Note we are being purposely datacenter-irreverent here, instead relying on latency alone to approximate what DCAwareRR would do;
        // this is because DCs for Atlas are always quite latency-close and should be used this way, not as if we have some cross-country backup DC.
        LoadBalancingPolicy policy = LatencyAwarePolicy.builder(new RoundRobinPolicy()).build();

        // If user wants, do not automatically add in new nodes to pool (useful during DC migrations / rebuilds)
        if (!config.autoRefreshNodes()) {
            policy = new WhiteListPolicy(policy, configuredHosts);
        }

        // also try and select coordinators who own the data we're talking about to avoid an extra hop,
        // but also shuffle which replica we talk to for a load balancing that comes at the expense of less effective caching
        policy = new TokenAwarePolicy(policy, true);

        clusterBuilder.withLoadBalancingPolicy(policy);

        cluster = clusterBuilder.build();

        Metadata metadata;
        try {
            metadata = cluster.getMetadata(); // special; this is the first place we connect to
            // hosts, this is where people will see failures
        } catch (NoHostAvailableException e) {
            if (e.getMessage().contains("Unknown compression algorithm")) {
                clusterBuilder.withCompression(Compression.NONE);
                cluster = clusterBuilder.build();
                metadata = cluster.getMetadata();
            } else {
                throw e;
            }
        }

        session = cluster.connect();

        clusterBuilder.withSocketOptions(new SocketOptions().setReadTimeoutMillis(CassandraConstants.LONG_RUNNING_QUERY_SOCKET_TIMEOUT_MILLIS));
        longRunningQueryCluster = clusterBuilder.build();
        longRunningQuerySession = longRunningQueryCluster.connect();

        if (log.isInfoEnabled()) {
            StringBuilder hostInfo = new StringBuilder();
            for (Host host : metadata.getAllHosts()) {
                hostInfo.append(String.format(
                        "Datatacenter: %s; Host: %s; Rack: %s\n",
                        host.getDatacenter(),
                        host.getAddress(),
                        host.getRack()));
            }
            log.info(String.format(
                    "Initialized cassandra cluster using new API with hosts %s, seen keyspaces %s, cluster name %s",
                    hostInfo.toString(),
                    metadata.getKeyspaces(),
                    metadata.getClusterName()));
        }
    }

    @Override
    public void initializeFromFreshInstance() {
        // we already did our init in our factory method
    }

    @Override
    public void close() {
        log.info("Closing CQLKeyValueService");
        session.close();
        cluster.close();
        CQLKeyValueServices.traceRetrievalExec.shutdown();
        if(compactionManager != null) {
            compactionManager.close();
        }
        super.close();
    }

    private void performInitialSetup() {
        Metadata metadata = cluster.getMetadata();

        final CassandraKeyValueServiceConfig config = configManager.getConfig();
        String partitioner = metadata.getPartitioner();
        if (!config.safetyDisabled()) {
            Validate.isTrue(
                    CassandraConstants.ALLOWED_PARTITIONERS.contains(partitioner),
                    "partitioner is: " + partitioner);
        }


        Set<Peer> peers = CQLKeyValueServices.getPeers(session);

        boolean noDatacentersPresentInCluster = Iterables.all(peers, new Predicate<Peer>() {
            @Override
            public boolean apply(Peer peer) {
                return peer.data_center == null;
            }
        });

        boolean allNodesHaveSaneNumberOfVnodes = Iterables.all(peers, new Predicate<Peer>() {
            @Override
            public boolean apply(Peer peer) {
                return peer.tokens.size() > CassandraConstants.ABSOLUTE_MINIMUM_NUMBER_OF_TOKENS_PER_NODE;
            }
        });

        // node we're querying doesn't count itself as a peer
        if (peers.size() > 0 && !allNodesHaveSaneNumberOfVnodes) {
            throw new IllegalStateException("All nodes in cluster must have sane number of vnodes (or cluster must consist of a single node).");
        }

        Set<String> dcsInCluster = Sets.newHashSet();
        if (!noDatacentersPresentInCluster) {
            for (Peer peer: peers) {
                dcsInCluster.add(peer.data_center);
                if (peer.data_center == null) {
                    throw new IllegalStateException("Cluster should not mix datacenter-aware and non-datacenter-aware nodes.");
                }
            }
        }

        if (metadata.getKeyspace(config.keyspace()) == null) { // keyspace previously didn't exist; we need to set it up
            createKeyspace(config.keyspace(), dcsInCluster);
            return;
        }

        createTables(ImmutableMap.of(CassandraConstants.METADATA_TABLE, Integer.MAX_VALUE));
    }

    @Override
    public Map<Cell, Value> getRows(final String tableName,
                                    final Iterable<byte[]> rows,
                                    ColumnSelection selection,
                                    final long startTs) {
        if (!selection.allColumnsSelected()) {
            Collection<byte[]> selectedColumns = selection.getSelectedColumns();
            Set<Cell> cells = Sets.newHashSetWithExpectedSize(selectedColumns.size() * Iterables.size(rows));
            for (byte[] row : rows) {
                for (byte[] col : selectedColumns) {
                    cells.add(Cell.create(row, col));
                }
            }
            try {
                StartTsResultsCollector collector = new StartTsResultsCollector(startTs);
                loadWithTs(tableName, cells, startTs, collector, readConsistency);
                return collector.collectedResults;
            } catch (Throwable t) {
                throw Throwables.throwUncheckedException(t);
            }
        }

        try {
            return getRowsAllColsInternal(tableName, rows, startTs);
        } catch (Throwable t) {
            throw Throwables.throwUncheckedException(t);
        }
    }

    private Map<Cell, Value> getRowsAllColsInternal(final String tableName,
                                                    final Iterable<byte[]> rows,
                                                    final long startTs) throws Exception {
        int rowCount = 0;
        String getRowsQuery = "SELECT * FROM " + getFullTableName(tableName) + " WHERE " + CassandraConstants.ROW_NAME
                + " = ?";
        Map<Cell, Value> result = Maps.newHashMap();
        final CassandraKeyValueServiceConfig config = configManager.getConfig();
        int fetchBatchCount = config.fetchBatchCount();
        for (final List<byte[]> batch : Iterables.partition(rows, fetchBatchCount)) {
            rowCount += batch.size();
            List<ResultSetFuture> resultSetFutures = Lists.newArrayListWithExpectedSize(rowCount);
            PreparedStatement preparedStatement = getPreparedStatement(tableName, getRowsQuery);
            for (byte[] row : batch) {
                BoundStatement boundStatement = preparedStatement.bind(ByteBuffer.wrap(row));
                resultSetFutures.add(session.executeAsync(boundStatement));
            }
            for (ResultSetFuture resultSetFuture : resultSetFutures) {
                ResultSet resultSet;
                try {
                    resultSet = resultSetFuture.getUninterruptibly();
                } catch (Throwable t) {
                    throw Throwables.throwUncheckedException(t);
                }
                for (Row row : resultSet.all()) {
                    Cell c = Cell.create(CQLKeyValueServices.getRowName(row), CQLKeyValueServices.getColName(row));
                    if ((CQLKeyValueServices.getTs(row) < startTs)
                            && (!result.containsKey(c) || (result.get(c).getTimestamp() < CQLKeyValueServices.getTs(row)))) {
                        result.put(
                                Cell.create(CQLKeyValueServices.getRowName(row), CQLKeyValueServices.getColName(row)),
                                Value.create(CQLKeyValueServices.getValue(row), CQLKeyValueServices.getTs(row)));
                    }
                }
                CQLKeyValueServices.logTracedQuery(getRowsQuery, resultSet, session, statementCache);
            }
        }
        if (rowCount > fetchBatchCount) {
            log.warn("Rebatched in getRows a call to " + tableName + " that attempted to multiget "
                    + rowCount + " rows; this may indicate overly-large batching on a higher level.\n"
                    + CassandraKeyValueServices.getFilteredStackTrace("com.palantir"));
        }
        return result;
    }

    @Override
    public Map<Cell, Value> get(String tableName, Map<Cell, Long> timestampByCell) {
        try {
            long firstTs = timestampByCell.values().iterator().next();
            if (Iterables.all(timestampByCell.values(), Predicates.equalTo(firstTs))) {
                StartTsResultsCollector collector = new StartTsResultsCollector(firstTs);
                loadWithTs(tableName, timestampByCell.keySet(), firstTs, collector, readConsistency);
                return collector.collectedResults;
            }

            SetMultimap<Long, Cell> cellsByTs = HashMultimap.create();
            Multimaps.invertFrom(Multimaps.forMap(timestampByCell), cellsByTs);
            Builder<Cell, Value> builder = ImmutableMap.builder();
            for (long ts : cellsByTs.keySet()) {
                StartTsResultsCollector collector = new StartTsResultsCollector(ts);
                loadWithTs(tableName, cellsByTs.get(ts), ts, collector, readConsistency);
                builder.putAll(collector.collectedResults);
            }
            return builder.build();
        } catch (Throwable t) {
            throw Throwables.throwUncheckedException(t);
        }
    }

    private void loadWithTs(final String tableName,
                            final Set<Cell> cells,
                            final long startTs,
                            final Visitor<Map<Cell, Value>> v,
                            final ConsistencyLevel consistency) throws Exception {
        final CassandraKeyValueServiceConfig config = configManager.getConfig();
        int fetchBatchCount = config.fetchBatchCount();
        Map<byte[], List<Cell>> cellsByPrimaryKey = partitionCellsByPrimaryKey(cells);
        final String loadWithTsQuery = "SELECT * FROM " + getFullTableName(tableName) + " "
                + "WHERE " + CassandraConstants.ROW_NAME + " = ? AND " + CassandraConstants.COL_NAME_COL + " = ? AND " + CassandraConstants.TS_COL
                + " > ? LIMIT 1";
        if (cells.size() > fetchBatchCount) {
            log.warn("Re-batching in loadWithTs a call to " + tableName
                    + " that attempted to multiget " + cells.size()
                    + " cells; this may indicate overly-large batching on a higher level.\n"
                    + CassandraKeyValueServices.getFilteredStackTrace("com.palantir"));
        }
        final PreparedStatement preparedStatement = getPreparedStatement(tableName, loadWithTsQuery).setConsistencyLevel(consistency);
        List<ResultSetFuture> resultSetFutures = Lists.newArrayListWithCapacity(cells.size());
        for (final List<Cell> primaryKeyPartition : cellsByPrimaryKey.values()) {
            if (primaryKeyPartition.size() > fetchBatchCount) {
                log.warn("Re-batching in loadWithTs a call to {} that attempted to multiget {} cells; " +
                                "this may indicate overly-large batching on a higher level.\n {}",
                        tableName, cells.size(), CassandraKeyValueServices.getFilteredStackTrace("com.palantir"));
            }
            for (final List<Cell> partition : Lists.partition(primaryKeyPartition, fetchBatchCount)) {
                BatchStatement batchStatement = new BatchStatement(BatchStatement.Type.UNLOGGED);
                if (shouldTraceQuery(tableName)) {
                    batchStatement.enableTracing();
                }
                for (Cell c : partition) {
                    BoundStatement boundStatement = preparedStatement.bind();
                    boundStatement.setBytes(CassandraConstants.ROW_NAME, ByteBuffer.wrap(c.getRowName()));
                    boundStatement.setBytes(
                            CassandraConstants.COL_NAME_COL,
                            ByteBuffer.wrap(c.getColumnName()));
                    boundStatement.setLong(CassandraConstants.TS_COL, ~startTs);
                    batchStatement.add(boundStatement);
                }
                ResultSetFuture resultSetFuture = session.executeAsync(batchStatement);
                resultSetFutures.add(resultSetFuture);
                Futures.addCallback(resultSetFuture, getCallback(v, loadWithTsQuery));
            }
        }
        for (ResultSetFuture rsf : resultSetFutures) {
            rsf.getUninterruptibly();
        }
    }

    private Map<byte[], List<Cell>> partitionCellsByPrimaryKey(Collection<Cell> cells) {
        return Multimaps.asMap(Multimaps.index(cells, Cells.getRowFunction()));
    }

    private FutureCallback<ResultSet> getCallback(final Visitor<Map<Cell, Value>> v,
                                                  final String query) {
        return new FutureCallback<ResultSet>() {
            @Override
            public void onSuccess(ResultSet resultSet) {
                List<Row> rows = resultSet.all();
                Map<Cell, Value> res = Maps.newHashMapWithExpectedSize(rows.size());
                for (Row row : rows) {
                    res.put(
                            Cell.create(CQLKeyValueServices.getRowName(row), CQLKeyValueServices.getColName(row)),
                            Value.create(CQLKeyValueServices.getValue(row), CQLKeyValueServices.getTs(row)));
                }
                CQLKeyValueServices.logTracedQuery(query, resultSet, session, statementCache);
                v.visit(res);
            }
            @Override
            public void onFailure(Throwable throwable) {
                log.error("Failed CQL query: {}, threw {}", query, throwable);
                throw Throwables.throwUncheckedException(throwable);
            }
        };
    }

    @Override
    public Map<Cell, Long> getLatestTimestamps(final String tableName,
                                               Map<Cell, Long> timestampByCell) {
        try {
            return getLatestTimestampsInternal(tableName, timestampByCell);
        } catch (Throwable t) {
            throw Throwables.throwUncheckedException(t);
        }
    }

    private Map<Cell, Long> getLatestTimestampsInternal(final String tableName,
                                                        Map<Cell, Long> timestampByCell) throws Exception {
        final CassandraKeyValueServiceConfig config = configManager.getConfig();
        int fetchBatchCount = config.fetchBatchCount();
        Iterable<List<Cell>> partitions = Iterables.partition(
                timestampByCell.keySet(),
                fetchBatchCount);
        int numPartitions = (timestampByCell.size() / fetchBatchCount)
                + (timestampByCell.size() % fetchBatchCount > 0 ? 1 : 0);
        List<Future<Map<Cell, Long>>> futures = Lists.newArrayListWithCapacity(numPartitions);
        final String loadOnlyTsQuery = "SELECT " + CassandraConstants.ROW_NAME + ", " + CassandraConstants.COL_NAME_COL + ", " + CassandraConstants.TS_COL
                + " FROM " + getFullTableName(tableName) + " " + "WHERE " + CassandraConstants.ROW_NAME + " = ? AND "
                + CassandraConstants.COL_NAME_COL + " = ? LIMIT 1";
        if (timestampByCell.size() > fetchBatchCount) {
            log.warn("Re-batching in getLatestTimestamps a call to " + tableName
                    + " that attempted to multiget " + timestampByCell.size()
                    + " cells; this may indicate overly-large batching on a higher level.\n"
                    + CassandraKeyValueServices.getFilteredStackTrace("com.palantir"));
        }
        for (final List<Cell> partition : partitions) {
            futures.add(executor.submit(new Callable<Map<Cell, Long>>() {
                @Override
                public Map<Cell, Long> call() throws Exception {
                    PreparedStatement preparedStatement = getPreparedStatement(tableName, loadOnlyTsQuery);
                    preparedStatement.setConsistencyLevel(readConsistency);
                    List<ResultSetFuture> resultSetFutures = Lists.newArrayListWithExpectedSize(partition.size());
                    for (Cell c : partition) {
                        BoundStatement boundStatement = preparedStatement.bind();
                        boundStatement.setBytes(CassandraConstants.ROW_NAME, ByteBuffer.wrap(c.getRowName()));
                        boundStatement.setBytes(
                                CassandraConstants.COL_NAME_COL,
                                ByteBuffer.wrap(c.getColumnName()));
                        resultSetFutures.add(session.executeAsync(boundStatement));
                    }
                    Map<Cell, Long> res = Maps.newHashMapWithExpectedSize(partition.size());
                    for (ResultSetFuture resultSetFuture : resultSetFutures) {
                        ResultSet resultSet = resultSetFuture.getUninterruptibly();
                        for (Row row : resultSet.all()) {
                            res.put(Cell.create(CQLKeyValueServices.getRowName(row), CQLKeyValueServices.getColName(row)), CQLKeyValueServices.getTs(row));
                        }
                        CQLKeyValueServices.logTracedQuery(loadOnlyTsQuery, resultSet, session, statementCache);
                    }
                    return res;
                }
            }));
        }
        Map<Cell, Long> res = Maps.newHashMapWithExpectedSize(timestampByCell.size());
        for (Future<Map<Cell, Long>> f : futures) {
            try {
                res.putAll(f.get());
            } catch (InterruptedException e) {
                throw Throwables.throwUncheckedException(e);
            } catch (ExecutionException e) {
                Throwables.throwIfInstance(e, Error.class);
                throw Throwables.throwUncheckedException(e.getCause());
            }
        }
        return res;
    }

    @Override
    public void put(final String tableName, final Map<Cell, byte[]> values, final long timestamp) {
        try {
            putInternal(
                    tableName,
                    KeyValueServices.toConstantTimestampValues(values.entrySet(), timestamp),
                    TransactionType.NONE);
        } catch (Throwable t) {
            throw Throwables.throwUncheckedException(t);
        }
    }

    @Override
    public void putWithTimestamps(String tableName, Multimap<Cell, Value> values) {
        try {
            putInternal(tableName, values.entries(), TransactionType.NONE);
        } catch (Throwable t) {
            throw Throwables.throwUncheckedException(t);
        }
    }

    @Override
    protected int getMultiPutBatchCount() {
        return configManager.getConfig().mutationBatchCount();
    }

    @Override
    public void multiPut(Map<String, ? extends Map<Cell, byte[]>> valuesByTable, final long timestamp) throws KeyAlreadyExistsException {
        Map<ResultSetFuture, String> resultSetFutures = Maps.newHashMap();
        for (Entry<String, ? extends Map<Cell, byte[]>> e : valuesByTable.entrySet()) {
            final String table = e.getKey();
            // We sort here because some key value stores are more efficient if you store adjacent keys together.
            NavigableMap<Cell, byte[]> sortedMap = ImmutableSortedMap.copyOf(e.getValue());


            Iterable<List<Entry<Cell, byte[]>>> partitions = partitionByCountAndBytes(sortedMap.entrySet(),
                    getMultiPutBatchCount(), getMultiPutBatchSizeBytes(), table, CQLKeyValueServices.MULTIPUT_ENTRY_SIZING_FUNCTION);


            for (final List<Entry<Cell, byte[]>> p : partitions) {
                List<Entry<Cell, Value>> partition = Lists.transform(p, new Function<Entry<Cell, byte[]>, Entry<Cell, Value>>() {
                    @Override
                    public Entry<Cell, Value> apply(Entry<Cell, byte[]> input) {
                        return Maps.immutableEntry(input.getKey(), Value.create(input.getValue(), timestamp));
                    }});
                resultSetFutures.put(getPutPartitionResultSetFuture(table, partition, TransactionType.NONE), table);
            }
        }

        for (Entry<ResultSetFuture, String> result : resultSetFutures.entrySet()) {
            ResultSet resultSet;
            try {
                resultSet = result.getKey().getUninterruptibly();
                resultSet.all();
            } catch (Throwable t) {
                throw Throwables.throwUncheckedException(t);
            }
            CQLKeyValueServices.logTracedQuery(getPutQuery(result.getValue()), resultSet, session, statementCache);
        }
    }

    private void putInternal(final String tableName, final Iterable<Map.Entry<Cell, Value>> values, TransactionType transactionType)
            throws Exception {
        final CassandraKeyValueServiceConfig config = configManager.getConfig();
        List<ResultSetFuture> resultSetFutures = Lists.newArrayList();
        int mutationBatchCount = config.mutationBatchCount();
        long mutationBatchSizeBytes = config.mutationBatchSizeBytes();
        for (List<Entry<Cell, Value>> partition : partitionByCountAndBytes(
                values,
                mutationBatchCount,
                mutationBatchSizeBytes,
                tableName,
                CQLKeyValueServices.PUT_ENTRY_SIZING_FUNCTION)) {
            resultSetFutures.add(getPutPartitionResultSetFuture(tableName, partition, transactionType));
        }

        final String putQuery = getPutQueryForPossibleTransaction(tableName, transactionType);
        for (ResultSetFuture resultSetFuture : resultSetFutures) {
            ResultSet resultSet;
            try {
                resultSet = resultSetFuture.getUninterruptibly();
                resultSet.all();
            } catch (Throwable t) {
                throw Throwables.throwUncheckedException(t);
            }
            CQLKeyValueServices.logTracedQuery(putQuery, resultSet, session, statementCache);
        }
    }

    private String getPutQueryForPossibleTransaction(String tableName, TransactionType transactionType) {
        return transactionType.equals(TransactionType.LIGHTWEIGHT_TRANSACTION_REQUIRED)? getPutUnlessExistsQuery(tableName) : getPutQuery(tableName);
    }

    private String getPutUnlessExistsQuery(String tableName) {
        return getPutQuery(tableName) + " IF NOT EXISTS";
    }

    private String getPutQuery(String tableName) {
        return "INSERT INTO " + getFullTableName(tableName) + " (" + CassandraConstants.ROW_NAME + ", " + CassandraConstants.COL_NAME_COL + ", " + CassandraConstants.TS_COL + ", " + CassandraConstants.VALUE_COL + ") VALUES (?, ?, ?, ?)";
    }

    private ResultSetFuture getPutPartitionResultSetFuture(String tableName,
                                                           List<Entry<Cell, Value>> partition,
                                                           TransactionType transactionType) {
        PreparedStatement preparedStatement = getPreparedStatement(tableName, getPutQueryForPossibleTransaction(tableName, transactionType));
        preparedStatement.setConsistencyLevel(writeConsistency);

        // Be mindful when using the atomicity semantics of UNLOGGED batch statements.
        // This usage should be okay, as the KVS.multiPut explicitly does not guarantee
        // atomicity across cells (nor batch isolation, which we also cannot provide)
        BatchStatement batchStatement = new BatchStatement(BatchStatement.Type.UNLOGGED);
        if (shouldTraceQuery(tableName)) {
            batchStatement.enableTracing();
        }
        for (Entry<Cell, Value> e : partition) {
            BoundStatement boundStatement = preparedStatement.bind();
            boundStatement.setBytes(CassandraConstants.ROW_NAME, ByteBuffer.wrap(e.getKey().getRowName()));
            boundStatement.setBytes(CassandraConstants.COL_NAME_COL, ByteBuffer.wrap(e.getKey().getColumnName()));
            boundStatement.setLong(CassandraConstants.TS_COL, ~e.getValue().getTimestamp());
            boundStatement.setBytes(CassandraConstants.VALUE_COL, ByteBuffer.wrap(e.getValue().getContents()));
            if (partition.size() > 1) {
                batchStatement.add(boundStatement);
            } else {
                return session.executeAsync(boundStatement);
            }
        }
        return session.executeAsync(batchStatement);
    }

    @Override
    public void truncateTable(final String tableName) {
        truncateTables(ImmutableSet.of(tableName));
    }

    @Override
    public void truncateTables(final Set<String> tablesToTruncate) {
        String truncateQuery = "TRUNCATE %s"; // full table name (ks.cf)

        for (String tableName : tablesToTruncate) {
            BoundStatement truncateStatement =
                    getPreparedStatement(tableName, String.format(truncateQuery, getFullTableName(tableName)))
                            .setConsistencyLevel(ConsistencyLevel.ALL)
                            .bind();

            try {
                ResultSet resultSet = longRunningQuerySession.execute(truncateStatement);
                CQLKeyValueServices.logTracedQuery(truncateQuery, resultSet, session, statementCache);
            } catch (com.datastax.driver.core.exceptions.UnavailableException e) {
                throw new InsufficientConsistencyException("Truncating tables requires all Cassandra nodes to be up and available.", e);
            }
        }

        CQLKeyValueServices.waitForSchemaVersionsToCoalesce("truncateTables(" + tablesToTruncate.size() + " tables)", this);
    }

    @Override
    public void delete(final String tableName, final Multimap<Cell, Long> keys) {
        int cellCount = 0;
        final String deleteQuery = "DELETE FROM " + getFullTableName(tableName) + " WHERE "
                + CassandraConstants.ROW_NAME + " = ? AND " + CassandraConstants.COL_NAME_COL + " = ? AND " + CassandraConstants.TS_COL + " = ?";
        final CassandraKeyValueServiceConfig config = configManager.getConfig();
        int fetchBatchCount = config.fetchBatchCount();
        for (final List<Cell> batch : Iterables.partition(keys.keySet(), fetchBatchCount)) {
            cellCount += batch.size();
            PreparedStatement deleteStatement = getPreparedStatement(tableName, deleteQuery).setConsistencyLevel(deleteConsistency);
            List<ResultSetFuture> resultSetFutures = Lists.newArrayList();
            for (Cell key : batch) {
                for (long ts : Ordering.natural().immutableSortedCopy(keys.get(key))) {
                    BoundStatement boundStatement = deleteStatement.bind(
                            ByteBuffer.wrap(key.getRowName()),
                            ByteBuffer.wrap(key.getColumnName()),
                            ~ts
                    );
                    resultSetFutures.add(longRunningQuerySession.executeAsync(boundStatement));
                }
            }
            for (ResultSetFuture resultSetFuture : resultSetFutures) {
                ResultSet resultSet;
                try {
                    resultSet = resultSetFuture.getUninterruptibly();
                    resultSet.all();
                } catch (Throwable t) {
                    throw Throwables.throwUncheckedException(t);
                }
                CQLKeyValueServices.logTracedQuery(deleteQuery, resultSet, session, statementCache);
            }
        }
        if (cellCount > fetchBatchCount) {
            log.warn("Rebatched in delete a call to " + tableName + " that attempted to delete "
                    + cellCount
                    + " cells; this may indicate overly-large batching on a higher level.\n"
                    + CassandraKeyValueServices.getFilteredStackTrace("com.palantir"));
        }
    }

    // TODO: after cassandra change: handle multiRanges
    @Override
    @Idempotent
    public Map<RangeRequest, TokenBackedBasicResultsPage<RowResult<Value>, byte[]>> getFirstBatchForRanges(String tableName,
                                                                                                           Iterable<RangeRequest> rangeRequests,
                                                                                                           long timestamp) {
        return KeyValueServices.getFirstBatchForRangesUsingGetRangeConcurrent(
                executor,
                this,
                tableName,
                rangeRequests,
                timestamp,
                configManager.getConfig().rangesConcurrency());
    }

    // TODO: after cassandra change: handle reverse ranges
    // TODO: after cassandra change: handle column filtering
    @Override
    @Idempotent
    public ClosableIterator<RowResult<Value>> getRange(String tableName,
                                                       final RangeRequest rangeRequest,
                                                       final long timestamp) {
        return getRangeWithPageCreator(
                tableName,
                rangeRequest,
                timestamp,
                readConsistency,
                ValueExtractor.SUPPLIER);
    }

    @Override
    @Idempotent
    public ClosableIterator<RowResult<Set<Long>>> getRangeOfTimestamps(String tableName,
                                                                       RangeRequest rangeRequest,
                                                                       long timestamp) {
        return getRangeWithPageCreator(
                tableName,
                rangeRequest,
                timestamp,
                deleteConsistency,
                TimestampExtractor.SUPPLIER);
    }

    @Override
    @Idempotent
    public ClosableIterator<RowResult<Set<Value>>> getRangeWithHistory(String tableName,
                                                                       RangeRequest rangeRequest,
                                                                       long timestamp) {
        return getRangeWithPageCreator(
                tableName,
                rangeRequest,
                timestamp,
                deleteConsistency,
                HistoryExtractor.SUPPLIER);
    }

    public <T, U> ClosableIterator<RowResult<U>> getRangeWithPageCreator(final String tableName,
                                                                         final RangeRequest rangeRequest,
                                                                         final long timestamp,
                                                                         final com.datastax.driver.core.ConsistencyLevel consistency,
                                                                         final Supplier<ResultsExtractor<T, U>> resultsExtractor) {
        if (rangeRequest.isReverse()) {
            throw new UnsupportedOperationException();
        }
        final int batchHint = rangeRequest.getBatchHint() == null ? 100 : rangeRequest.getBatchHint();
        final ColumnSelection selection = rangeRequest.getColumnNames().isEmpty() ? ColumnSelection.all() : ColumnSelection.create(rangeRequest.getColumnNames());
        final byte[] endExclusive = rangeRequest.getEndExclusive();
        final StringBuilder bindQuery = new StringBuilder();
        bindQuery.append("SELECT * FROM " + getFullTableName(tableName) + " WHERE token("
                + CassandraConstants.ROW_NAME + ") >= token(?) ");
        if (endExclusive.length > 0) {
            bindQuery.append("AND token(" + CassandraConstants.ROW_NAME + ") < token(?) ");
        }
        bindQuery.append("LIMIT " + batchHint);
        final String getLastRowQuery = "SELECT * FROM " + getFullTableName(tableName) + " WHERE "
                + CassandraConstants.ROW_NAME + " = ?";
        return ClosableIterators.wrap(new AbstractPagingIterable<RowResult<U>, TokenBackedBasicResultsPage<RowResult<U>, byte[]>>() {
            @Override
            protected TokenBackedBasicResultsPage<RowResult<U>, byte[]> getFirstPage()
                    throws Exception {
                return getPage(rangeRequest.getStartInclusive());
            }

            @Override
            protected TokenBackedBasicResultsPage<RowResult<U>, byte[]> getNextPage(TokenBackedBasicResultsPage<RowResult<U>, byte[]> previous)
                    throws Exception {
                return getPage(previous.getTokenForNextPage());
            }

            TokenBackedBasicResultsPage<RowResult<U>, byte[]> getPage(final byte[] startKey)
                    throws Exception {
                BoundStatement boundStatement = getPreparedStatement(tableName, bindQuery.toString())
                        .setConsistencyLevel(consistency)
                        .bind();

                boundStatement.setBytes(0, ByteBuffer.wrap(startKey));
                if (endExclusive.length > 0) {
                    boundStatement.setBytes(1, ByteBuffer.wrap(endExclusive));
                }
                ResultSet resultSet = session.execute(boundStatement);
                List<Row> rows = Lists.newArrayList(resultSet.all());
                CQLKeyValueServices.logTracedQuery(bindQuery.toString(), resultSet, session, statementCache);
                byte[] maxRow = null;
                ResultsExtractor<T, U> extractor = resultsExtractor.get();
                for (Row row : rows) {
                    byte[] rowName = CQLKeyValueServices.getRowName(row);
                    if (maxRow == null) {
                        maxRow = rowName;
                    } else {
                        maxRow = PtBytes.BYTES_COMPARATOR.max(maxRow, rowName);
                    }
                }
                if (maxRow == null) {
                    return new SimpleTokenBackedResultsPage<RowResult<U>, byte[]>(
                            endExclusive,
                            ImmutableList.<RowResult<U>> of(),
                            false);
                }
                // get the rest of the last row
                BoundStatement boundLastRow = getPreparedStatement(tableName, getLastRowQuery).bind();

                boundLastRow.setBytes(CassandraConstants.ROW_NAME, ByteBuffer.wrap(maxRow));
                try {
                    resultSet = session.execute(boundLastRow);
                } catch (com.datastax.driver.core.exceptions.UnavailableException e) {
                    throw new InsufficientConsistencyException("This operation requires all Cassandra nodes to be up and available.", e);
                }
                rows.addAll(resultSet.all());
                CQLKeyValueServices.logTracedQuery(getLastRowQuery, resultSet, session, statementCache);
                for (Row row : rows) {
                    extractor.internalExtractResult(
                            timestamp,
                            selection,
                            CQLKeyValueServices.getRowName(row),
                            CQLKeyValueServices.getColName(row),
                            CQLKeyValueServices.getValue(row),
                            CQLKeyValueServices.getTs(row));
                }
                SortedMap<byte[], SortedMap<byte[], U>> resultsByRow = Cells.breakCellsUpByRow(extractor.asMap());
                return ResultsExtractor.getRowResults(endExclusive, maxRow, resultsByRow);
            }

        }.iterator());
    }

    @Override
    public void dropTable(final String tableName) {
        dropTables(ImmutableSet.of(tableName));
    }

    @Override
    public void dropTables(final Set<String> tablesToDrop) {
        String dropQuery = "DROP TABLE IF EXISTS %s"; // full table name (ks.cf)

        for (String tableName : tablesToDrop) {
            BoundStatement dropStatement =
                    getPreparedStatement(tableName, String.format(dropQuery, getFullTableName(tableName)))
                            .setConsistencyLevel(ConsistencyLevel.ALL)
                            .bind();
            try {
                ResultSet resultSet = longRunningQuerySession.execute(dropStatement);
                CQLKeyValueServices.logTracedQuery(dropQuery, resultSet, session, statementCache);
            } catch (com.datastax.driver.core.exceptions.UnavailableException e) {
                throw new InsufficientConsistencyException("Dropping tables requires all Cassandra nodes to be up and available.", e);
            }
        }

        CQLKeyValueServices.waitForSchemaVersionsToCoalesce("dropTables(" + tablesToDrop.size() + " tables)", this);

        put(CassandraConstants.METADATA_TABLE, Maps.toMap(
                        Lists.transform(Lists.newArrayList(tablesToDrop), new Function<String, Cell>() {
                            @Override
                            public Cell apply(String tableName) {
                                return CQLKeyValueServices.getMetadataCell(tableName);
                            }}),
                        Functions.constant(PtBytes.EMPTY_BYTE_ARRAY)),
                System.currentTimeMillis());
    }

    private void createKeyspace(String keyspaceName, Set<String> dcsInCluster) {
        final CassandraKeyValueServiceConfig config = configManager.getConfig();
        String create_keyspace = "create keyspace if not exists %s with replication = %s and durable_writes = true";
        String replication;
        if (dcsInCluster.size() > 0) { // default to user-set RF on every user datacenter; user can alter keyspace if this is not what they want
            replication = "{ 'class' : 'NetworkTopologyStrategy'[";
            for (Iterator<String> iter =  dcsInCluster.iterator(); iter.hasNext();) {
                String datacenter = iter.next();
                replication +=  "'" + datacenter + "' : " + config.replicationFactor();
                if (iter.hasNext()) {
                    replication += ", ";
                }
            }
            replication += "]} ";
        } else {
            replication = "{ 'class' : 'SimpleStrategy', 'replication_factor' : " + config.replicationFactor() + "}";
        }

        session.execute(
                getPreparedStatement(CassandraConstants.NO_TABLE,
                        String.format(create_keyspace, keyspaceName, replication))
                        .setConsistencyLevel(ConsistencyLevel.ALL)
                        .bind());

        CQLKeyValueServices.waitForSchemaVersionsToCoalesce("Initial creation of the Atlas keyspace", this);
    }

    @Override
    public void createTable(final String tableName, final int maxValueSizeInBytes) {
        createTables(ImmutableMap.of(tableName, maxValueSizeInBytes));
    }

    @Override
    public void createTables(final Map<String, Integer> tableNamesToMaxValueSizeInBytes) {
        String createQuery = "CREATE TABLE %s ( " // full table name (ks.cf)
                + CassandraConstants.ROW_NAME + " blob, "
                + CassandraConstants.COL_NAME_COL + " blob, "
                + CassandraConstants.TS_COL + " bigint, "
                + CassandraConstants.VALUE_COL + " blob, "
                + "PRIMARY KEY ("
                + CassandraConstants.ROW_NAME + ", "
                + CassandraConstants.COL_NAME_COL + ", "
                + CassandraConstants.TS_COL + ")) "
                + "WITH COMPACT STORAGE AND CLUSTERING ORDER BY ("
                + CassandraConstants.COL_NAME_COL + " ASC, "
                + CassandraConstants.TS_COL + " ASC) "
                + "AND compaction = {'sstable_size_in_mb': '80', 'class': 'org.apache.cassandra.db.compaction.LeveledCompactionStrategy'}";

        final CassandraKeyValueServiceConfig config = configManager.getConfig();
        Collection<com.datastax.driver.core.TableMetadata> tables = cluster.getMetadata().getKeyspace(config.keyspace()).getTables();

        Iterable<String> existingTables = Iterables.transform(tables, new Function<com.datastax.driver.core.TableMetadata, String>() {
            @Override
            public String apply(com.datastax.driver.core.TableMetadata input) {
                return input.getName();
            }
        });

        for (String tableName : Sets.difference(tableNamesToMaxValueSizeInBytes.keySet(), Sets.newHashSet(existingTables))) {
            BoundStatement createStatement =
                    getPreparedStatement(tableName, String.format(createQuery, getFullTableName(tableName)))
                            .setConsistencyLevel(ConsistencyLevel.ALL)
                            .bind();
            try {
                ResultSet resultSet = longRunningQuerySession.execute(createStatement);
                CQLKeyValueServices.logTracedQuery(createQuery, resultSet, session, statementCache);
            } catch (com.datastax.driver.core.exceptions.UnavailableException e) {
                throw new InsufficientConsistencyException("Creating tables requires all Cassandra nodes to be up and available.", e);
            }
        }
        CQLKeyValueServices.waitForSchemaVersionsToCoalesce("createTables(" + tableNamesToMaxValueSizeInBytes.size() + " tables)", this);
    }

    @Override
    public Set<String> getAllTableNames() {
        final CassandraKeyValueServiceConfig config = configManager.getConfig();
        List<Row> rows = session.execute(statementCache.getUnchecked(
                "SELECT columnfamily_name FROM system.schema_columnfamilies WHERE keyspace_name = ?")
                .bind(config.keyspace()))
                .all();

        Set<String> existingTables = Sets.newHashSet(Iterables.transform(rows, new Function<Row, String>(){
            @Override
            public String apply(Row row) {
                return row.getString("columnfamily_name");
            }}));

        return Sets.filter(existingTables, new Predicate<String>() {
            @Override
            public boolean apply(String tableName) {
                return !tableName.startsWith("_") || tableName.startsWith(AtlasDbConstants.NAMESPACE_PREFIX);
            }
        });
    }

    @Override
    public byte[] getMetadataForTable(String tableName) {
        Cell cell = CQLKeyValueServices.getMetadataCell(tableName);
        Value v = get(CassandraConstants.METADATA_TABLE, ImmutableMap.of(cell, Long.MAX_VALUE)).get(
                cell);
        if (v == null) {
            return new byte[0];
        } else {
            return v.getContents();
        }
    }

    @Override
    public void putMetadataForTable(final String tableName, final byte[] meta) {
        putMetadataForTables(ImmutableMap.of(tableName, meta));
    }

    @Override
    public void putMetadataForTables(final Map<String, byte[]> tableNameToMetadata) {
        Map<Cell, byte[]> cellToMetadata = Maps.newHashMap();
        for (Entry<String, byte[]> tableEntry : tableNameToMetadata.entrySet()) {
            cellToMetadata.put(CQLKeyValueServices.getMetadataCell(tableEntry.getKey()), tableEntry.getValue());
            CQLKeyValueServices.setSettingsForTable(tableEntry.getKey(), tableEntry.getValue(), this);
        }
        put(CassandraConstants.METADATA_TABLE, cellToMetadata, System.currentTimeMillis());
        CQLKeyValueServices.waitForSchemaVersionsToCoalesce("putMetadataForTables(" + tableNameToMetadata.size() +" tables)", this);
    }

    @Override
    public void addGarbageCollectionSentinelValues(String tableName, Set<Cell> cells) {
        try {
            final Value value = Value.create(new byte[0], Value.INVALID_VALUE_TIMESTAMP);
            putInternal(
                    tableName,
                    Iterables.transform(cells, new Function<Cell, Map.Entry<Cell, Value>>() {
                        @Override
                        public Entry<Cell, Value> apply(Cell cell) {
                            return Maps.immutableEntry(cell, value);
                        }
                    }), TransactionType.NONE);
        } catch (Throwable t) {
            throw Throwables.throwUncheckedException(t);
        }
    }

    @Override
    public Multimap<Cell, Long> getAllTimestamps(String tableName, Set<Cell> cells, long ts) {
        AllTimestampsCollector collector = new AllTimestampsCollector();
        try {
            loadWithTs(tableName, cells, ts, collector, deleteConsistency);
        } catch (com.datastax.driver.core.exceptions.UnavailableException e) {
            throw new InsufficientConsistencyException("Get all timestamps requires all Cassandra nodes to be up and available.", e);
        } catch (Throwable t) {
            throw Throwables.throwUncheckedException(t);
        }
        return collector.collectedResults;
    }

    @Override
    public void putUnlessExists(String tableName, Map<Cell, byte[]> values)
            throws KeyAlreadyExistsException {
        try {
            putInternal(
                    tableName,
                    KeyValueServices.toConstantTimestampValues(values.entrySet(), AtlasDbConstants.TRANSACTION_TS),
                    TransactionType.LIGHTWEIGHT_TRANSACTION_REQUIRED);
        } catch (Throwable t) {
            throw Throwables.throwUncheckedException(t);
        }
    }

    String getFullTableName(String tableName) {
        return configManager.getConfig().keyspace() + ".\"" + tableName + "\"";
    }

    @Override
    public void compactInternally(String tableName) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(tableName), "tableName:[%s] should not be null or empty", tableName);
        final CassandraKeyValueServiceConfig config = configManager.getConfig();
        long compactionTimeoutSeconds = config.compactionTimeoutSeconds();
        try {
            alterTableForCompaction(tableName, 0, 0.0f);
            CQLKeyValueServices.waitForSchemaVersionsToCoalesce("setting up tables for compaction", this);
            compactionManager.forceTableCompaction(compactionTimeoutSeconds, config.keyspace(), tableName);
        } catch (TimeoutException e) {
            log.error("Compaction could not finish in {} seconds! {}", compactionTimeoutSeconds, e.getMessage());
            log.error(compactionManager.getPendingCompactionStatus());
        } finally {
            alterTableForCompaction(tableName, CassandraConstants.GC_GRACE_SECONDS, CassandraConstants.TOMBSTONE_THRESHOLD_RATIO);
            CQLKeyValueServices.waitForSchemaVersionsToCoalesce("setting up tables post-compaction", this);
        }
    }

    private void alterTableForCompaction(final String tableName, int gcGraceSeconds, float tombstoneThreshold) {
        log.trace("Altering table {} to have gc_grace_seconds={} and tombstone_threshold=%.2f", tableName, gcGraceSeconds, tombstoneThreshold);
        String alterTableQuery =
                "ALTER TABLE " + getFullTableName(tableName)
                        + " WITH gc_grace_seconds = " + gcGraceSeconds
                        + " and compaction = {'class':'org.apache.cassandra.db.compaction.LeveledCompactionStrategy', 'tombstone_threshold':"
                        + tombstoneThreshold + "};";

        BoundStatement alterTable = getPreparedStatement(tableName, alterTableQuery)
                .setConsistencyLevel(ConsistencyLevel.ALL)
                .bind();
        ResultSet resultSet;
        try {
            resultSet = session.execute(alterTable);
        } catch (UnavailableException e) {
            throw new InsufficientConsistencyException("Alter table requires all Cassandra nodes to be up and available.", e);
        } catch (Exception e) {
            throw Throwables.throwUncheckedException(e);
        }
        CQLKeyValueServices.logTracedQuery(alterTableQuery, resultSet, session, statementCache);
        return;
    }

    PreparedStatement getPreparedStatement(String tableName, String query) {
        try {
            if (shouldTraceQuery(tableName)) {
                return statementCache.get(query).enableTracing();
            } else {
                return statementCache.get(query).disableTracing();
            }
        } catch (ExecutionException e) {
            Throwables.throwIfInstance(e, Error.class);
            throw Throwables.throwUncheckedException(e.getCause());
        }
    }
}
