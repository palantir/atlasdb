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
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;

import org.apache.commons.lang.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.HostDistance;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.PoolingOptions;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ProtocolOptions.Compression;
import com.datastax.driver.core.QueryOptions;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SocketOptions;
import com.datastax.driver.core.exceptions.InvalidQueryException;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.driver.core.exceptions.UnavailableException;
import com.datastax.driver.core.policies.LatencyAwarePolicy;
import com.datastax.driver.core.policies.LoadBalancingPolicy;
import com.datastax.driver.core.policies.RoundRobinPolicy;
import com.datastax.driver.core.policies.TokenAwarePolicy;
import com.datastax.driver.core.policies.WhiteListPolicy;
import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.base.Strings;
import com.google.common.base.Supplier;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.google.common.collect.Ordering;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.Sets;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfig;
import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfigManager;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.ColumnSelection;
import com.palantir.atlasdb.keyvalue.api.InsufficientConsistencyException;
import com.palantir.atlasdb.keyvalue.api.KeyAlreadyExistsException;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.keyvalue.cassandra.CQLKeyValueServices.AllTimestampsCollector;
import com.palantir.atlasdb.keyvalue.cassandra.CQLKeyValueServices.Peer;
import com.palantir.atlasdb.keyvalue.cassandra.CQLKeyValueServices.StartTsResultsCollector;
import com.palantir.atlasdb.keyvalue.cassandra.CQLKeyValueServices.TransactionType;
import com.palantir.atlasdb.keyvalue.cassandra.jmx.CassandraJmxCompaction;
import com.palantir.atlasdb.keyvalue.cassandra.jmx.CassandraJmxCompactionManager;
import com.palantir.atlasdb.keyvalue.impl.AbstractKeyValueService;
import com.palantir.atlasdb.keyvalue.impl.Cells;
import com.palantir.atlasdb.keyvalue.impl.KeyValueServices;
import com.palantir.common.annotation.Idempotent;
import com.palantir.common.base.ClosableIterator;
import com.palantir.common.base.ClosableIterators;
import com.palantir.common.base.Throwables;
import com.palantir.util.Visitor;
import com.palantir.util.paging.AbstractPagingIterable;
import com.palantir.util.paging.SimpleTokenBackedResultsPage;
import com.palantir.util.paging.TokenBackedBasicResultsPage;

public class CQLKeyValueService extends AbstractKeyValueService {
    private static final Logger log = LoggerFactory.getLogger(CQLKeyValueService.class);

    private Cluster cluster, longRunningQueryCluster;
    Session session, longRunningQuerySession;

    CQLStatementCache cqlStatementCache;

    private final CassandraKeyValueServiceConfigManager configManager;
    private final Optional<CassandraJmxCompactionManager> compactionManager;

    private ConsistencyLevel readConsistency = ConsistencyLevel.LOCAL_QUORUM;
    private final ConsistencyLevel writeConsistency = ConsistencyLevel.EACH_QUORUM;
    private final ConsistencyLevel deleteConsistency = ConsistencyLevel.ALL;

    private boolean limitBatchSizesToServerDefaults = false;

    public static CQLKeyValueService create(CassandraKeyValueServiceConfigManager configManager) {
        Optional<CassandraJmxCompactionManager> compactionManager = CassandraJmxCompaction.createJmxCompactionManager(configManager);
        final CQLKeyValueService ret = new CQLKeyValueService(configManager, compactionManager);
        ret.initializeConnectionPool();
        ret.performInitialSetup();
        return ret;
    }

    protected CQLKeyValueService(CassandraKeyValueServiceConfigManager configManager,
                                 Optional<CassandraJmxCompactionManager> compactionManager) {
        super(AbstractKeyValueService.createFixedThreadPool("Atlas CQL KVS", configManager.getConfig().poolSize()));
        this.configManager = configManager;
        this.compactionManager = compactionManager;
    }

    protected void initializeConnectionPool() {
        final CassandraKeyValueServiceConfig config = configManager.getConfig();
        Collection<InetSocketAddress> configuredHosts = config.servers();
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

        Metadata metadata;
        try {
            cluster = clusterBuilder.build();
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
        } catch (IllegalStateException e) {
            if (e.getMessage().contains("requested compression is not available")) { // god dammit datastax what did I do to _you_
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

        cqlStatementCache = new CQLStatementCache(session, longRunningQuerySession);

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
        if(compactionManager.isPresent()) {
            compactionManager.get().close();
        }
        super.close();
    }

    protected void performInitialSetup() {
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

        createTables(ImmutableMap.of(CassandraConstants.METADATA_TABLE, AtlasDbConstants.EMPTY_TABLE_METADATA));
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
                loadWithTs(tableName, cells, startTs, false, collector, readConsistency);
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
            PreparedStatement preparedStatement = getPreparedStatement(tableName, getRowsQuery, session);
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
                CQLKeyValueServices.logTracedQuery(getRowsQuery, resultSet, session, cqlStatementCache.NORMAL_QUERY);
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
                loadWithTs(tableName, timestampByCell.keySet(), firstTs, false, collector, readConsistency);
                return collector.collectedResults;
            }

            SetMultimap<Long, Cell> cellsByTs = HashMultimap.create();
            Multimaps.invertFrom(Multimaps.forMap(timestampByCell), cellsByTs);
            Builder<Cell, Value> builder = ImmutableMap.builder();
            for (long ts : cellsByTs.keySet()) {
                StartTsResultsCollector collector = new StartTsResultsCollector(ts);
                loadWithTs(tableName, cellsByTs.get(ts), ts, false, collector, readConsistency);
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
                            boolean loadAllTs,
                            final Visitor<Multimap<Cell, Value>> v,
                            final ConsistencyLevel consistency) throws Exception {
        final String loadWithTsQuery = "SELECT * FROM " + getFullTableName(tableName) + " "
                + "WHERE " + CassandraConstants.ROW_NAME + " = ? AND " + CassandraConstants.COL_NAME_COL + " = ? AND " + CassandraConstants.TS_COL
                + " > ?" + (!loadAllTs ? " LIMIT 1" : "");
        final CassandraKeyValueServiceConfig config = configManager.getConfig();
        if (cells.size() > config.fetchBatchCount()) {
            log.warn("A call to " + tableName
                    + " is performing a multiget " + cells.size()
                    + " cells; this may indicate overly-large batching on a higher level.\n"
                    + CassandraKeyValueServices.getFilteredStackTrace("com.palantir"));
        }
        final PreparedStatement preparedStatement = getPreparedStatement(tableName, loadWithTsQuery, session).setConsistencyLevel(consistency);
        List<ResultSetFuture> resultSetFutures = Lists.newArrayListWithCapacity(cells.size());

        for (Cell cell : cells) {
            ResultSetFuture resultSetFuture = session.executeAsync(
                    preparedStatement.bind(
                            ByteBuffer.wrap(cell.getRowName()),
                            ByteBuffer.wrap(cell.getColumnName()),
                            ~startTs));
            resultSetFutures.add(resultSetFuture);
        }

        for (ResultSetFuture rsf : resultSetFutures) {
            visitResults(rsf.getUninterruptibly(), v, loadWithTsQuery, loadAllTs);
        }
    }

    // todo use this for insert and delete batch-to-owner-coordinator mapping
    private Map<byte[], List<Cell>> partitionCellsByPrimaryKey(Collection<Cell> cells) {
        return Multimaps.asMap(Multimaps.index(cells, Cells.getRowFunction()));
    }

    private void visitResults(ResultSet resultSet, Visitor<Multimap<Cell, Value>> v, String query, boolean loadAllTs) {
        List<Row> rows = resultSet.all();
        Multimap<Cell, Value> res;
        if (loadAllTs) {
            res = HashMultimap.create();
        } else {
            res = HashMultimap.create(rows.size(), 1);
        }
        for (Row row : rows) {
            res.put(Cell.create(CQLKeyValueServices.getRowName(row), CQLKeyValueServices.getColName(row)),
                    Value.create(CQLKeyValueServices.getValue(row), CQLKeyValueServices.getTs(row)));
        }
        CQLKeyValueServices.logTracedQuery(query, resultSet, session, cqlStatementCache.NORMAL_QUERY);
        v.visit(res);
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
                    PreparedStatement preparedStatement = getPreparedStatement(tableName, loadOnlyTsQuery, session);
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
                        CQLKeyValueServices.logTracedQuery(loadOnlyTsQuery, resultSet, session, cqlStatementCache.NORMAL_QUERY);
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
            CQLKeyValueServices.logTracedQuery(getPutQuery(result.getValue(), CassandraConstants.NO_TTL), resultSet, session, cqlStatementCache.NORMAL_QUERY);
        }
    }

    private void putInternal(final String tableName, final Iterable<Map.Entry<Cell, Value>> values, TransactionType transactionType)
            throws Exception {
        putInternal(tableName, values, transactionType, CassandraConstants.NO_TTL, false);
    }

    protected void putInternal(final String tableName, final Iterable<Map.Entry<Cell, Value>> values, TransactionType transactionType, final int ttl, boolean recursive)
            throws Exception {
        List<ResultSetFuture> resultSetFutures = Lists.newArrayList();
        int mutationBatchCount = configManager.getConfig().mutationBatchCount();
        long mutationBatchSizeBytes = limitBatchSizesToServerDefaults?
                CQLKeyValueServices.UNCONFIGURED_DEFAULT_BATCH_SIZE_BYTES : configManager.getConfig().mutationBatchSizeBytes();
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
                CQLKeyValueServices.logTracedQuery(putQuery, resultSet, session, cqlStatementCache.NORMAL_QUERY);
            } catch (InvalidQueryException e) {
                if (e.getMessage().contains("Batch too large") && !recursive) {
                    log.error("Attempted a put to " + tableName + " that the Cassandra server deemed to be too large to accept. Batch sizes on the Atlas-side have been artificially lowered to the Cassandra default maximum batch sizes.");
                    limitBatchSizesToServerDefaults = true;
                    try {
                        putInternal(tableName, values, transactionType, ttl, true);
                    } catch (Throwable t) {
                        throw Throwables.throwUncheckedException(t);
                    }
                } else {
                    throw Throwables.throwUncheckedException(e);
                }
            } catch (Throwable t) {
                throw Throwables.throwUncheckedException(t);
            }
        }
    }

    private String getPutQueryForPossibleTransaction(String tableName, TransactionType transactionType) {
        return getPutQueryForPossibleTransaction(tableName, transactionType, CassandraConstants.NO_TTL);
    }

    private String getPutQueryForPossibleTransaction(String tableName, TransactionType transactionType, int ttl) {
        return transactionType.equals(TransactionType.LIGHTWEIGHT_TRANSACTION_REQUIRED)? getPutUnlessExistsQuery(tableName, ttl) : getPutQuery(tableName, ttl);
    }

    private String getPutUnlessExistsQuery(String tableName, int ttl) {
        if (ttl <= 0) {
            return getPutQuery(tableName, CassandraConstants.NO_TTL) + " IF NOT EXISTS";
        } else {
            return getPutQuery(tableName, CassandraConstants.NO_TTL) + " IF NOT EXISTS USING TTL " + ttl;
        }
    }

    protected String getPutQuery(String tableName, int ttl) {
        String putQuery = "INSERT INTO " + getFullTableName(tableName) + " (" + CassandraConstants.ROW_NAME + ", " + CassandraConstants.COL_NAME_COL + ", " + CassandraConstants.TS_COL + ", " + CassandraConstants.VALUE_COL + ") VALUES (?, ?, ?, ?)";
        if (ttl >= 0) {
            putQuery += " USING TTL " + ttl;
        }
        return putQuery;
    }

    protected ResultSetFuture getPutPartitionResultSetFuture(String tableName,
                                                             List<Entry<Cell, Value>> partition,
                                                             TransactionType transactionType) {
        return getPutPartitionResultSetFuture(tableName, partition, transactionType, CassandraConstants.NO_TTL);
    }

    protected ResultSetFuture getPutPartitionResultSetFuture(String tableName,
                                                             List<Entry<Cell, Value>> partition,
                                                             TransactionType transactionType,
                                                             int ttl) {
        PreparedStatement preparedStatement = getPreparedStatement(tableName, getPutQueryForPossibleTransaction(tableName, transactionType, ttl), session);
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
                    getPreparedStatement(tableName, String.format(truncateQuery, getFullTableName(tableName)), longRunningQuerySession)
                            .setConsistencyLevel(ConsistencyLevel.ALL)
                            .bind();

            try {
                ResultSet resultSet = longRunningQuerySession.execute(truncateStatement);
                CQLKeyValueServices.logTracedQuery(truncateQuery, resultSet, session, cqlStatementCache.NORMAL_QUERY);
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
            PreparedStatement deleteStatement = getPreparedStatement(tableName, deleteQuery, longRunningQuerySession).setConsistencyLevel(deleteConsistency);
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
                CQLKeyValueServices.logTracedQuery(deleteQuery, resultSet, session, cqlStatementCache.NORMAL_QUERY);
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
        int concurrency = configManager.getConfig().rangesConcurrency();
        return KeyValueServices.getFirstBatchForRangesUsingGetRangeConcurrent(
                executor,
                this,
                tableName,
                rangeRequests,
                timestamp,
                concurrency);
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
        if (rangeRequest.isEmptyRange()) {
            return ClosableIterators.wrap(ImmutableList.<RowResult<U>>of().iterator());
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
                BoundStatement boundStatement = getPreparedStatement(tableName, bindQuery.toString(), session)
                        .setConsistencyLevel(consistency)
                        .bind();

                boundStatement.setBytes(0, ByteBuffer.wrap(startKey));
                if (endExclusive.length > 0) {
                    boundStatement.setBytes(1, ByteBuffer.wrap(endExclusive));
                }
                ResultSet resultSet = session.execute(boundStatement);
                List<Row> rows = Lists.newArrayList(resultSet.all());
                CQLKeyValueServices.logTracedQuery(bindQuery.toString(), resultSet, session, cqlStatementCache.NORMAL_QUERY);
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
                BoundStatement boundLastRow = getPreparedStatement(tableName, getLastRowQuery, session).bind();

                boundLastRow.setBytes(CassandraConstants.ROW_NAME, ByteBuffer.wrap(maxRow));
                try {
                    resultSet = session.execute(boundLastRow);
                } catch (com.datastax.driver.core.exceptions.UnavailableException e) {
                    throw new InsufficientConsistencyException("This operation requires all Cassandra nodes to be up and available.", e);
                }
                rows.addAll(resultSet.all());
                CQLKeyValueServices.logTracedQuery(getLastRowQuery, resultSet, session, cqlStatementCache.NORMAL_QUERY);
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
                    getPreparedStatement(tableName, String.format(dropQuery, getFullTableName(tableName)), longRunningQuerySession)
                            .setConsistencyLevel(ConsistencyLevel.ALL)
                            .bind();
            try {
                ResultSet resultSet = longRunningQuerySession.execute(dropStatement);
                CQLKeyValueServices.logTracedQuery(dropQuery, resultSet, session, cqlStatementCache.NORMAL_QUERY);
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
        String create_keyspace = "create keyspace if not exists %s with replication = %s and durable_writes = true";
        final CassandraKeyValueServiceConfig config = configManager.getConfig();
        String replication;
        if (dcsInCluster.size() > 0) { // default to user-set RF on every user datacenter; user can alter keyspace if this is not what they want
            replication = "{ 'class' : 'NetworkTopologyStrategy', ";
            for (Iterator<String> iter =  dcsInCluster.iterator(); iter.hasNext();) {
                String datacenter = iter.next();
                replication +=  "'" + datacenter + "' : " + config.replicationFactor();
                if (iter.hasNext()) {
                    replication += ", ";
                }
            }
            replication += "} ";
        } else {
            replication = "{ 'class' : 'SimpleStrategy', 'replication_factor' : " + config.replicationFactor() + "}";
        }

        longRunningQuerySession.execute(
                getPreparedStatement(CassandraConstants.NO_TABLE,
                        String.format(create_keyspace, keyspaceName, replication),
                        longRunningQuerySession)
                        .setConsistencyLevel(ConsistencyLevel.ALL)
                        .bind());

        CQLKeyValueServices.waitForSchemaVersionsToCoalesce("Initial creation of the Atlas keyspace", this);
    }

    @Override
    public void createTable(final String tableName, byte[] tableMetadata) {
        createTables(ImmutableMap.of(tableName, tableMetadata));
    }

    @Override
    public void createTables(final Map<String, byte[]> tableNamesToTableMetadata) {
        final CassandraKeyValueServiceConfig config = configManager.getConfig();
        Collection<com.datastax.driver.core.TableMetadata> tables = cluster.getMetadata().getKeyspace(config.keyspace()).getTables();
        Set<String> existingTables = Sets.newHashSet(Iterables.transform(tables, new Function<com.datastax.driver.core.TableMetadata, String>() {
            @Override
            public String apply(com.datastax.driver.core.TableMetadata input) {
                return input.getName();
            }
        }));

        if (!existingTables.contains(CassandraConstants.METADATA_TABLE)) { // ScrubberStore likes to call createTable before our setup gets called...
            CQLKeyValueServices.createTableWithSettings(CassandraConstants.METADATA_TABLE, AtlasDbConstants.EMPTY_TABLE_METADATA, this);
        }

        Sets.SetView<String> tablesToCreate = Sets.difference(tableNamesToTableMetadata.keySet(), existingTables);
        for (String tableName : tablesToCreate) {
            try {
                CQLKeyValueServices.createTableWithSettings(tableName, tableNamesToTableMetadata.get(tableName), this);
            } catch (com.datastax.driver.core.exceptions.UnavailableException e) {
                throw new InsufficientConsistencyException("Creating tables requires all Cassandra nodes to be up and available.", e);
            }
        }

        if (!tablesToCreate.isEmpty()) {
            CQLKeyValueServices.waitForSchemaVersionsToCoalesce("createTables(" + tableNamesToTableMetadata.size() + " tables)", this);
        }

        internalPutMetadataForTables(tableNamesToTableMetadata, false);
    }

    @Override
    public Set<String> getAllTableNames() {
        final CassandraKeyValueServiceConfig config = configManager.getConfig();
        List<Row> rows = session.execute(cqlStatementCache.NORMAL_QUERY.getUnchecked(
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
        internalPutMetadataForTables(tableNameToMetadata, true);
    }

    private void internalPutMetadataForTables(final Map<String, byte[]> tableNameToMetadata, boolean possiblyNeedToPerformSettingsChanges) {
        Map<Cell, byte[]> cellToMetadata = Maps.newHashMap();
        for (Entry<String, byte[]> tableEntry : tableNameToMetadata.entrySet()) {
            byte[] existingMetadata = getMetadataForTable(tableEntry.getKey());
            if (!Arrays.equals(existingMetadata, tableEntry.getValue())) {
                cellToMetadata.put(CQLKeyValueServices.getMetadataCell(tableEntry.getKey()), tableEntry.getValue());
                if (possiblyNeedToPerformSettingsChanges) {
                    CQLKeyValueServices.setSettingsForTable(tableEntry.getKey(), tableEntry.getValue(), this);
                }
            }
        }
        if (!cellToMetadata.isEmpty()) {
            put(CassandraConstants.METADATA_TABLE, cellToMetadata, System.currentTimeMillis());
            if (possiblyNeedToPerformSettingsChanges) {
                CQLKeyValueServices.waitForSchemaVersionsToCoalesce("putMetadataForTables(" + tableNameToMetadata.size() +" tables)", this);
            }
        }
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
            loadWithTs(tableName, cells, ts, true, collector, deleteConsistency);
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
        String internalTableName = internalTableName(tableName);
        return configManager.getConfig().keyspace() + ".\"" + internalTableName + "\"";
    }

    @Override
    public void compactInternally(String tableName) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(tableName), "tableName:[%s] should not be null or empty", tableName);
        CassandraKeyValueServiceConfig config = configManager.getConfig();
        if (!compactionManager.isPresent()) {
            log.error("No compaction client was configured, but compact was called. If you actually want to clear deleted data immediately " +
                    "from Cassandra, lower your gc_grace_seconds setting and run `nodetool compact {} {}`.", config.keyspace(), tableName);
            return;
        }

        long compactionTimeoutSeconds = config.jmx().get().compactionTimeoutSeconds();
        try {
            alterTableForCompaction(tableName, 0, 0.0f);
            CQLKeyValueServices.waitForSchemaVersionsToCoalesce("setting up tables for compaction", this);
            compactionManager.get().performTombstoneCompaction(compactionTimeoutSeconds, config.keyspace(), tableName);
        } catch (TimeoutException e) {
            log.error("Compaction could not finish in {} seconds. {}", compactionTimeoutSeconds, e.getMessage());
            log.error(compactionManager.get().getCompactionStatus());
        } catch (InterruptedException e) {
            log.error("Compaction for {}.{} was interrupted.", config.keyspace(), tableName);
        } finally {
            alterTableForCompaction(tableName, CassandraConstants.GC_GRACE_SECONDS, CassandraConstants.TOMBSTONE_THRESHOLD_RATIO);
            CQLKeyValueServices.waitForSchemaVersionsToCoalesce("setting up tables post-compaction", this);
        }
    }

    private void alterTableForCompaction(String tableName, int gcGraceSeconds, float tombstoneThreshold) {
        log.trace("Altering table {} to have gc_grace_seconds={} and tombstone_threshold=%.2f", tableName, gcGraceSeconds, tombstoneThreshold);
        String alterTableQuery =
                "ALTER TABLE " + getFullTableName(tableName)
                        + " WITH gc_grace_seconds = " + gcGraceSeconds
                        + " and compaction = {'class':'org.apache.cassandra.db.compaction.LeveledCompactionStrategy', 'tombstone_threshold':"
                        + tombstoneThreshold + "};";

        BoundStatement alterTable = getPreparedStatement(tableName, alterTableQuery, longRunningQuerySession)
                .setConsistencyLevel(ConsistencyLevel.ALL)
                .bind();
        ResultSet resultSet;
        try {
            resultSet = longRunningQuerySession.execute(alterTable);
        } catch (UnavailableException e) {
            throw new InsufficientConsistencyException("Alter table requires all Cassandra nodes to be up and available.", e);
        } catch (Exception e) {
            throw Throwables.throwUncheckedException(e);
        }
        CQLKeyValueServices.logTracedQuery(alterTableQuery, resultSet, session, cqlStatementCache.NORMAL_QUERY);
        return;
    }

    PreparedStatement getPreparedStatement(String tableName, String query, Session sessionToBeUsed) {
        try {
            PreparedStatement statement;

            if (sessionToBeUsed == longRunningQuerySession) {
                statement =  cqlStatementCache.LONG_RUNNING_QUERY.get(query).enableTracing();
            } else {
                statement = cqlStatementCache.NORMAL_QUERY.get(query).enableTracing();
            }

            if (shouldTraceQuery(tableName)) {
                statement.enableTracing();
            } else {
                statement.disableTracing();
            }

            return statement;
        } catch (ExecutionException e) {
            Throwables.throwIfInstance(e, Error.class);
            throw Throwables.throwUncheckedException(e.getCause());
        }
    }
}