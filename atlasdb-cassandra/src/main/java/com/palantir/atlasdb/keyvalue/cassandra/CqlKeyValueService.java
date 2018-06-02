/*
 * Copyright 2015 Palantir Technologies, Inc. All rights reserved.
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
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.net.ssl.SSLContext;

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
import com.datastax.driver.core.RemoteEndpointAwareJdkSSLOptions;
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
import com.google.common.base.Functions;
import com.google.common.base.Joiner;
import com.google.common.base.Predicates;
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
import com.google.common.collect.SortedSetMultimap;
import com.google.common.collect.TreeMultimap;
import com.google.common.primitives.UnsignedBytes;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfig;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.CandidateCellForSweeping;
import com.palantir.atlasdb.keyvalue.api.CandidateCellForSweepingRequest;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.CheckAndSetRequest;
import com.palantir.atlasdb.keyvalue.api.ClusterAvailabilityStatus;
import com.palantir.atlasdb.keyvalue.api.ColumnSelection;
import com.palantir.atlasdb.keyvalue.api.InsufficientConsistencyException;
import com.palantir.atlasdb.keyvalue.api.KeyAlreadyExistsException;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.keyvalue.api.Write;
import com.palantir.atlasdb.keyvalue.cassandra.CqlKeyValueServices.AllTimestampsCollector;
import com.palantir.atlasdb.keyvalue.cassandra.CqlKeyValueServices.Local;
import com.palantir.atlasdb.keyvalue.cassandra.CqlKeyValueServices.Peer;
import com.palantir.atlasdb.keyvalue.cassandra.CqlKeyValueServices.StartTsResultsCollector;
import com.palantir.atlasdb.keyvalue.cassandra.CqlKeyValueServices.TransactionType;
import com.palantir.atlasdb.keyvalue.impl.AbstractKeyValueService;
import com.palantir.atlasdb.keyvalue.impl.Cells;
import com.palantir.atlasdb.keyvalue.impl.GetCandidateCellsForSweepingShim;
import com.palantir.atlasdb.keyvalue.impl.IterablePartitioner;
import com.palantir.atlasdb.keyvalue.impl.KeyValueServices;
import com.palantir.atlasdb.util.AnnotatedCallable;
import com.palantir.atlasdb.util.AnnotationType;
import com.palantir.common.annotation.Idempotent;
import com.palantir.common.base.ClosableIterator;
import com.palantir.common.base.ClosableIterators;
import com.palantir.common.base.Throwables;
import com.palantir.common.visitor.Visitor;
import com.palantir.remoting3.config.ssl.SslSocketFactories;
import com.palantir.util.paging.AbstractPagingIterable;
import com.palantir.util.paging.SimpleTokenBackedResultsPage;
import com.palantir.util.paging.TokenBackedBasicResultsPage;

@SuppressWarnings("VisibilityModifier")
public class CqlKeyValueService extends AbstractKeyValueService {
    private static final Logger log = LoggerFactory.getLogger(CqlKeyValueService.class);

    private Cluster cluster;
    private Cluster longRunningQueryCluster;
    private final CassandraKeyValueServiceConfig config;
    private ConsistencyLevel readConsistency = ConsistencyLevel.LOCAL_QUORUM;
    private final ConsistencyLevel writeConsistency = ConsistencyLevel.EACH_QUORUM;
    private final ConsistencyLevel deleteConsistency = ConsistencyLevel.ALL;

    protected CqlStatementCache cqlStatementCache;
    protected CqlKeyValueServices cqlKeyValueServices;

    Session session;
    Session longRunningQuerySession;
    final CqlFieldNameProvider fieldNameProvider;

    private boolean limitBatchSizesToServerDefaults = false;

    public static CqlKeyValueService create(CassandraKeyValueServiceConfig config) {
        final CqlKeyValueService ret = new CqlKeyValueService(config);
        ret.initializeConnectionPool();
        ret.performInitialSetup();
        return ret;
    }

    protected CqlKeyValueService(CassandraKeyValueServiceConfig config) {
        super(AbstractKeyValueService.createFixedThreadPool("Atlas CQL KVS", config.poolSize()));
        fieldNameProvider = new CqlFieldNameProvider(config);
        this.config = config;
    }

    @SuppressWarnings("CyclomaticComplexity")
    protected void initializeConnectionPool() {
        Collection<InetSocketAddress> configuredHosts = config.servers();
        Cluster.Builder clusterBuilder = Cluster.builder();
        clusterBuilder.addContactPointsWithPorts(configuredHosts);
        clusterBuilder.withClusterName("atlas_cassandra_cluster_" + config.getKeyspaceOrThrow()); // for JMX metrics
        clusterBuilder.withCompression(Compression.LZ4);

        if (config.sslConfiguration().isPresent()) {
            SSLContext sslContext = SslSocketFactories.createSslContext(config.sslConfiguration().get());
            clusterBuilder.withSSL(RemoteEndpointAwareJdkSSLOptions.builder().withSSLContext(sslContext).build());
        } else if (config.ssl().isPresent() && config.ssl().get()) {
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
        // Note we are being purposely datacenter-irreverent here, instead relying on latency alone
        // to approximate what DCAwareRR would do;
        // this is because DCs for Atlas are always quite latency-close and should be used this way,
        // not as if we have some cross-country backup DC.
        LoadBalancingPolicy policy = LatencyAwarePolicy.builder(new RoundRobinPolicy()).build();

        // If user wants, do not automatically add in new nodes to pool (useful during DC migrations / rebuilds)
        if (!config.autoRefreshNodes()) {
            policy = new WhiteListPolicy(policy, configuredHosts);
        }

        // also try and select coordinators who own the data we're talking about to avoid an extra hop,
        // but also shuffle which replica we talk to for a load balancing that comes at the expense
        // of less effective caching
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
            // god dammit datastax what did I do to _you_
            if (e.getMessage().contains("requested compression is not available")) {
                clusterBuilder.withCompression(Compression.NONE);
                cluster = clusterBuilder.build();
                metadata = cluster.getMetadata();
            } else {
                throw e;
            }
        }

        session = cluster.connect();

        clusterBuilder.withSocketOptions(new SocketOptions().setReadTimeoutMillis(
                CassandraConstants.LONG_RUNNING_QUERY_SOCKET_TIMEOUT_MILLIS));
        longRunningQueryCluster = clusterBuilder.build();
        longRunningQuerySession = longRunningQueryCluster.connect();

        cqlStatementCache = new CqlStatementCache(session, longRunningQuerySession);
        cqlKeyValueServices = new CqlKeyValueServices();

        if (log.isInfoEnabled()) {
            StringBuilder hostInfo = new StringBuilder();
            for (Host host : metadata.getAllHosts()) {
                hostInfo.append(String.format(
                        "Datatacenter: %s; Host: %s; Rack: %s%n",
                        host.getDatacenter(),
                        host.getAddress(),
                        host.getRack()));
            }
            log.info("Initialized cassandra cluster using new API with hosts {}, seen keyspaces {}, cluster name {}",
                    hostInfo.toString(),
                    metadata.getKeyspaces(),
                    metadata.getClusterName());
        }
    }

    @Override
    public void close() {
        log.info("Closing CqlKeyValueService");
        session.close();
        cluster.close();
        cqlKeyValueServices.shutdown();
        longRunningQuerySession.close();
        longRunningQueryCluster.close();
        super.close();
    }

    protected void performInitialSetup() {
        Metadata metadata = cluster.getMetadata();

        CassandraVerifier.validatePartitioner(metadata.getPartitioner(), config);

        Set<Peer> peers = CqlKeyValueServices.getPeers(session);

        boolean allNodesHaveSaneNumberOfVnodes = Iterables.all(peers,
                peer -> peer.tokens.size() > CassandraConstants.ABSOLUTE_MINIMUM_NUMBER_OF_TOKENS_PER_NODE);

        // node we're querying doesn't count itself as a peer
        if (peers.size() > 0 && !allNodesHaveSaneNumberOfVnodes) {
            throw new IllegalStateException("All nodes in cluster must have sane number of vnodes"
                    + " (or cluster must consist of a single node).");
        }

        Set<String> dcsInCluster = Sets.newHashSet();
        for (Peer peer : peers) {
            dcsInCluster.add(peer.dataCenter);
            if (peer.dataCenter == null) {
                throw new IllegalStateException("Cluster should not mix datacenter-aware"
                        + " and non-datacenter-aware nodes.");
            }
        }
        dcsInCluster.add(getLocalDataCenter());

        if (metadata.getKeyspace(config.getKeyspaceOrThrow()) == null) {
            // keyspace previously didn't exist; we need to set it up
            createKeyspace(config.getKeyspaceOrThrow(), dcsInCluster);
            return;
        }

        createTables(ImmutableMap.of(AtlasDbConstants.DEFAULT_METADATA_TABLE, AtlasDbConstants.EMPTY_TABLE_METADATA));
    }

    private String getLocalDataCenter() {
        Local local = CqlKeyValueServices.getLocal(session);
        return local.dataCenter;
    }

    @Override
    public Map<Cell, Value> getRows(final TableReference tableRef,
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
                loadWithTs(tableRef, cells, startTs, false, collector, readConsistency);
                return collector.getCollectedResults();
            } catch (Throwable t) {
                throw Throwables.throwUncheckedException(t);
            }
        }

        try {
            return getRowsAllColsInternal(tableRef, rows, startTs);
        } catch (Throwable t) {
            throw Throwables.throwUncheckedException(t);
        }
    }

    private Map<Cell, Value> getRowsAllColsInternal(final TableReference tableRef,
                                                    final Iterable<byte[]> rows,
                                                    final long startTs) throws Exception {
        int rowCount = 0;
        Map<Cell, Value> result = Maps.newHashMap();
        int fetchBatchCount = config.fetchBatchCount();
        List<ResultSetFuture> resultSetFutures = Lists.newArrayListWithExpectedSize(rowCount);

        for (final List<byte[]> batch : Iterables.partition(rows, fetchBatchCount)) {
            rowCount += batch.size();
            String getRowsQuery = String.format("SELECT * FROM %s WHERE %s IN (%s)",
                    getFullTableName(tableRef),
                    fieldNameProvider.row(),
                    Joiner.on(",").join(Iterables.limit(Iterables.cycle("?"), batch.size())));
            PreparedStatement preparedStatement = getPreparedStatement(tableRef, getRowsQuery, session);
            Object[] args = batch.stream().map(ByteBuffer::wrap).toArray();
            resultSetFutures.add(session.executeAsync(preparedStatement.bind(args)));

            for (ResultSetFuture resultSetFuture : resultSetFutures) {
                ResultSet resultSet;
                try {
                    resultSet = resultSetFuture.getUninterruptibly();
                } catch (Throwable t) {
                    throw Throwables.throwUncheckedException(t);
                }
                for (Row row : resultSet.all()) {
                    Cell cell = Cell.create(getRowName(row), getColName(row));
                    if ((getTs(row) < startTs)
                            && (!result.containsKey(cell) || (result.get(cell).getTimestamp() < getTs(row)))) {
                        result.put(
                                Cell.create(getRowName(row), getColName(row)),
                                Value.create(getValue(row), getTs(row)));
                    }
                }
                cqlKeyValueServices.logTracedQuery(getRowsQuery, resultSet, session, cqlStatementCache.normalQuery);
            }
        }
        if (rowCount > fetchBatchCount) {
            log.warn("Rebatched in getRows a call to {} that attempted to multiget {} rows; "
                    + "this may indicate overly-large batching on a higher level.\n{}",
                    tableRef.getQualifiedName(),
                    rowCount,
                    CassandraKeyValueServices.getFilteredStackTrace("com.palantir"));
        }
        return result;
    }

    @Override
    public Map<Cell, Value> get(TableReference tableRef, Map<Cell, Long> timestampByCell) {
        if (timestampByCell.isEmpty()) {
            log.info("Attempted get on '{}' table with empty cells", tableRef);
            return ImmutableMap.of();
        }

        try {
            long firstTs = timestampByCell.values().iterator().next();
            if (Iterables.all(timestampByCell.values(), Predicates.equalTo(firstTs))) {
                StartTsResultsCollector collector = new StartTsResultsCollector(firstTs);
                loadWithTs(tableRef, timestampByCell.keySet(), firstTs, false, collector, readConsistency);
                return collector.getCollectedResults();
            }

            SetMultimap<Long, Cell> cellsByTs = HashMultimap.create();
            Multimaps.invertFrom(Multimaps.forMap(timestampByCell), cellsByTs);
            Builder<Cell, Value> builder = ImmutableMap.builder();
            for (long ts : cellsByTs.keySet()) {
                StartTsResultsCollector collector = new StartTsResultsCollector(ts);
                loadWithTs(tableRef, cellsByTs.get(ts), ts, false, collector, readConsistency);
                builder.putAll(collector.getCollectedResults());
            }
            return builder.build();
        } catch (Throwable t) {
            throw Throwables.throwUncheckedException(t);
        }
    }

    private void loadWithTs(final TableReference tableRef,
                            final Set<Cell> cells,
                            final long startTs,
                            boolean loadAllTs,
                            final Visitor<Multimap<Cell, Value>> visitor,
                            final ConsistencyLevel consistency) throws Exception {
        List<ResultSetFuture> resultSetFutures = Lists.newArrayListWithCapacity(cells.size());
        SortedSetMultimap<byte[], Cell> cellsByCol =
                TreeMultimap.create(UnsignedBytes.lexicographicalComparator(), Ordering.natural());
        for (Cell cell : cells) {
            cellsByCol.put(cell.getColumnName(), cell);
        }
        for (Entry<byte[], SortedSet<Cell>> entry : Multimaps.asMap(cellsByCol).entrySet()) {
            if (entry.getValue().size() > config.fetchBatchCount()) {
                log.warn("A call to {} is performing a multiget {} cells; this may indicate overly-large batching "
                        + "on a higher level.\n{}",
                        tableRef,
                        entry.getValue().size(),
                        CassandraKeyValueServices.getFilteredStackTrace("com.palantir"));
            }
            for (List<Cell> batch : Iterables.partition(entry.getValue(), config.fetchBatchCount())) {
                String rowBinds = Joiner.on(",").join(Iterables.limit(Iterables.cycle('?'), batch.size()));
                final String loadWithTsQuery = "SELECT * FROM " + getFullTableName(tableRef)
                        + " WHERE " + fieldNameProvider.row()
                        + " IN (" + rowBinds
                        + ") AND " + fieldNameProvider.column()
                        + " = ? AND " + fieldNameProvider.timestamp()
                        + " > ?" + (!loadAllTs ? " LIMIT 1" : "");
                final PreparedStatement preparedStatement = getPreparedStatement(tableRef, loadWithTsQuery, session)
                              .setConsistencyLevel(consistency);

                Object[] args = new Object[batch.size() + 2];
                for (int i = 0; i < batch.size(); i++) {
                    args[i] = ByteBuffer.wrap(batch.get(i).getRowName());
                }
                args[batch.size()] = ByteBuffer.wrap(entry.getKey());
                args[batch.size() + 1] = ~startTs;
                ResultSetFuture resultSetFuture = session.executeAsync(
                        preparedStatement.bind(args));
                resultSetFutures.add(resultSetFuture);
            }
        }

        String loggedLoadWithTsQuery = "SELECT * FROM " + getFullTableName(tableRef) + " "
                + "WHERE " + fieldNameProvider.row()
                + " IN (?, ...) AND " + fieldNameProvider.column()
                + " = ? AND " + fieldNameProvider.timestamp()
                + " > ?" + (!loadAllTs ? " LIMIT 1" : "");
        for (ResultSetFuture rsf : resultSetFutures) {
            visitResults(rsf.getUninterruptibly(), visitor, loggedLoadWithTsQuery, loadAllTs);
        }
    }

    // TODO(unknown): use this for insert and delete batch-to-owner-coordinator mapping
    private Map<byte[], List<Cell>> partitionCellsByPrimaryKey(Collection<Cell> cells) {
        return Multimaps.asMap(Multimaps.index(cells, Cells.getRowFunction()));
    }

    private void visitResults(
            ResultSet resultSet,
            Visitor<Multimap<Cell, Value>> visitor,
            String query,
            boolean loadAllTs) {
        List<Row> rows = resultSet.all();
        Multimap<Cell, Value> res;
        if (loadAllTs) {
            res = HashMultimap.create();
        } else {
            res = HashMultimap.create(rows.size(), 1);
        }
        for (Row row : rows) {
            res.put(Cell.create(getRowName(row), getColName(row)),
                    Value.create(getValue(row), getTs(row)));
        }
        cqlKeyValueServices.logTracedQuery(query, resultSet, session, cqlStatementCache.normalQuery);
        visitor.visit(res);
    }

    @Override
    public Map<Cell, Long> getLatestTimestamps(final TableReference tableRef,
                                               Map<Cell, Long> timestampByCell) {
        try {
            return getLatestTimestampsInternal(tableRef, timestampByCell);
        } catch (Throwable t) {
            throw Throwables.throwUncheckedException(t);
        }
    }

    @Override
    public void put(Stream<Write> writes) {
        throw new UnsupportedOperationException("I have another PR out to delete this");
    }

    private Map<Cell, Long> getLatestTimestampsInternal(final TableReference tableRef,
                                                        Map<Cell, Long> timestampByCell) throws Exception {
        int fetchBatchCount = config.fetchBatchCount();
        Iterable<List<Cell>> partitions = Iterables.partition(
                timestampByCell.keySet(),
                fetchBatchCount);
        int numPartitions = (timestampByCell.size() / fetchBatchCount)
                + (timestampByCell.size() % fetchBatchCount > 0 ? 1 : 0);
        List<Future<Map<Cell, Long>>> futures = Lists.newArrayListWithCapacity(numPartitions);
        String loadOnlyTsQuery = "SELECT "
                + fieldNameProvider.row() + ", "
                + fieldNameProvider.column() + ", "
                + fieldNameProvider.timestamp()
                + " FROM " + getFullTableName(tableRef)
                + " WHERE " + fieldNameProvider.row() + " = ?"
                + " AND " + fieldNameProvider.column() + " = ?"
                + " LIMIT 1";
        if (timestampByCell.size() > fetchBatchCount) {
            log.warn("Re-batching in getLatestTimestamps a call to {} that attempted to multiget {} cells; "
                    + "this may indicate overly-large batching on a higher level.\n{}",
                    tableRef,
                    timestampByCell.size(),
                    CassandraKeyValueServices.getFilteredStackTrace("com.palantir"));
        }
        for (final List<Cell> partition : partitions) {
            futures.add(executor.submit(AnnotatedCallable.wrapWithThreadName(AnnotationType.PREPEND,
                    "Atlas CQL getLatestTimestamps partition of " + partition.size(), () -> {
                        PreparedStatement preparedStatement = getPreparedStatement(tableRef, loadOnlyTsQuery,
                                session);
                        preparedStatement.setConsistencyLevel(readConsistency);
                        List<ResultSetFuture> resultSetFutures = Lists.newArrayListWithExpectedSize(
                                partition.size());
                        for (Cell c : partition) {
                            BoundStatement boundStatement = preparedStatement.bind();
                            boundStatement.setBytes(fieldNameProvider.row(), ByteBuffer.wrap(c.getRowName()));
                            boundStatement.setBytes(
                                    fieldNameProvider.column(),
                                    ByteBuffer.wrap(c.getColumnName()));
                            resultSetFutures.add(session.executeAsync(boundStatement));
                        }
                        Map<Cell, Long> res = Maps.newHashMapWithExpectedSize(partition.size());
                        for (ResultSetFuture resultSetFuture : resultSetFutures) {
                            ResultSet resultSet = resultSetFuture.getUninterruptibly();
                            for (Row row : resultSet.all()) {
                                res.put(Cell.create(getRowName(row), getColName(row)), getTs(row));
                            }
                            cqlKeyValueServices.logTracedQuery(
                                    loadOnlyTsQuery,
                                    resultSet,
                                    session,
                                    cqlStatementCache.normalQuery);
                        }
                        return res;
                    })));
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
    public void putWithTimestamps(TableReference tableRef, Multimap<Cell, Value> values) {
        try {
            putInternal(tableRef, values.entries(), TransactionType.NONE);
        } catch (Throwable t) {
            throw Throwables.throwUncheckedException(t);
        }
    }

    @Override
    protected int getMultiPutBatchCount() {
        return config.mutationBatchCount();
    }

    @Override
    public void multiPut(Map<TableReference, ? extends Map<Cell, byte[]>> valuesByTable, long timestamp)
            throws KeyAlreadyExistsException {
        Map<ResultSetFuture, TableReference> resultSetFutures = Maps.newHashMap();
        for (Entry<TableReference, ? extends Map<Cell, byte[]>> e : valuesByTable.entrySet()) {
            final TableReference table = e.getKey();
            // We sort here because some key value stores are more efficient if you store adjacent keys together.
            NavigableMap<Cell, byte[]> sortedMap = ImmutableSortedMap.copyOf(e.getValue());


            Iterable<List<Entry<Cell, byte[]>>> partitions = IterablePartitioner.partitionByCountAndBytes(
                    sortedMap.entrySet(),
                    getMultiPutBatchCount(),
                    getMultiPutBatchSizeBytes(),
                    table,
                    CqlKeyValueServices.MULTIPUT_ENTRY_SIZING_FUNCTION);


            for (final List<Entry<Cell, byte[]>> p : partitions) {
                List<Entry<Cell, Value>> partition = Lists.transform(p,
                        input -> Maps.immutableEntry(input.getKey(), Value.create(input.getValue(), timestamp)));
                resultSetFutures.put(getPutPartitionResultSetFuture(table, partition, TransactionType.NONE), table);
            }
        }

        for (Entry<ResultSetFuture, TableReference> result : resultSetFutures.entrySet()) {
            ResultSet resultSet;
            try {
                resultSet = result.getKey().getUninterruptibly();
                resultSet.all();
            } catch (Throwable t) {
                throw Throwables.throwUncheckedException(t);
            }
            cqlKeyValueServices.logTracedQuery(
                    getPutQuery(result.getValue(), CassandraConstants.NO_TTL),
                    resultSet,
                    session,
                    cqlStatementCache.normalQuery);
        }
    }

    private void putInternal(
            TableReference tableRef,
            Iterable<Map.Entry<Cell, Value>> values,
            TransactionType transactionType) throws Exception {
        putInternal(tableRef, values, transactionType, CassandraConstants.NO_TTL, false);
    }

    protected void putInternal(
            TableReference tableRef,
            Iterable<Map.Entry<Cell, Value>> values,
            TransactionType transactionType,
            int ttl,
            boolean recursive) throws Exception {
        List<ResultSetFuture> resultSetFutures = Lists.newArrayList();
        int mutationBatchCount = config.mutationBatchCount();
        long mutationBatchSizeBytes = limitBatchSizesToServerDefaults
                ? CqlKeyValueServices.UNCONFIGURED_DEFAULT_BATCH_SIZE_BYTES
                : config.mutationBatchSizeBytes();
        for (List<Entry<Cell, Value>> partition : IterablePartitioner.partitionByCountAndBytes(
                values,
                mutationBatchCount,
                mutationBatchSizeBytes,
                tableRef,
                CqlKeyValueServices.PUT_ENTRY_SIZING_FUNCTION)) {
            resultSetFutures.add(getPutPartitionResultSetFuture(tableRef, partition, transactionType));
        }

        final String putQuery = getPutQueryForPossibleTransaction(tableRef, transactionType);
        for (ResultSetFuture resultSetFuture : resultSetFutures) {
            ResultSet resultSet;
            try {
                resultSet = resultSetFuture.getUninterruptibly();
                resultSet.all();
                cqlKeyValueServices.logTracedQuery(putQuery, resultSet, session, cqlStatementCache.normalQuery);
                if (!resultSet.wasApplied()) {
                    throw new KeyAlreadyExistsException("This transaction row already exists: " + putQuery);
                }
            } catch (InvalidQueryException e) {
                if (e.getMessage().contains("Batch too large") && !recursive) {
                    log.error("Attempted a put to {} that the Cassandra server"
                            + " deemed to be too large to accept. Batch sizes on the Atlas-side"
                            + " have been artificially lowered to the Cassandra default maximum batch sizes.",
                            tableRef);
                    limitBatchSizesToServerDefaults = true;
                    try {
                        putInternal(tableRef, values, transactionType, ttl, true);
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

    private String getPutQueryForPossibleTransaction(TableReference tableRef, TransactionType transactionType) {
        return getPutQueryForPossibleTransaction(tableRef, transactionType, CassandraConstants.NO_TTL);
    }

    private String getPutQueryForPossibleTransaction(
            TableReference tableRef,
            TransactionType transactionType,
            int ttl) {
        return transactionType.equals(TransactionType.LIGHTWEIGHT_TRANSACTION_REQUIRED)
                ? getPutUnlessExistsQuery(tableRef, ttl)
                : getPutQuery(tableRef, ttl);
    }

    private String getPutUnlessExistsQuery(TableReference tableRef, int ttl) {
        if (ttl <= 0) {
            return getPutQuery(tableRef, CassandraConstants.NO_TTL) + " IF NOT EXISTS";
        } else {
            return getPutQuery(tableRef, CassandraConstants.NO_TTL) + " IF NOT EXISTS USING TTL " + ttl;
        }
    }

    protected String getPutQuery(TableReference tableName, int ttl) {
        String putQuery = "INSERT INTO " + getFullTableName(tableName)
                + " (" + fieldNameProvider.row() + ", "
                + fieldNameProvider.column() + ", "
                + fieldNameProvider.timestamp() + ", "
                + fieldNameProvider.value() + ")"
                + " VALUES (?, ?, ?, ?)";
        if (ttl >= 0) {
            putQuery += " USING TTL " + ttl;
        }
        return putQuery;
    }

    protected ResultSetFuture getPutPartitionResultSetFuture(TableReference tableRef,
                                                             List<Entry<Cell, Value>> partition,
                                                             TransactionType transactionType) {
        return getPutPartitionResultSetFuture(tableRef, partition, transactionType, CassandraConstants.NO_TTL);
    }

    protected ResultSetFuture getPutPartitionResultSetFuture(TableReference tableRef,
                                                             List<Entry<Cell, Value>> partition,
                                                             TransactionType transactionType,
                                                             int ttl) {
        PreparedStatement preparedStatement = getPreparedStatement(
                tableRef,
                getPutQueryForPossibleTransaction(tableRef, transactionType, ttl),
                session);
        preparedStatement.setConsistencyLevel(writeConsistency);

        // Be mindful when using the atomicity semantics of UNLOGGED batch statements.
        // This usage should be okay, as the KVS.multiPut explicitly does not guarantee
        // atomicity across cells (nor batch isolation, which we also cannot provide)
        BatchStatement batchStatement = new BatchStatement(BatchStatement.Type.UNLOGGED);
        if (shouldTraceQuery(tableRef)) {
            batchStatement.enableTracing();
        }
        for (Entry<Cell, Value> e : partition) {
            BoundStatement boundStatement = preparedStatement.bind();
            boundStatement.setBytes(fieldNameProvider.row(), ByteBuffer.wrap(e.getKey().getRowName()));
            boundStatement.setBytes(fieldNameProvider.column(), ByteBuffer.wrap(e.getKey().getColumnName()));
            boundStatement.setLong(fieldNameProvider.timestamp(), ~e.getValue().getTimestamp());
            boundStatement.setBytes(fieldNameProvider.value(), ByteBuffer.wrap(e.getValue().getContents()));
            if (partition.size() > 1) {
                batchStatement.add(boundStatement);
            } else {
                return session.executeAsync(boundStatement);
            }
        }
        return session.executeAsync(batchStatement);
    }

    @Override
    public void truncateTable(final TableReference tableRef) {
        truncateTables(ImmutableSet.of(tableRef));
    }

    @Override
    public void truncateTables(final Set<TableReference> tablesToTruncate) {
        String truncateQuery = "TRUNCATE %s"; // full table name (ks.cf)

        for (TableReference tableRef : tablesToTruncate) {
            BoundStatement truncateStatement =
                    getPreparedStatement(
                            tableRef,
                            String.format(truncateQuery, getFullTableName(tableRef)),
                            longRunningQuerySession)
                            .setConsistencyLevel(ConsistencyLevel.ALL)
                            .bind();

            try {
                ResultSet resultSet = longRunningQuerySession.execute(truncateStatement);
                cqlKeyValueServices.logTracedQuery(truncateQuery, resultSet, session, cqlStatementCache.normalQuery);
            } catch (com.datastax.driver.core.exceptions.UnavailableException e) {
                throw new InsufficientConsistencyException("Truncating tables requires all Cassandra"
                        + " nodes to be up and available.", e);
            }
        }

        CqlKeyValueServices.waitForSchemaVersionsToCoalesce(
                "truncateTables(" + tablesToTruncate.size() + " tables)",
                this);
    }

    @Override
    public void delete(final TableReference tableRef, final Multimap<Cell, Long> keys) {
        int cellCount = 0;
        String deleteQuery = "DELETE FROM " + getFullTableName(tableRef)
                + " WHERE " + fieldNameProvider.row() + " = ?"
                + " AND " + fieldNameProvider.column() + " = ?"
                + " AND " + fieldNameProvider.timestamp() + " = ?";
        int fetchBatchCount = config.fetchBatchCount();
        for (final List<Cell> batch : Iterables.partition(keys.keySet(), fetchBatchCount)) {
            cellCount += batch.size();
            PreparedStatement deleteStatement = getPreparedStatement(tableRef, deleteQuery, longRunningQuerySession)
                    .setConsistencyLevel(deleteConsistency);
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
                cqlKeyValueServices.logTracedQuery(deleteQuery, resultSet, session, cqlStatementCache.normalQuery);
            }
        }
        if (cellCount > fetchBatchCount) {
            log.warn("Rebatched in delete a call to {} that attempted to delete {} cells; "
                    + "this may indicate overly-large batching on a higher level.\n{}",
                    tableRef, cellCount, CassandraKeyValueServices.getFilteredStackTrace("com.palantir"));
        }
    }

    // TODO(unknown): after cassandra change: handle multiRanges
    @Override
    @Idempotent
    public Map<RangeRequest, TokenBackedBasicResultsPage<RowResult<Value>, byte[]>> getFirstBatchForRanges(
            TableReference tableRef,
            Iterable<RangeRequest> rangeRequests,
            long timestamp) {
        int concurrency = config.rangesConcurrency();
        return KeyValueServices.getFirstBatchForRangesUsingGetRangeConcurrent(
                executor,
                this,
                tableRef,
                rangeRequests,
                timestamp,
                concurrency);
    }

    // TODO(unknown): after cassandra change: handle reverse ranges
    // TODO(unknown): after cassandra change: handle column filtering
    @Override
    @Idempotent
    public ClosableIterator<RowResult<Value>> getRange(TableReference tableRef,
                                                       final RangeRequest rangeRequest,
                                                       final long timestamp) {
        return getRangeWithPageCreator(
                tableRef,
                rangeRequest,
                timestamp,
                readConsistency,
                ValueExtractor::create);
    }

    @Override
    @Idempotent
    public ClosableIterator<RowResult<Set<Long>>> getRangeOfTimestamps(TableReference tableRef,
                                                                       RangeRequest rangeRequest,
                                                                       long timestamp) {
        return getRangeWithPageCreator(
                tableRef,
                rangeRequest,
                timestamp,
                deleteConsistency,
                TimestampExtractor::new);
    }

    @Override
    public ClosableIterator<List<CandidateCellForSweeping>> getCandidateCellsForSweeping(TableReference tableRef,
            CandidateCellForSweepingRequest request) {
        return new GetCandidateCellsForSweepingShim(this).getCandidateCellsForSweeping(tableRef, request);
    }

    public <T> ClosableIterator<RowResult<T>> getRangeWithPageCreator(
            TableReference tableRef,
            RangeRequest rangeRequest,
            long timestamp,
            com.datastax.driver.core.ConsistencyLevel consistency,
            Supplier<ResultsExtractor<T>> resultsExtractor) {
        if (rangeRequest.isReverse()) {
            throw new UnsupportedOperationException();
        }
        if (rangeRequest.isEmptyRange()) {
            return ClosableIterators.wrap(ImmutableList.<RowResult<T>>of().iterator());
        }
        final int batchHint = rangeRequest.getBatchHint() == null ? 100 : rangeRequest.getBatchHint();
        final ColumnSelection selection = rangeRequest.getColumnNames().isEmpty()
                ? ColumnSelection.all()
                : ColumnSelection.create(rangeRequest.getColumnNames());
        final byte[] endExclusive = rangeRequest.getEndExclusive();
        final StringBuilder bindQuery = new StringBuilder();
        bindQuery.append("SELECT * FROM " + getFullTableName(tableRef) + " WHERE token("
                + fieldNameProvider.row() + ") >= token(?) ");
        if (endExclusive.length > 0) {
            bindQuery.append("AND token(" + fieldNameProvider.row() + ") < token(?) ");
        }
        bindQuery.append("LIMIT " + batchHint);
        final String getLastRowQuery = "SELECT * FROM " + getFullTableName(tableRef) + " WHERE "
                + fieldNameProvider.row() + " = ?";
        return ClosableIterators.wrap(
                new AbstractPagingIterable<RowResult<T>, TokenBackedBasicResultsPage<RowResult<T>, byte[]>>() {
                    @Override
                    protected TokenBackedBasicResultsPage<RowResult<T>, byte[]> getFirstPage()
                            throws Exception {
                        return getPage(rangeRequest.getStartInclusive());
                    }

                    @Override
                    protected TokenBackedBasicResultsPage<RowResult<T>, byte[]> getNextPage(
                            TokenBackedBasicResultsPage<RowResult<T>, byte[]> previous)
                            throws Exception {
                        return getPage(previous.getTokenForNextPage());
                    }

                    TokenBackedBasicResultsPage<RowResult<T>, byte[]> getPage(final byte[] startKey)
                            throws Exception {
                        BoundStatement boundStatement = getPreparedStatement(tableRef, bindQuery.toString(), session)
                                .setConsistencyLevel(consistency)
                                .bind();

                        boundStatement.setBytes(0, ByteBuffer.wrap(startKey));
                        if (endExclusive.length > 0) {
                            boundStatement.setBytes(1, ByteBuffer.wrap(endExclusive));
                        }
                        ResultSet resultSet = session.execute(boundStatement);
                        List<Row> rows = Lists.newArrayList(resultSet.all());
                        cqlKeyValueServices.logTracedQuery(
                                bindQuery.toString(), resultSet, session, cqlStatementCache.normalQuery);
                        byte[] maxRow = null;
                        ResultsExtractor<T> extractor = resultsExtractor.get();
                        for (Row row : rows) {
                            byte[] rowName = getRowName(row);
                            if (maxRow == null) {
                                maxRow = rowName;
                            } else {
                                maxRow = PtBytes.BYTES_COMPARATOR.max(maxRow, rowName);
                            }
                        }
                        if (maxRow == null) {
                            return new SimpleTokenBackedResultsPage<>(
                                    endExclusive,
                                    ImmutableList.of(),
                                    false);
                        }
                        // get the rest of the last row
                        BoundStatement boundLastRow = getPreparedStatement(tableRef, getLastRowQuery, session).bind();

                        boundLastRow.setBytes(fieldNameProvider.row(), ByteBuffer.wrap(maxRow));
                        try {
                            resultSet = session.execute(boundLastRow);
                        } catch (com.datastax.driver.core.exceptions.UnavailableException e) {
                            throw new InsufficientConsistencyException("This operation requires all Cassandra"
                                    + " nodes to be up and available.", e);
                        }
                        rows.addAll(resultSet.all());
                        cqlKeyValueServices.logTracedQuery(
                                getLastRowQuery, resultSet, session, cqlStatementCache.normalQuery);
                        for (Row row : rows) {
                            extractor.internalExtractResult(
                                    timestamp,
                                    selection,
                                    getRowName(row),
                                    getColName(row),
                                    getValue(row),
                                    getTs(row));
                        }
                        SortedMap<byte[], SortedMap<byte[], T>> resultsByRow =
                                Cells.breakCellsUpByRow(extractor.asMap());
                        return ResultsExtractor.getRowResults(endExclusive, maxRow, resultsByRow);
                    }
            }.iterator());
    }

    @Override
    public void dropTable(final TableReference tableRef) {
        dropTables(ImmutableSet.of(tableRef));
    }

    @Override
    public void dropTables(final Set<TableReference> tablesToDrop) {
        String dropQuery = "DROP TABLE IF EXISTS %s"; // full table name (ks.cf)

        for (TableReference tableRef : tablesToDrop) {
            BoundStatement dropStatement =
                    getPreparedStatement(
                            tableRef,
                            String.format(dropQuery, getFullTableName(tableRef)),
                            longRunningQuerySession)
                            .setConsistencyLevel(ConsistencyLevel.ALL)
                            .bind();
            try {
                ResultSet resultSet = longRunningQuerySession.execute(dropStatement);
                cqlKeyValueServices.logTracedQuery(dropQuery, resultSet, session, cqlStatementCache.normalQuery);
            } catch (com.datastax.driver.core.exceptions.UnavailableException e) {
                throw new InsufficientConsistencyException("Dropping tables requires all Cassandra"
                        + " nodes to be up and available.", e);
            }
        }

        CqlKeyValueServices.waitForSchemaVersionsToCoalesce("dropTables(" + tablesToDrop.size() + " tables)", this);

        KeyValueServices.put(this, AtlasDbConstants.DEFAULT_METADATA_TABLE, Maps.toMap(
                        Lists.transform(Lists.newArrayList(tablesToDrop), CqlKeyValueServices::getMetadataCell),
                        Functions.constant(PtBytes.EMPTY_BYTE_ARRAY)),
                System.currentTimeMillis());
    }

    private void createKeyspace(String keyspaceName, Set<String> dcsInCluster) {
        String createKeyspace = "create keyspace if not exists %s with replication = %s and durable_writes = true";

        String replication = dcsInCluster.stream()
                .map(datacenter -> "'" + datacenter + "' : " + config.replicationFactor())
                .collect(Collectors.joining(", ", "{ 'class' : 'NetworkTopologyStrategy', ", "} "));

        longRunningQuerySession.execute(
                getPreparedStatement(CassandraConstants.NO_TABLE,
                        String.format(createKeyspace, keyspaceName, replication),
                        longRunningQuerySession)
                        .setConsistencyLevel(ConsistencyLevel.ALL)
                        .bind());

        CqlKeyValueServices.waitForSchemaVersionsToCoalesce("Initial creation of the Atlas keyspace", this);
    }

    @Override
    public void createTable(final TableReference tableRef, byte[] tableMetadata) {
        createTables(ImmutableMap.of(tableRef, tableMetadata));
    }

    @Override
    public void createTables(final Map<TableReference, byte[]> tableRefsToTableMetadata) {
        Collection<com.datastax.driver.core.TableMetadata> tables = cluster.getMetadata()
                .getKeyspace(config.getKeyspaceOrThrow()).getTables();
        Set<TableReference> existingTables = Sets.newHashSet(Iterables.transform(tables,
                input -> TableReference.createUnsafe(input.getName())));

        // ScrubberStore likes to call createTable before our setup gets called...
        if (!existingTables.contains(AtlasDbConstants.DEFAULT_METADATA_TABLE)) {
            cqlKeyValueServices.createTableWithSettings(
                    AtlasDbConstants.DEFAULT_METADATA_TABLE,
                    AtlasDbConstants.EMPTY_TABLE_METADATA,
                    this);
        }

        Set<TableReference> tablesToCreate = Sets.difference(tableRefsToTableMetadata.keySet(), existingTables);
        for (TableReference tableRef : tablesToCreate) {
            try {
                cqlKeyValueServices.createTableWithSettings(tableRef, tableRefsToTableMetadata.get(tableRef), this);
            } catch (com.datastax.driver.core.exceptions.UnavailableException e) {
                throw new InsufficientConsistencyException("Creating tables requires all Cassandra"
                        + " nodes to be up and available.", e);
            }
        }

        if (!tablesToCreate.isEmpty()) {
            CqlKeyValueServices.waitForSchemaVersionsToCoalesce("createTables(" + tableRefsToTableMetadata.size()
                    + " tables)", this);
        }

        internalPutMetadataForTables(tableRefsToTableMetadata, false);
    }

    @Override
    public Set<TableReference> getAllTableNames() {
        List<Row> rows = session.execute(cqlStatementCache.normalQuery.getUnchecked(
                "SELECT columnfamily_name FROM system.schema_columnfamilies WHERE keyspace_name = ?")
                .bind(config.getKeyspaceOrThrow()))
                .all();

        Set<TableReference> existingTables = Sets.newHashSet(Iterables.transform(rows,
                row -> TableReference.fromInternalTableName(row.getString("columnfamily_name"))));

        return Sets.filter(existingTables, tableRef ->
                !tableRef.getQualifiedName().startsWith("_")
                        || tableRef.getQualifiedName().startsWith(AtlasDbConstants.NAMESPACE_PREFIX));
    }

    @Override
    public byte[] getMetadataForTable(TableReference tableRef) {
        Cell cell = CqlKeyValueServices.getMetadataCell(tableRef);
        Value value = get(AtlasDbConstants.DEFAULT_METADATA_TABLE, ImmutableMap.of(cell, Long.MAX_VALUE)).get(
                cell);
        if (value == null) {
            return new byte[0];
        } else {
            return value.getContents();
        }
    }

    @Override
    public void putMetadataForTable(final TableReference tableRef, final byte[] meta) {
        putMetadataForTables(ImmutableMap.of(tableRef, meta));
    }

    @Override
    public void putMetadataForTables(final Map<TableReference, byte[]> tableRefToMetadata) {
        internalPutMetadataForTables(tableRefToMetadata, true);
    }

    private void internalPutMetadataForTables(
            Map<TableReference, byte[]> tableNameToMetadata,
            boolean possiblyNeedToPerformSettingsChanges) {
        Map<Cell, byte[]> cellToMetadata = Maps.newHashMap();
        for (Entry<TableReference, byte[]> tableEntry : tableNameToMetadata.entrySet()) {
            byte[] existingMetadata = getMetadataForTable(tableEntry.getKey());
            if (!Arrays.equals(existingMetadata, tableEntry.getValue())) {
                cellToMetadata.put(CqlKeyValueServices.getMetadataCell(tableEntry.getKey()), tableEntry.getValue());
                if (possiblyNeedToPerformSettingsChanges) {
                    CqlKeyValueServices.setSettingsForTable(tableEntry.getKey(), tableEntry.getValue(), this);
                }
            }
        }
        if (!cellToMetadata.isEmpty()) {
            KeyValueServices.put(this,
                    AtlasDbConstants.DEFAULT_METADATA_TABLE, cellToMetadata, System.currentTimeMillis());
            if (possiblyNeedToPerformSettingsChanges) {
                CqlKeyValueServices.waitForSchemaVersionsToCoalesce(
                        "putMetadataForTables(" + tableNameToMetadata.size() + " tables)", this);
            }
        }
    }
    @Override
    public void addGarbageCollectionSentinelValues(TableReference tableRef, Iterable<Cell> cells) {
        try {
            final Value value = Value.create(new byte[0], Value.INVALID_VALUE_TIMESTAMP);
            putInternal(
                    tableRef,
                    Iterables.transform(cells, cell -> Maps.immutableEntry(cell, value)), TransactionType.NONE);
        } catch (Throwable t) {
            throw Throwables.throwUncheckedException(t);
        }
    }

    @Override
    public Multimap<Cell, Long> getAllTimestamps(TableReference tableRef, Set<Cell> cells, long ts) {
        AllTimestampsCollector collector = new AllTimestampsCollector();
        try {
            loadWithTs(tableRef, cells, ts, true, collector, deleteConsistency);
        } catch (com.datastax.driver.core.exceptions.UnavailableException e) {
            throw new InsufficientConsistencyException("Get all timestamps requires all Cassandra"
                    + " nodes to be up and available.", e);
        } catch (Throwable t) {
            throw Throwables.throwUncheckedException(t);
        }
        return collector.getCollectedResults();
    }

    @Override
    public void putUnlessExists(TableReference tableRef, Map<Cell, byte[]> values)
            throws KeyAlreadyExistsException {
        try {
            putInternal(
                    tableRef,
                    KeyValueServices.toConstantTimestampValues(values.entrySet(), AtlasDbConstants.TRANSACTION_TS),
                    TransactionType.LIGHTWEIGHT_TRANSACTION_REQUIRED);
        } catch (Throwable t) {
            throw Throwables.throwUncheckedException(t);
        }
    }

    @Override
    public boolean supportsCheckAndSet() {
        return false;
    }

    @Override
    public void checkAndSet(CheckAndSetRequest checkAndSetRequest) {
        throw new UnsupportedOperationException("Check and set is not yet supported for CQL KeyValueService");
    }

    String getFullTableName(TableReference tableRef) {
        return config.getKeyspaceOrThrow() + ".\"" + internalTableName(tableRef) + "\"";
    }

    @Override
    public void compactInternally(TableReference tableRef) {
        /* no-op */
    }

    @Override
    public ClusterAvailabilityStatus getClusterAvailabilityStatus() {
        throw new UnsupportedOperationException("getClusterAvailabilityStatus has not been implemented for CQL KVS");
    }

    private void alterTableForCompaction(TableReference tableRef, int gcGraceSeconds, float tombstoneThreshold) {
        log.trace("Altering table {} to have gc_grace_seconds={} and tombstone_threshold={}",
                tableRef, gcGraceSeconds, String.format("%.2f", tombstoneThreshold));
        String alterTableQuery =
                "ALTER TABLE " + getFullTableName(tableRef)
                        + " WITH gc_grace_seconds = " + gcGraceSeconds
                        + " and compaction = {'class':'" + CassandraConstants.LEVELED_COMPACTION_STRATEGY
                        + "', 'tombstone_threshold':"
                        + tombstoneThreshold + "};";

        BoundStatement alterTable = getPreparedStatement(tableRef, alterTableQuery, longRunningQuerySession)
                .setConsistencyLevel(ConsistencyLevel.ALL)
                .bind();
        ResultSet resultSet;
        try {
            resultSet = longRunningQuerySession.execute(alterTable);
        } catch (UnavailableException e) {
            throw new InsufficientConsistencyException("Alter table requires all Cassandra"
                    + " nodes to be up and available.", e);
        } catch (Exception e) {
            throw Throwables.throwUncheckedException(e);
        }
        cqlKeyValueServices.logTracedQuery(alterTableQuery, resultSet, session, cqlStatementCache.normalQuery);
    }

    PreparedStatement getPreparedStatement(TableReference tableRef, String query, Session sessionToBeUsed) {
        try {
            PreparedStatement statement;

            if (sessionToBeUsed == longRunningQuerySession) {
                statement =  cqlStatementCache.longRunningQuery.get(query).enableTracing();
            } else {
                statement = cqlStatementCache.normalQuery.get(query).enableTracing();
            }

            if (shouldTraceQuery(tableRef)) {
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

    private boolean shouldTraceQuery(TableReference tableRef) {
        return tracingPrefs.shouldTraceQuery(tableRef.getQualifiedName());
    }

    byte[] getRowName(Row row) {
        return CassandraKeyValueServices.getBytesFromByteBuffer(row.getBytes(fieldNameProvider.row()));
    }

    byte[] getColName(Row row) {
        return CassandraKeyValueServices.getBytesFromByteBuffer(row.getBytes(fieldNameProvider.column()));
    }

    long getTs(Row row) {
        return ~row.getLong(fieldNameProvider.timestamp());
    }

    byte[] getValue(Row row) {
        return CassandraKeyValueServices.getBytesFromByteBuffer(row.getBytes(fieldNameProvider.value()));
    }
}
