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

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Set;
import java.util.SortedMap;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeoutException;

import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.Cassandra.Client;
import org.apache.cassandra.thrift.CfDef;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.KsDef;
import org.apache.cassandra.thrift.NotFoundException;
import org.apache.commons.lang.Validate;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.ExecutionInfo;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.HostDistance;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.PoolingOptions;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ProtocolOptions.Compression;
import com.datastax.driver.core.QueryOptions;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.driver.core.exceptions.UnavailableException;
import com.datastax.driver.core.policies.LatencyAwarePolicy;
import com.datastax.driver.core.policies.RoundRobinPolicy;
import com.datastax.driver.core.policies.TokenAwarePolicy;
import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicates;
import com.google.common.base.Strings;
import com.google.common.base.Supplier;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ArrayListMultimap;
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
import com.palantir.atlasdb.keyvalue.impl.AbstractKeyValueService;
import com.palantir.atlasdb.keyvalue.impl.Cells;
import com.palantir.atlasdb.keyvalue.impl.KeyValueServices;
import com.palantir.atlasdb.table.description.TableMetadata;
import com.palantir.common.annotation.Idempotent;
import com.palantir.common.base.ClosableIterator;
import com.palantir.common.base.ClosableIterators;
import com.palantir.common.base.Throwables;
import com.palantir.common.concurrent.NamedThreadFactory;
import com.palantir.common.concurrent.PTExecutors;
import com.palantir.common.pooling.PoolingContainer;
import com.palantir.util.Visitor;
import com.palantir.util.paging.AbstractPagingIterable;
import com.palantir.util.paging.SimpleTokenBackedResultsPage;
import com.palantir.util.paging.TokenBackedBasicResultsPage;


public class CQLKeyValueService extends AbstractKeyValueService {
    private static final Logger log = LoggerFactory.getLogger(CQLKeyValueService.class);

    private static final Function<Entry<Cell, byte[]>, Long> ENTRY_SIZING_FUNCTION = new Function<Entry<Cell, byte[]>, Long>() {
        @Override
        public Long apply(Entry<Cell, byte[]> entry) {
            long totalSize = 0;
            totalSize += entry.getValue().length;
            totalSize += Cells.getApproxSizeOfCell(entry.getKey());
            return totalSize;
        }
    };

    private static final Set<String> HIDDEN_TABLES = ImmutableSet.of(
            CassandraConstants.METADATA_TABLE, CassandraTimestampBoundStore.TIMESTAMP_TABLE);

    private Cluster cluster;
    private Session session;

    private final LoadingCache<String, PreparedStatement> statementCache = CacheBuilder.newBuilder()
            .build(new CacheLoader<String, PreparedStatement>() {
                @Override
                public PreparedStatement load(String query) {
                    return session.prepare(query);
                }
            });

    private final CassandraKeyValueServiceConfigManager configManager;
    private final CassandraClientPoolingManager cassandraClientPoolingManager;
    private final CassandraJMXCompactionManager compactionManager;
    private final ManyClientPoolingContainer containerPoolToUpdate;
    private final PoolingContainer<Client> clientPool;
    private final ScheduledExecutorService hostRefreshExecutor = PTExecutors.newScheduledThreadPool(1);

    private ConsistencyLevel readConsistency = ConsistencyLevel.LOCAL_QUORUM;
    private final ConsistencyLevel writeConsistency = ConsistencyLevel.EACH_QUORUM;
    private final ConsistencyLevel deleteConsistency = ConsistencyLevel.ALL;

    private static final String ROW_NAME = "key";
    private static final String COL_NAME_COL = "column1";
    private static final String TS_COL = "column2";
    private static final String VALUE_COL = "value";

    private static final long TRANSACTION_TS = 0L;

    private static enum TransactionType {
        NONE,
        LIGHTWEIGHT_TRANSACTION_REQUIRED
    }

    public static CQLKeyValueService create(CassandraKeyValueServiceConfigManager configManager) {
        final CassandraKeyValueServiceConfig config = configManager.getConfig();
        Preconditions.checkArgument(!config.servers().isEmpty(), "servers set was empty");
        final CQLKeyValueService ret = new CQLKeyValueService(configManager);
        try {
            ret.initializeFromFreshInstance(ImmutableList.copyOf(config.servers()), config.replicationFactor());
            ret.getPoolingManager().submitHostRefreshTask();
        } catch (Throwable t) {
            throw Throwables.throwUncheckedException(t);
        }
        return ret;
    }

    private CQLKeyValueService(CassandraKeyValueServiceConfigManager configManager) {
        super(PTExecutors.newFixedThreadPool(configManager.getConfig().poolSize() * 2, new NamedThreadFactory(
                "CQLKeyValueService",
                false)));
        this.configManager = configManager;
        this.containerPoolToUpdate = ManyClientPoolingContainer.create(configManager.getConfig());
        this.clientPool = new RetriablePoolingContainer(this.containerPoolToUpdate);
        this.cassandraClientPoolingManager = new CassandraClientPoolingManager(containerPoolToUpdate, clientPool, configManager);
        this.compactionManager = CassandraJMXCompactionManager.newInstance(configManager.getConfig());
    }

    private CassandraClientPoolingManager getPoolingManager() {
        return cassandraClientPoolingManager;
    }

    private void initializeConnectionPoolWithNewAPI(Set<String> hosts) {
        Set<InetAddress> addresses = Sets.newHashSetWithExpectedSize(hosts.size());
        for (String host : hosts) {
            try {
                addresses.add(InetAddress.getByName(host));
            } catch (UnknownHostException e) {
                log.error(String.format("Couldn't lookup host %s", host), e);
            }
        }

        final CassandraKeyValueServiceConfig config = configManager.getConfig();

        Cluster.Builder clusterBuilder = Cluster.builder();
        clusterBuilder.addContactPoints(addresses);
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
        LatencyAwarePolicy latencyAwarePolicy = LatencyAwarePolicy.builder(new RoundRobinPolicy()).build();

        // also try and select coordinators who own the data we're talking about to avoid an extra hop,
        // but also shuffle which replica we talk to for a load balancing that comes at the expense of less effective caching
        TokenAwarePolicy masterLoadBalancingPolicy = new TokenAwarePolicy(latencyAwarePolicy, true);

        clusterBuilder.withLoadBalancingPolicy(masterLoadBalancingPolicy);

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

        String partitioner = metadata.getPartitioner();
        String clusterName = metadata.getClusterName();
        List<KeyspaceMetadata> keyspaces = metadata.getKeyspaces();

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
                keyspaces.toString(),
                clusterName));

        if (!config.safetyDisabled()) {
            Validate.isTrue(CassandraConstants.ALLOWED_PARTITIONERS.contains(partitioner), "partitioner is: " + partitioner);
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
        clientPool.shutdownPooling();
        hostRefreshExecutor.shutdown();
        traceRetrievalExec.shutdown();
        configManager.shutdown();
        if (compactionManager != null) {
            compactionManager.close();
        }
        super.close();
    }

    private void initializeFromFreshInstance(List<String> hosts, int replicationFactor) {
        Map<String, Throwable> errorsByHost = Maps.newHashMap();
        initializeConnectionPoolWithNewAPI(ImmutableSet.copyOf(hosts));

        final CassandraKeyValueServiceConfig config = configManager.getConfig();
        int port = config.port();
        String keyspace = config.keyspace();
        boolean ssl = config.ssl();
        boolean safetyDisabled = config.safetyDisabled();
        for (String host : hosts) {
            Cassandra.Client client = null;
            try {
                int socketTimeoutMillis = config.socketTimeoutMillis();
                int socketQueryTimeoutMillis = config.socketQueryTimeoutMillis();

                client = CassandraClientPoolingContainer.getClientInternal(host, port, ssl, socketTimeoutMillis, socketQueryTimeoutMillis);
                String partitioner = client.describe_partitioner();
                if (!safetyDisabled) {
                    Validate.isTrue(CassandraConstants.ALLOWED_PARTITIONERS.contains(partitioner), "partitioner is: " + partitioner);
                }
                KsDef ks = null;
                try {
                    ks = client.describe_keyspace(keyspace);
                } catch (NotFoundException e) {
                    // need to create key space
                }

                Set<String> currentHosts = cassandraClientPoolingManager.getCurrentHostsFromServer(client);
                cassandraClientPoolingManager.setHostsToCurrentHostNames(currentHosts);
                if (ks != null) {
                    CassandraVerifier.checkAndSetReplicationFactor(
                            client,
                            ks,
                            false,
                            replicationFactor,
                            safetyDisabled);
                    lowerConsistencyWhenSafe(client, ks, replicationFactor);
                    // Can't call system_update_keyspace to update replication factor if CfDefs are
                    // set
                    ks.setCf_defs(ImmutableList.<CfDef> of());
                    client.system_update_keyspace(ks);
                    client.set_keyspace(keyspace);
                    CassandraVerifier.sanityCheckRingConsistency(
                            currentHosts,
                            port,
                            keyspace,
                            ssl,
                            safetyDisabled,
                            socketTimeoutMillis,
                            socketQueryTimeoutMillis);
                    createTables(ImmutableMap.of(CassandraConstants.METADATA_TABLE, Integer.MAX_VALUE));
                    return;
                }
                ks = new KsDef(
                        keyspace,
                        CassandraConstants.NETWORK_STRATEGY,
                        ImmutableList.<CfDef> of());
                CassandraVerifier.checkAndSetReplicationFactor(
                        client,
                        ks,
                        true,
                        replicationFactor,
                        safetyDisabled);
                lowerConsistencyWhenSafe(client, ks, replicationFactor);
                ks.setDurable_writes(true);
                client.system_add_keyspace(ks);
                client.set_keyspace(keyspace);
                CassandraVerifier.sanityCheckRingConsistency(
                        currentHosts,
                        port,
                        keyspace,
                        ssl,
                        safetyDisabled,
                        socketTimeoutMillis,
                        socketQueryTimeoutMillis);
                createTables(ImmutableMap.of(CassandraConstants.METADATA_TABLE, Integer.MAX_VALUE));
                return;
            } catch (TException e) {
                log.warn("failed to connect to host: " + host, e);
                errorsByHost.put(host, e);
            } finally {
                if (client != null) {
                    client.getOutputProtocol().getTransport().close();
                }
            }
        }
        throw new IllegalStateException(CassandraKeyValueServices.buildErrorMessage(
                "Could not connect to any Cassandra hosts",
                errorsByHost));
    }

    private void lowerConsistencyWhenSafe(Client client, KsDef ks, int desiredRf) {
        final CassandraKeyValueServiceConfig config = configManager.getConfig();
        Set<String> dcs;
        try {
            dcs = CassandraVerifier.sanityCheckDatacenters(client, desiredRf, config.safetyDisabled());
        } catch (InvalidRequestException e) {
            return;
        } catch (TException e) {
            return;
        }

        Map<String, String> strategyOptions = Maps.newHashMap(ks.getStrategy_options());
        if (dcs.size() == 1) {
            String dc = dcs.iterator().next();
            if (strategyOptions.get(dc) != null) {
                int currentRF = Integer.parseInt(strategyOptions.get(dc));
                if (currentRF == desiredRf) {
                    if (currentRF == 2) {
                        log.info("Setting Read Consistency to ONE, as cluster has only one datacenter at RF2.");
                        readConsistency = ConsistencyLevel.ONE;
                    }
                }
            }
        }
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
        final CassandraKeyValueServiceConfig config = configManager.getConfig();
        int rowCount = 0;
        int fetchBatchCount = config.fetchBatchCount();
        String getRowsQuery = "SELECT * FROM " + getFullTableName(tableName) + " WHERE " + ROW_NAME
                + " = ?";
        Map<Cell, Value> result = Maps.newHashMap();
        for (final List<byte[]> batch : Iterables.partition(rows, fetchBatchCount)) {
            rowCount += batch.size();
            List<ResultSetFuture> resultSetFutures = Lists.newArrayListWithCapacity(rowCount);
            PreparedStatement preparedStatement = getPreparedStatement(getRowsQuery);
            for (byte[] row : batch) {
                BoundStatement boundStatement = preparedStatement.bind();
                if (shouldTraceQuery(tableName)) {
                    boundStatement.enableTracing();
                }
                boundStatement.setBytes(ROW_NAME, ByteBuffer.wrap(row));
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
                    Cell c = Cell.create(getRowName(row), getColName(row));
                    if ((getTs(row) < startTs)
                            && (!result.containsKey(c) || (result.get(c).getTimestamp() < getTs(row)))) {
                        result.put(
                                Cell.create(getRowName(row), getColName(row)),
                                Value.create(getValue(row), getTs(row)));
                    }
                }
                logTracedQuery(getRowsQuery, resultSet);
            }
        }
        if (rowCount > fetchBatchCount) {
            log.warn("Rebatched in getRows a call to " + tableName + " that attempted to multiget "
                    + rowCount
                    + " rows; this may indicate overly-large batching on a higher level.\n"
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
        Iterable<List<Cell>> partitions = Iterables.partition(cells, fetchBatchCount);
        int numPartitions = (cells.size() / fetchBatchCount)
                + (cells.size() % fetchBatchCount > 0 ? 1 : 0);
        List<Future<?>> futures = Lists.newArrayListWithCapacity(numPartitions);
        final String loadWithTsQuery = "SELECT * FROM " + getFullTableName(tableName) + " "
                + "WHERE " + ROW_NAME + " = ? AND " + COL_NAME_COL + " = ? AND " + TS_COL
                + " > ? LIMIT 1";
        if (cells.size() > fetchBatchCount) {
            log.warn("Re-batching in loadWithTs a call to " + tableName
                    + " that attempted to multiget " + cells.size()
                    + " cells; this may indicate overly-large batching on a higher level.\n"
                    + CassandraKeyValueServices.getFilteredStackTrace("com.palantir"));
        }
        for (final List<Cell> partition : partitions) {
            futures.add(executor.submit(new Callable<Void>() {
                @Override
                public Void call() {
                    PreparedStatement preparedStatement = getPreparedStatement(loadWithTsQuery);
                    preparedStatement.setConsistencyLevel(consistency);
                    List<ResultSetFuture> resultSetFutures = Lists.newArrayListWithExpectedSize(partition.size());
                    for (Cell c : partition) {
                        BoundStatement boundStatement = preparedStatement.bind();
                        if (shouldTraceQuery(tableName)) {
                            boundStatement.enableTracing();
                        }
                        boundStatement.setBytes(ROW_NAME, ByteBuffer.wrap(c.getRowName()));
                        boundStatement.setBytes(
                                COL_NAME_COL,
                                ByteBuffer.wrap(c.getColumnName()));
                        boundStatement.setLong(TS_COL, ~startTs);
                        resultSetFutures.add(session.executeAsync(boundStatement));
                    }
                    Map<Cell, Value> res = Maps.newHashMapWithExpectedSize(partition.size());
                    for (ResultSetFuture resultSetFuture : resultSetFutures) {
                        ResultSet resultSet = resultSetFuture.getUninterruptibly();
                        for (Row row : resultSet.all()) {
                            res.put(
                                    Cell.create(getRowName(row), getColName(row)),
                                    Value.create(getValue(row), getTs(row)));
                        }
                        logTracedQuery(loadWithTsQuery, resultSet);
                    }
                    v.visit(res);
                    return null;
                }
            }));
        }
        for (Future<?> f : futures) {
            try {
                f.get();
            } catch (InterruptedException e) {
                throw Throwables.throwUncheckedException(e);
            } catch (ExecutionException e) {
                Throwables.throwIfInstance(e, Error.class);
                Throwables.rewrapAndThrowIfInstance(e.getCause(), Exception.class);
                Throwables.rewrapAndThrowUncheckedException(e.getCause());
            }
        }
    }

    static interface ThreadSafeCQLResultVisitor extends Visitor<Map<Cell, Value>> {
        // marker
    }

    static class StartTsResultsCollector implements ThreadSafeCQLResultVisitor {
        final Map<Cell, Value> collectedResults = Maps.newConcurrentMap();
        final ValueExtractor extractor = new ValueExtractor(collectedResults);
        final long startTs;

        public StartTsResultsCollector(long startTs) {
            this.startTs = startTs;
        }

        @Override
        public void visit(Map<Cell, Value> results) {
            collectedResults.putAll(results);
        }
    }

    static class AllTimestampsCollector implements ThreadSafeCQLResultVisitor {
        final Multimap<Cell, Long> collectedResults = HashMultimap.create();

        @Override
        public synchronized void visit(Map<Cell, Value> results) {
            for (Entry<Cell, Value> e : results.entrySet()) {
                collectedResults.put(e.getKey(), e.getValue().getTimestamp());
            }
        }
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
        final String loadOnlyTsQuery = "SELECT " + ROW_NAME + ", " + COL_NAME_COL + ", " + TS_COL
                + " FROM " + getFullTableName(tableName) + " " + "WHERE " + ROW_NAME + " = ? AND "
                + COL_NAME_COL + " = ? LIMIT 1";
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
                    PreparedStatement preparedStatement = getPreparedStatement(loadOnlyTsQuery);
                    preparedStatement.setConsistencyLevel(readConsistency);
                    List<ResultSetFuture> resultSetFutures = Lists.newArrayListWithExpectedSize(partition.size());
                    for (Cell c : partition) {
                        BoundStatement boundStatement = preparedStatement.bind();
                        if (shouldTraceQuery(tableName)) {
                            boundStatement.enableTracing();
                        }
                        boundStatement.setBytes(ROW_NAME, ByteBuffer.wrap(c.getRowName()));
                        boundStatement.setBytes(
                                COL_NAME_COL,
                                ByteBuffer.wrap(c.getColumnName()));
                        resultSetFutures.add(session.executeAsync(boundStatement));
                    }
                    Map<Cell, Long> res = Maps.newHashMapWithExpectedSize(partition.size());
                    for (ResultSetFuture resultSetFuture : resultSetFutures) {
                        ResultSet resultSet = resultSetFuture.getUninterruptibly();
                        for (Row row : resultSet.all()) {
                            res.put(Cell.create(getRowName(row), getColName(row)), getTs(row));
                        }
                        logTracedQuery(loadOnlyTsQuery, resultSet);
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
                    getMultiPutBatchCount(), getMultiPutBatchSizeBytes(), table, ENTRY_SIZING_FUNCTION);

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
            logTracedQuery(getPutQuery(resultSetFutures.get(result.getValue())), resultSet);
        }
    }

    private static final long TS_SIZE = 4L;

    private static final Function<Entry<Cell, Value>, Long> SIZING_FUNCTION = new Function<Entry<Cell, Value>, Long>() {
        @Override
        public Long apply(Entry<Cell, Value> input) {
            return input.getValue().getContents().length + TS_SIZE
                    + Cells.getApproxSizeOfCell(input.getKey());
        }
    };

    private void putInternal(final String tableName, final Iterable<Map.Entry<Cell, Value>> values, TransactionType transactionType)
            throws Exception {
        final CassandraKeyValueServiceConfig config = configManager.getConfig();
        List<ResultSetFuture> resultSetFutures = Lists.newArrayList();
        int mutationBatchCount = config.mutationBatchCount();
        int mutationBatchSizeBytes = config.mutationBatchSizeBytes();
        for (List<Entry<Cell, Value>> partition : partitionByCountAndBytes(
                values,
                mutationBatchCount,
                mutationBatchSizeBytes,
                tableName,
                SIZING_FUNCTION)) {
            resultSetFutures.add(getPutPartitionResultSetFuture(tableName, partition, transactionType));
        }

        for (ResultSetFuture resultSetFuture : resultSetFutures) {
            ResultSet resultSet;
            try {
                resultSet = resultSetFuture.getUninterruptibly();
                resultSet.all();
                if (!resultSet.wasApplied()) {
                    throw new KeyAlreadyExistsException("We already have a value for this timestamp");
                }
            } catch (KeyAlreadyExistsException t) {
                throw t;
            } catch (Throwable t) {
                throw Throwables.throwUncheckedException(t);
            }
            logTracedQuery(getPutQuery(tableName), resultSet);
        }
    }

    private String getPutQueryForPossibleTransaction(String tableName, TransactionType transactionType) {
        return transactionType.equals(TransactionType.LIGHTWEIGHT_TRANSACTION_REQUIRED)? getPutUnlessExistsQuery(tableName) : getPutQuery(tableName);
    }

    private String getPutUnlessExistsQuery(String tableName) {
        return getPutQuery(tableName) + " IF NOT EXISTS";
    }

    private String getPutQuery(String tableName) {
        return "INSERT INTO " + getFullTableName(tableName) + " (" + ROW_NAME + ", " + COL_NAME_COL + ", " + TS_COL + ", " + VALUE_COL + ") VALUES (?, ?, ?, ?)";
    }

    private ResultSetFuture getPutPartitionResultSetFuture(String tableName,
                                                           List<Entry<Cell, Value>> partition,
                                                          TransactionType transactionType) {
        PreparedStatement preparedStatement = getPreparedStatement(getPutQueryForPossibleTransaction(tableName, transactionType));
        preparedStatement.setConsistencyLevel(writeConsistency);

        // Be mindful when using the atomicity semantics of UNLOGGED batch statements.
        // This usage should be okay, as the KVS.multiPut explicitly does not guarantee
        // atomicity across cells (nor batch isolation, which we also cannot provide)
        BatchStatement batchStatement = new BatchStatement(BatchStatement.Type.UNLOGGED);
        for (Entry<Cell, Value> e : partition) {
            BoundStatement boundStatement = preparedStatement.bind();
            if (shouldTraceQuery(tableName)) {
                boundStatement.enableTracing();
            }
            boundStatement.setBytes(ROW_NAME, ByteBuffer.wrap(e.getKey().getRowName()));
            boundStatement.setBytes(COL_NAME_COL, ByteBuffer.wrap(e.getKey().getColumnName()));
            boundStatement.setLong(TS_COL, ~e.getValue().getTimestamp());
            boundStatement.setBytes(VALUE_COL, ByteBuffer.wrap(e.getValue().getContents()));
            if (partition.size() > 1) {
                batchStatement.add(boundStatement);
                if (shouldTraceQuery(tableName)) {
                    batchStatement.enableTracing();
                }
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
            PreparedStatement createStatement = getPreparedStatement(String.format(truncateQuery, getFullTableName(tableName)));
            createStatement.setConsistencyLevel(ConsistencyLevel.ALL);
            BoundStatement boundStatement = createStatement.bind();

            if (shouldTraceQuery(tableName)) {
                boundStatement.enableTracing();
            }
            try {
                ResultSet resultSet = session.execute(boundStatement);
                logTracedQuery(truncateQuery, resultSet);
            } catch (com.datastax.driver.core.exceptions.UnavailableException e) {
                throw new InsufficientConsistencyException("Truncating tables requires all Cassandra nodes to be up and available.", e);
            }
        }

        waitForSchemaVersionsToCoalesce("truncateTables(" + tablesToTruncate.size() + " tables)");
    }

    @Override
    public void delete(final String tableName, final Multimap<Cell, Long> keys) {
        final CassandraKeyValueServiceConfig config = configManager.getConfig();
        int cellCount = 0;
        int fetchBatchCount = config.fetchBatchCount();
        final String deleteQuery = "DELETE FROM " + getFullTableName(tableName) + " WHERE "
                + ROW_NAME + " = ? AND " + COL_NAME_COL + " = ? AND " + TS_COL + " = ?";
        for (final List<Cell> batch : Iterables.partition(keys.keySet(), fetchBatchCount)) {
            cellCount += batch.size();
            PreparedStatement preparedStatement = getPreparedStatement(deleteQuery);
            preparedStatement.setConsistencyLevel(deleteConsistency);
            List<ResultSetFuture> resultSetFutures = Lists.newArrayList();
            for (Cell key : batch) {
                for (long ts : Ordering.natural().immutableSortedCopy(keys.get(key))) {
                    BoundStatement boundStatement = preparedStatement.bind();
                    if (shouldTraceQuery(tableName)) {
                        boundStatement.enableTracing();
                    }
                    boundStatement.setBytes(ROW_NAME, ByteBuffer.wrap(key.getRowName()));
                    boundStatement.setBytes(COL_NAME_COL, ByteBuffer.wrap(key.getColumnName()));
                    boundStatement.setLong(TS_COL, ~ts);
                    resultSetFutures.add(session.executeAsync(boundStatement));
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
                logTracedQuery(deleteQuery, resultSet);
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
        final CassandraKeyValueServiceConfig config = configManager.getConfig();
        int concurrency = config.rangesConcurrency();
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
        final int batchHint = rangeRequest.getBatchHint() == null ? 100 : rangeRequest.getBatchHint();
        final ColumnSelection selection = rangeRequest.getColumnNames().isEmpty() ? ColumnSelection.all() : ColumnSelection.create(rangeRequest.getColumnNames());
        final byte[] endExclusive = rangeRequest.getEndExclusive();
        final StringBuilder bindQuery = new StringBuilder();
        bindQuery.append("SELECT * FROM " + getFullTableName(tableName) + " WHERE token("
                + ROW_NAME + ") >= token(?) ");
        if (endExclusive.length > 0) {
            bindQuery.append("AND token(" + ROW_NAME + ") < token(?) ");
        }
        bindQuery.append("LIMIT " + batchHint);
        final String getLastRowQuery = "SELECT * FROM " + getFullTableName(tableName) + " WHERE "
                + ROW_NAME + " = ?";
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
                PreparedStatement preparedStatement = getPreparedStatement(bindQuery.toString());
                preparedStatement.setConsistencyLevel(consistency);
                BoundStatement boundStatement = preparedStatement.bind();
                if (shouldTraceQuery(tableName)) {
                    boundStatement.enableTracing();
                }
                boundStatement.setBytes(0, ByteBuffer.wrap(startKey));
                if (endExclusive.length > 0) {
                    boundStatement.setBytes(1, ByteBuffer.wrap(endExclusive));
                }
                ResultSet resultSet = session.execute(boundStatement);
                List<Row> rows = Lists.newArrayList(resultSet.all());
                logTracedQuery(bindQuery.toString(), resultSet);
                byte[] maxRow = null;
                ResultsExtractor<T, U> extractor = resultsExtractor.get();
                for (Row row : rows) {
                    byte[] rowName = getRowName(row);
                    if (maxRow == null) {
                        maxRow = rowName;
                    } else {
                        maxRow = PtBytes.BYTES_COMPARATOR.max(maxRow, rowName);
                    }
                }
                if (maxRow == null) {
                    return new SimpleTokenBackedResultsPage<RowResult<U>, byte[]>(
                            endExclusive,
                            ImmutableList.<RowResult<U>>of(),
                            false);
                }
                // get the rest of the last row
                PreparedStatement prepareLastRow = getPreparedStatement(getLastRowQuery);
                BoundStatement boundLastRow = prepareLastRow.bind();
                if (shouldTraceQuery(tableName)) {
                    boundLastRow.enableTracing();
                }
                boundLastRow.setBytes(ROW_NAME, ByteBuffer.wrap(maxRow));
                try {
                    resultSet = session.execute(boundLastRow);
                } catch (com.datastax.driver.core.exceptions.UnavailableException e) {
                    throw new InsufficientConsistencyException("This operation requires all Cassandra nodes to be up and available.", e);
                }
                rows.addAll(resultSet.all());
                logTracedQuery(getLastRowQuery, resultSet);
                for (Row row : rows) {
                    extractor.internalExtractResult(
                            timestamp,
                            selection,
                            getRowName(row),
                            getColName(row),
                            getValue(row),
                            getTs(row));
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
                PreparedStatement createStatement = getPreparedStatement(String.format(dropQuery, getFullTableName(tableName)));
                createStatement.setConsistencyLevel(ConsistencyLevel.ALL);
                BoundStatement boundStatement = createStatement.bind();

                if (shouldTraceQuery(tableName)) {
                    boundStatement.enableTracing();
                }

                try {
                    ResultSet resultSet = session.execute(boundStatement);
                    logTracedQuery(dropQuery, resultSet);
                } catch (com.datastax.driver.core.exceptions.UnavailableException e) {
                    throw new InsufficientConsistencyException("Dropping tables requires all Cassandra nodes to be up and available.", e);
                }
            }

        waitForSchemaVersionsToCoalesce("dropTables(" + tablesToDrop.size() + " tables)");

        put(CassandraConstants.METADATA_TABLE, Maps.toMap(
                Lists.transform(Lists.newArrayList(tablesToDrop), new Function<String, Cell>() {
                    @Override
                    public Cell apply(String tableName) {
                        return getMetadataCell(tableName);
                    }}),
                Functions.constant(PtBytes.EMPTY_BYTE_ARRAY)),
                System.currentTimeMillis());
    }


    @Override
    public void createTable(final String tableName, final int maxValueSizeInBytes) {
        createTables(ImmutableMap.of(tableName, maxValueSizeInBytes));
    }

    @Override
    public void createTables(final Map<String, Integer> tableNamesToMaxValueSizeInBytes) {
        String createQuery = "CREATE TABLE IF NOT EXISTS %s ( " // full table name (ks.cf)
                + ROW_NAME + " blob, "
                + COL_NAME_COL + " blob, "
                + TS_COL + " bigint, "
                + VALUE_COL + " blob, "
                + "PRIMARY KEY ("
                + ROW_NAME + ", "
                + COL_NAME_COL + ", "
                + TS_COL + ")) "
                + "WITH COMPACT STORAGE AND CLUSTERING ORDER BY ("
                + COL_NAME_COL + " ASC, "
                + TS_COL + " ASC) "
                + "AND compaction = {'sstable_size_in_mb': '80', 'class': 'org.apache.cassandra.db.compaction.LeveledCompactionStrategy'}";

        for (String tableName : tableNamesToMaxValueSizeInBytes.keySet()) {
            PreparedStatement createStatement = getPreparedStatement(String.format(createQuery, getFullTableName(tableName)));
            createStatement.setConsistencyLevel(ConsistencyLevel.ALL);
            BoundStatement boundStatement = createStatement.bind();

            if (shouldTraceQuery(tableName)) {
                boundStatement.enableTracing();
            }
            try {
                ResultSet resultSet = session.execute(boundStatement);
                logTracedQuery(createQuery, resultSet);
            } catch (com.datastax.driver.core.exceptions.UnavailableException e) {
                throw new InsufficientConsistencyException("Creating tables requires all Cassandra nodes to be up and available.", e);
            }
        }
        waitForSchemaVersionsToCoalesce("createTables(" + tableNamesToMaxValueSizeInBytes.size() + " tables)");
    }

    @Override
    public Set<String> getAllTableNames() {
        BoundStatement boundStatement = statementCache.getUnchecked(
                "SELECT columnfamily_name FROM system.schema_columnfamilies WHERE keyspace_name = ?").bind();
        final CassandraKeyValueServiceConfig config = configManager.getConfig();
        boundStatement.setString("keyspace_name", config.keyspace());
        List<Row> rows = session.executeAsync(boundStatement).getUninterruptibly().all();
        Set<String> tableNames = Sets.newHashSetWithExpectedSize(rows.size());
        for (Row row : rows) {
            String tableName = row.getString(0);
            if (!HIDDEN_TABLES.contains(tableName)) {
                tableNames.add(tableName);
            }
        }
        return tableNames;
    }

    @Override
    public byte[] getMetadataForTable(String tableName) {
        Cell cell = getMetadataCell(tableName);
        Value v = get(CassandraConstants.METADATA_TABLE, ImmutableMap.of(cell, Long.MAX_VALUE)).get(
                cell);
        if (v == null) {
            return PtBytes.EMPTY_BYTE_ARRAY;
        } else {
            return v.getContents();
        }
    }

    private Cell getMetadataCell(String tableName) {
        return Cell.create(tableName.getBytes(), "m".getBytes());
    }

    @Override
    public void putMetadataForTable(final String tableName, final byte[] meta) {
        putMetadataForTables(ImmutableMap.of(tableName, meta));
    }

    @Override
    public void putMetadataForTables(final Map<String, byte[]> tableNameToMetadata) {
        Map<Cell, byte[]> cellToMetadata = Maps.newHashMap();
        for (Entry<String, byte[]> tableEntry : tableNameToMetadata.entrySet()) {
            cellToMetadata.put(getMetadataCell(tableEntry.getKey()), tableEntry.getValue());
            setSettingsForTable(tableEntry.getKey(), tableEntry.getValue());
        }
        put(CassandraConstants.METADATA_TABLE, cellToMetadata, System.currentTimeMillis());
        waitForSchemaVersionsToCoalesce("putMetadataForTables(" + tableNameToMetadata.size() +" tables)");
    }

    private void setSettingsForTable(String tableName, byte[] rawMetadata) {
        int explicitCompressionBlockSizeKB = 0;
        boolean negativeLookups = false;
        double falsePositiveChance = CassandraConstants.DEFAULT_LEVELED_COMPACTION_BLOOM_FILTER_FP_CHANCE;

        if (rawMetadata != null && rawMetadata.length != 0) {
            TableMetadata tableMetadata = TableMetadata.BYTES_HYDRATOR.hydrateFromBytes(rawMetadata);
            explicitCompressionBlockSizeKB = tableMetadata.getExplicitCompressionBlockSizeKB();
            negativeLookups = tableMetadata.hasNegativeLookups();
        }
        if (negativeLookups) {
            falsePositiveChance = CassandraConstants.NEGATIVE_LOOKUPS_BLOOM_FILTER_FP_CHANCE;
        }

        StringBuilder sb = new StringBuilder();
        sb.append("ALTER TABLE " + getFullTableName(tableName) + " WITH "
                + "bloom_filter_fp_chance = " + falsePositiveChance + " ");
        int chunkLength = AtlasDbConstants.MINIMUM_COMPRESSION_BLOCK_SIZE_KB;
        if (explicitCompressionBlockSizeKB != 0) {
            chunkLength = explicitCompressionBlockSizeKB;
        }

        sb.append("AND caching = '{\"keys\":\"ALL\", \"rows_per_partition\":\"ALL\"}' ");
        sb.append("AND compaction = {'sstable_size_in_mb': '80', 'class': 'org.apache.cassandra.db.compaction.LeveledCompactionStrategy'} ");
        sb.append("AND compression = {'chunk_length_kb': '" + chunkLength + "', "
                  + "'sstable_compression': '" + CassandraConstants.DEFAULT_COMPRESSION_TYPE + "'}");
        PreparedStatement preparedStatement = getPreparedStatement(sb.toString());
        preparedStatement.setConsistencyLevel(ConsistencyLevel.ALL);
        try {
            session.executeAsync(preparedStatement.bind()).getUninterruptibly().all();
        } catch (Throwable t) {
            throw Throwables.throwUncheckedException(t);
        }
    }

    @Override
    public void addGarbageCollectionSentinelValues(String tableName, Set<Cell> cells) {
        try {
            final Value value = Value.create(PtBytes.EMPTY_BYTE_ARRAY, Value.INVALID_VALUE_TIMESTAMP);
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
                    KeyValueServices.toConstantTimestampValues(values.entrySet(), TRANSACTION_TS),
                    TransactionType.LIGHTWEIGHT_TRANSACTION_REQUIRED);
        } catch (Throwable t) {
            throw Throwables.throwUncheckedException(t);
        }
    }

    private String getFullTableName(String tableName) {
        return configManager.getConfig().keyspace() + ".\"" + tableName + "\"";
    }

    private byte[] getRowName(Row row) {
        return CassandraKeyValueServices.getBytesFromByteBuffer(row.getBytes(ROW_NAME));
    }

    private byte[] getColName(Row row) {
        return CassandraKeyValueServices.getBytesFromByteBuffer(row.getBytes(COL_NAME_COL));
    }

    private long getTs(Row row) {
        return ~row.getLong(TS_COL);
    }

    private byte[] getValue(Row row) {
        return CassandraKeyValueServices.getBytesFromByteBuffer(row.getBytes(VALUE_COL));
    }

    private PreparedStatement getPreparedStatement(String query) {
        try {
            return statementCache.get(query);
        } catch (ExecutionException e) {
            Throwables.throwIfInstance(e, Error.class);
            throw Throwables.throwUncheckedException(e.getCause());
        }
    }

    private static final ExecutorService traceRetrievalExec = PTExecutors.newFixedThreadPool(8);
    private static final int MAX_TRIES = 20;
    private static final long TRACE_RETRIEVAL_MS_BETWEEN_TRIES = 500;

    private void logTracedQuery(final String tracedQuery, ResultSet resultSet) {
        if (log.isInfoEnabled()) {

            List<ExecutionInfo> allExecutionInfo = Lists.newArrayList(resultSet.getAllExecutionInfo());
            for (final ExecutionInfo info : allExecutionInfo) {
                if (info.getQueryTrace() == null) {
                    continue;
                }
                final UUID traceId = info.getQueryTrace().getTraceId();
                log.info("Traced query " + tracedQuery + " with trace uuid " + traceId);
                traceRetrievalExec.submit(new Callable<Void>() {
                    @Override
                    public Void call() throws Exception {
                        StringBuilder sb = new StringBuilder();
                        sb.append("Retrieving traced query " + tracedQuery + " trace uuid: "
                                + traceId);
                        int tries = 0;
                        boolean success = false;
                        while (tries < MAX_TRIES) {

                            BoundStatement sessionStatement = statementCache.getUnchecked(
                                    "SELECT * FROM system_traces.sessions WHERE session_id = ?").bind();
                            sessionStatement.setUUID("session_id", traceId);
                            ResultSetFuture sessionFuture = session.executeAsync(sessionStatement);

                            BoundStatement eventStatement = statementCache.getUnchecked(
                                    "SELECT * FROM system_traces.events WHERE session_id = ?").bind();
                            eventStatement.setUUID("session_id", traceId);
                            ResultSetFuture eventFuture = session.executeAsync(eventStatement);

                            Row sessionRow = sessionFuture.getUninterruptibly().one();
                            List<Row> eventRows = eventFuture.getUninterruptibly().all();

                            if (sessionRow != null && !sessionRow.isNull("duration")) {

                                sb.append(" requestType: ").append(sessionRow.getString("request"));
                                sb.append(" coordinator: ").append(sessionRow.getInet("coordinator"));
                                sb.append(" started_at: ").append(sessionRow.getDate("started_at").getTime());
                                sb.append(" duration: ").append(sessionRow.getInt("duration"));
                                if (!sessionRow.isNull("parameters")) {
                                    sb.append("\nparameters: "
                                            + Collections.unmodifiableMap(sessionRow.getMap(
                                            "parameters",
                                            String.class,
                                            String.class)));
                                }

                                for (Row eventRow : eventRows) {
                                    sb.append(eventRow.getString("activity"))
                                    .append(" on ")
                                    .append(eventRow.getInet("source")).append("[")
                                    .append(eventRow.getString("thread")).append("] at ")
                                    .append(eventRow.getUUID("event_id").timestamp()).append(" (")
                                    .append(eventRow.getInt("source_elapsed")).append(" elapsed)\n");
                                }
                                success = true;
                                break;
                            }
                            tries++;
                            Thread.sleep(TRACE_RETRIEVAL_MS_BETWEEN_TRIES);
                        }
                        if (!success) {
                            sb.append(" (retrieval timed out)");
                        }
                        log.info(sb.toString());
                        return null;
                    }
                });
            }
        }
    }

    @Override
    public void compactInternally(String tableName) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(tableName), "tableName:[%s] should not be null or empty", tableName);
        final CassandraKeyValueServiceConfig config = configManager.getConfig();
        long compactionTimeoutSeconds = config.compactionTimeoutSeconds();
        try {
            alterGcGraceSeconds(tableName, 0);
            alterTombstoneThreshold(tableName, 0.0f);
            compactionManager.forceTableCompaction(compactionTimeoutSeconds, config.keyspace(), tableName);
        } catch (TimeoutException e) {
            log.error("Compaction could not finish in {} seconds! {}", compactionTimeoutSeconds, e.getMessage());
            log.error(compactionManager.getPendingCompactionStatus());
        } finally {
            alterGcGraceSeconds(tableName, CassandraConstants.GC_GRACE_SECONDS);
            alterTombstoneThreshold(tableName, CassandraConstants.TOMBSTONE_THRESHOLD_RATIO);
        }
    }

    private void alterGcGraceSeconds(final String tableName, int gcGraceSeconds) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(tableName), "tableName:[%s] should not be null or empty", tableName);
        Preconditions.checkArgument(gcGraceSeconds >= 0, "gcGraceSeconds:[%s] should not be negative", gcGraceSeconds);
        log.trace("Altering gc_grace_seconds into {} seconds for table:{}", gcGraceSeconds, tableName);
        String setGcGraceSecondQuery = "ALTER TABLE " + getFullTableName(tableName)
                + " WITH gc_grace_seconds = '" + gcGraceSeconds + "';";
        PreparedStatement preparedStatement = getPreparedStatement(setGcGraceSecondQuery);
        preparedStatement.setConsistencyLevel(ConsistencyLevel.ALL);
        BoundStatement boundStatement = preparedStatement.bind();
        if (shouldTraceQuery(tableName)) {
            boundStatement.enableTracing();
        }
        ResultSet resultSet;
        try {
            resultSet = session.executeAsync(preparedStatement.bind()).getUninterruptibly();
            resultSet.all();
        } catch (UnavailableException e) {
            throw new InsufficientConsistencyException("Alter table requires all Cassandra nodes to be up and available.", e);
        } catch (Exception e) {
            throw Throwables.throwUncheckedException(e);
        }
        logTracedQuery(setGcGraceSecondQuery, resultSet);
        return;
    }


    private void alterTombstoneThreshold(final String tableName, float tombstoneThreshold) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(tableName), "tableName:[%s] should not be null or empty", tableName);
        Preconditions.checkArgument(tombstoneThreshold >= 0.0, "gcGraceSeconds:[%s] should not be negative", tombstoneThreshold);
        log.trace("Altering tombstone_threshold into {} for table:{}", tombstoneThreshold, tableName);
        String setTombStoneQuery = "ALTER TABLE " + getFullTableName(tableName)
                + " WITH compaction = {'class':'LeveledCompactionStrategy', 'tombstone_threshold':'" + tombstoneThreshold + "';";
        PreparedStatement preparedStatement = getPreparedStatement(setTombStoneQuery);
        preparedStatement.setConsistencyLevel(ConsistencyLevel.ALL);
        BoundStatement boundStatement = preparedStatement.bind();
        if (shouldTraceQuery(tableName)) {
            boundStatement.enableTracing();
        }
        ResultSet resultSet;
        try {
            resultSet = session.executeAsync(preparedStatement.bind()).getUninterruptibly();
            resultSet.all();
        } catch (UnavailableException e) {
            throw new InsufficientConsistencyException("Alter table requires all Cassandra nodes to be up and available.", e);
        } catch (Exception e) {
            throw Throwables.throwUncheckedException(e);
        }
        logTracedQuery(setTombStoneQuery, resultSet);
        return;
    }

    private void waitForSchemaVersionsToCoalesce(String encapsulatingOperationDescription) {
        PreparedStatement peerInfoQuery = getPreparedStatement("select peer, schema_version from system.peers;");
        peerInfoQuery.setConsistencyLevel(ConsistencyLevel.ALL);

        Multimap<UUID, InetAddress> peerInfo = ArrayListMultimap.create();
        long start = System.currentTimeMillis();
        long sleepTime = 100;
        do {
            peerInfo.clear();
            for (Row row : session.execute(peerInfoQuery.bind()).all()) {
                peerInfo.put(row.getUUID("schema_version"), row.getInet("peer"));
            }

            if (peerInfo.keySet().size() <= 1) { // full schema agreement
                return;
            }
            sleepTime = Math.min(sleepTime * 2, 5000);
        } while (System.currentTimeMillis() < start + CassandraConstants.SECONDS_WAIT_FOR_VERSIONS*1000);

        StringBuilder sb = new StringBuilder();
        sb.append(String.format("Cassandra cluster cannot come to agreement on schema versions, during operation: %s.", encapsulatingOperationDescription));

        for ( Entry<UUID, Collection<InetAddress>> versionToPeer : peerInfo.asMap().entrySet()) {
            sb.append(String.format("\nAt schema version %s:", versionToPeer.getKey()));
            for (InetAddress peer: versionToPeer.getValue()) {
                sb.append(String.format("\n\tNode: %s", peer));
            }
        }
        sb.append("\nFind the nodes above that diverge from the majority schema " +
                "(or have schema 'UNKNOWN', which likely means they are down/unresponsive) " +
                "and examine their logs to determine the issue. Fixing the underlying issue and restarting Cassandra " +
                "should resolve the problem. You can quick-check this with 'nodetool describecluster'.");
        throw new IllegalStateException(sb.toString());
    }
}
