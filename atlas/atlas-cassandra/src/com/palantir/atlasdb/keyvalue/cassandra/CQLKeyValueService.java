// Copyright 2015 Palantir Technologies
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.palantir.atlasdb.keyvalue.cassandra;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
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

import javax.annotation.Nullable;

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
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.QueryOptions;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.base.Supplier;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
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
import com.palantir.atlasdb.property.AtlasSystemPropertyManager;
import com.palantir.atlasdb.table.description.TableMetadata;
import com.palantir.atlasdb.transaction.impl.TransactionConstants;
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

    private Cluster cluster;
    private Session session;
    private final LoadingCache<String, PreparedStatement> statementCache = CacheBuilder.newBuilder().build(new CacheLoader<String, PreparedStatement>() {
        @Override
        public PreparedStatement load(String query) {
            return session.prepare(query);
        }});

    private final int port;
    private final boolean isSsl, safetyDisabled;
    private final int mutationBatchCount, mutationBatchSizeBytes, fetchBatchCount;
    private final int poolSize;
    private final int poolTimeout;
    private final String keyspace;
    private final CassandraClientPoolingManager cassandraClientPoolingManager;
    private final ManyClientPoolingContainer containerPoolToUpdate;
    private final PoolingContainer<Client> clientPool;
    private final ScheduledExecutorService hostRefreshExecutor = PTExecutors.newScheduledThreadPool(1);
    private final AtlasSystemPropertyManager systemProperties;

    private ConsistencyLevel readConsistency = ConsistencyLevel.LOCAL_QUORUM;
    private final ConsistencyLevel writeConsistency = ConsistencyLevel.EACH_QUORUM;
    private final ConsistencyLevel deleteConsistency = ConsistencyLevel.ALL;

    private static final String ROW_NAME = "key";
    private static final String COL_NAME_COL = "column1";
    private static final String TS_COL = "column2";
    private static final String VALUE_COL = "value";

    private static final long TRANSACTION_TS = 0L;

    public static CQLKeyValueService create(Set<String> hosts,
                                            final int port,
                                            final int poolSize,
                                            int poolTimeout,
                                            String keyspace,
                                            boolean isSsl,
                                            int replicationFactor,
                                            final int mutationBatchCount,
                                            final int mutationBatchSizeBytes,
                                            final int fetchBatchCount,
                                            boolean safetyDisabled,
                                            boolean autoRefreshNodes,
                                            AtlasSystemPropertyManager systemProperties) {
        Preconditions.checkArgument(!hosts.isEmpty(), "hosts set was empty");
        final CQLKeyValueService ret = new CQLKeyValueService(
                hosts,
                port,
                poolSize,
                poolTimeout,
                keyspace,
                isSsl,
                mutationBatchCount,
                mutationBatchSizeBytes,
                fetchBatchCount,
                safetyDisabled,
                autoRefreshNodes,
                systemProperties);
        try {
            ret.initializeFromFreshInstance(ImmutableList.copyOf(hosts), replicationFactor);
            ret.getPoolingManager().submitHostRefreshTask();
        } catch (Throwable t) {
            throw Throwables.throwUncheckedException(t);
        }
        return ret;
    }

    private CQLKeyValueService(Set<String> hosts,
                               int port,
                               int poolSize,
                               int poolTimeout,
                               String keyspace,
                               boolean isSsl,
                               int mutationBatchCount,
                               int mutationBatchSizeBytes,
                               int fetchBatchCount,
                               boolean safetyDisabled,
                               boolean autoRefreshNodes,
                               AtlasSystemPropertyManager systemProperties) {
        super(PTExecutors.newFixedThreadPool(poolSize * 2, new NamedThreadFactory(
                "CQLKeyValueService",
                false)));
        this.port = port;
        this.isSsl = isSsl;
        this.keyspace = keyspace;
        this.poolSize = poolSize;
        this.poolTimeout = poolTimeout;
        this.mutationBatchCount = mutationBatchCount;
        this.mutationBatchSizeBytes = mutationBatchSizeBytes;
        this.fetchBatchCount = fetchBatchCount;
        this.safetyDisabled = safetyDisabled;
        this.systemProperties = systemProperties;
        this.containerPoolToUpdate = ManyClientPoolingContainer.create(
                hosts,
                port,
                hosts.size(),
                keyspace,
                isSsl,
                safetyDisabled);
        this.clientPool = new RetriablePoolingContainer(this.containerPoolToUpdate);
        cassandraClientPoolingManager = new CassandraClientPoolingManager(
                containerPoolToUpdate,
                clientPool,
                port,
                isSsl,
                hosts.size(),
                keyspace,
                safetyDisabled,
                autoRefreshNodes);
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

        Cluster.Builder clusterBuilder = Cluster.builder();
        clusterBuilder.addContactPoints(addresses);
        clusterBuilder.withClusterName("atlas_cassandra_cluster_" + keyspace); // for JMX metrics
        clusterBuilder.withCompression(Compression.LZ4);
        clusterBuilder.withProtocolVersion(ProtocolVersion.V3);

        // todo (clockfort) remove dynamic classloading after we ship a non-patch release
        ClassLoader cl = CQLKeyValueService.class.getClassLoader();
        try {
            Class<?> metrics = cl.loadClass("com.codahale.metrics.Metric");
            log.info("Runtime loaded " + metrics.getName());
        } catch (ClassNotFoundException e) {
            log.error("Unable to load metrics class com.codahale.metrics for Cassandra cluster, running with metrics off.");
            clusterBuilder.withoutMetrics();
        }
        try {
            Class<?> netty = cl.loadClass("com.datastax.shaded.netty.bootstrap.Bootstrap");
            log.info("Runtime loaded " + netty.getName());
        } catch (ClassNotFoundException e) {
            throw new IllegalStateException("Couldn't find necessary netty library.");
        }

        if (isSsl) {
            clusterBuilder.withSSL();
        }

        PoolingOptions poolingOptions = new PoolingOptions();
        poolingOptions.setMaxSimultaneousRequestsPerHostThreshold(HostDistance.LOCAL, poolSize);
        poolingOptions.setMaxSimultaneousRequestsPerHostThreshold(HostDistance.REMOTE, poolSize);
        poolingOptions.setPoolTimeoutMillis(poolTimeout);
        clusterBuilder.withPoolingOptions(poolingOptions);

        // defaults for queries; can override on per-query basis
        QueryOptions queryOptions = new QueryOptions();
        queryOptions.setFetchSize(mutationBatchCount);
        clusterBuilder.withQueryOptions(queryOptions);

        // todo add policy classes

        cluster = clusterBuilder.build();
        Metadata metadata = cluster.getMetadata(); // special; this is the first place we connect to
                                                   // hosts, this is where people will see failures
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

        if (!safetyDisabled) {
            Validate.isTrue(CassandraConstants.PARTITIONER.equals(partitioner), "partitioner is: "
                    + partitioner);
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
        super.close();
    }

    private void initializeFromFreshInstance(List<String> hosts, int replicationFactor) {
        Map<String, Throwable> errorsByHost = Maps.newHashMap();
        initializeConnectionPoolWithNewAPI(ImmutableSet.copyOf(hosts));
        for (String host : hosts) {
            Cassandra.Client client = null;
            try {
                client = CassandraKeyValueServices.getClientInternal(host, port, isSsl);
                String partitioner = client.describe_partitioner();
                if (!safetyDisabled) {
                    Validate.isTrue(
                            CassandraConstants.PARTITIONER.equals(partitioner),
                            "partitioner is: " + partitioner);
                }
                KsDef ks = null;
                try {
                    ks = client.describe_keyspace(keyspace);
                } catch (NotFoundException e) {
                    // need to create key space
                }

                Set<String> currentHosts = cassandraClientPoolingManager.getCurrentHostNamesFromServer(client);
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
                            isSsl,
                            safetyDisabled);
                    createTableInternal(CassandraConstants.METADATA_TABLE);
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
                        isSsl,
                        safetyDisabled);
                createTableInternal(CassandraConstants.METADATA_TABLE);
                if (client.describe_version().startsWith("1.2")) {
                    throw new UnsupportedOperationException("Cassandra version must be 2.x+ in order to safely use CQL driver.");
                }
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
        Set<String> dcs;
        try {
            dcs = CassandraVerifier.sanityCheckDatacenters(client, desiredRf, safetyDisabled);
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
            Set<Cell> cells = Sets.newHashSet();
            for (byte[] row : rows) {
                for (byte[] col : selection.getSelectedColumns()) {
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
        String getRowsQuery = "SELECT * FROM " + getFullTableName(tableName) + " WHERE " + ROW_NAME
                + " = ?";
        Map<Cell, Value> result = Maps.newHashMap();
        for (final List<byte[]> batch : Iterables.partition(rows, fetchBatchCount)) {
            rowCount += batch.size();
            List<ResultSetFuture> resultSetFutures = Lists.newArrayListWithExpectedSize(rowCount);
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
        putInternal(
                tableName,
                KeyValueServices.toConstantTimestampValues(values.entrySet(), timestamp),
                false);
    }

    @Override
    public void putWithTimestamps(String tableName, Multimap<Cell, Value> values) {
        putInternal(tableName, values.entries(), false);
    }

    @Override
    protected int getMultiPutBatchCount() {
        return CassandraConstants.PUT_BATCH_SIZE;
    }

    @Override
    public void multiPut(Map<String, ? extends Map<Cell, byte[]>> valuesByTable, final long timestamp) throws KeyAlreadyExistsException {
        Map<ResultSetFuture, String> resultSetFutures = Maps.newHashMap();
        for (Entry<String, ? extends Map<Cell, byte[]>> e : valuesByTable.entrySet()) {
            final String table = e.getKey();
            // We sort here because some key value stores are more efficient if you store adjacent keys together.
            NavigableMap<Cell, byte[]> sortedMap = ImmutableSortedMap.copyOf(e.getValue());


            Iterable<List<Entry<Cell, byte[]>>> partitions = partitionByCountAndBytes(sortedMap.entrySet(),
                    getMultiPutBatchCount(), getMultiPutBatchSizeBytes(), table, new Function<Entry<Cell, byte[]>, Long>(){

                @Override
                public Long apply(Entry<Cell, byte[]> entry) {
                    long totalSize = 0;
                    totalSize += entry.getValue().length;
                    totalSize += Cells.getApproxSizeOfCell(entry.getKey());
                    return totalSize;
                }});


            for (final List<Entry<Cell, byte[]>> p : partitions) {
                List<Entry<Cell, Value>> partition = Lists.transform(p, new Function<Entry<Cell, byte[]>, Entry<Cell, Value>>() {
                    @Override
                    public Entry<Cell, Value> apply(Entry<Cell, byte[]> input) {
                        return Maps.immutableEntry(input.getKey(), Value.create(input.getValue(), timestamp));
                    }});
                resultSetFutures.put(getPutPartitionResultSetFuture(table, partition, false), table);
            }
        }

        for (ResultSetFuture resultSetFuture : resultSetFutures.keySet()) {
            ResultSet resultSet;
            try {
                resultSet = resultSetFuture.getUninterruptibly();
                resultSet.all();
            } catch (Throwable t) {
                throw Throwables.throwUncheckedException(t);
            }
            logTracedQuery(getPutQuery(resultSetFutures.get(resultSetFuture), false), resultSet);
        }
    }

    private final long TS_SIZE = 4L;

    private void putInternal(final String tableName, final Iterable<Map.Entry<Cell, Value>> values, boolean addNotExists)
            throws KeyAlreadyExistsException {
        Function<Entry<Cell, Value>, Long> sizingFunction = new Function<Entry<Cell, Value>, Long>() {
            @Override
            public Long apply(@Nullable Entry<Cell, Value> input) {
                return input.getValue().getContents().length + TS_SIZE
                        + Cells.getApproxSizeOfCell(input.getKey());
            }
        };

        List<ResultSetFuture> resultSetFutures = Lists.newArrayList();
        for (List<Entry<Cell, Value>> partition : partitionByCountAndBytes(
                values,
                mutationBatchCount,
                mutationBatchSizeBytes,
                tableName,
                sizingFunction)) {
            resultSetFutures.add(getPutPartitionResultSetFuture(tableName, partition, addNotExists));
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
            logTracedQuery(getPutQuery(tableName, addNotExists), resultSet);
        }
    }

    private String getPutQuery(String tableName, boolean addNotExists) {
        StringBuilder sb = new StringBuilder("INSERT INTO " + getFullTableName(tableName) + " (" + ROW_NAME + ", " + COL_NAME_COL
                + ", " + TS_COL + ", " + VALUE_COL + ") VALUES (?, ?, ?, ?)");
        if (addNotExists) {
            sb.append(" IF NOT EXISTS");
        }
        return sb.toString();
    }

    private ResultSetFuture getPutPartitionResultSetFuture(String tableName,
                                                           List<Entry<Cell, Value>> partition,
                                                           boolean addNotExists) {
        PreparedStatement preparedStatement = getPreparedStatement(getPutQuery(tableName, addNotExists));
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
        String truncateQuery = "TRUNCATE " + getFullTableName(tableName);
        PreparedStatement preparedStatement = getPreparedStatement(truncateQuery);
        preparedStatement.setConsistencyLevel(deleteConsistency);
        BoundStatement boundStatement = preparedStatement.bind();
        if (shouldTraceQuery(tableName)) {
            boundStatement.enableTracing();
        }
        ResultSet resultSet;
        try {
            resultSet = session.executeAsync(boundStatement).getUninterruptibly();
            resultSet.all();
        } catch (com.datastax.driver.core.exceptions.UnavailableException e) {
            throw new InsufficientConsistencyException("Truncate requires all Cassandra nodes to be up and available.", e);
        } catch (Throwable t) {
            throw Throwables.throwUncheckedException(t);
        }
        logTracedQuery(truncateQuery, resultSet);
    }

    @Override
    public void delete(final String tableName, final Multimap<Cell, Long> keys) {
        int cellCount = 0;
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
        return KeyValueServices.getFirstBatchForRangesUsingGetRangeConcurrent(
                executor,
                this,
                tableName,
                rangeRequests,
                timestamp);
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
                            ImmutableList.<RowResult<U>> of(),
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
        String dropQuery = "DROP TABLE IF EXISTS " + getFullTableName(tableName);
        PreparedStatement preparedStatement = getPreparedStatement(dropQuery);
        preparedStatement.setConsistencyLevel(ConsistencyLevel.ALL);
        ResultSet resultSet;
        try {
            resultSet = session.executeAsync(preparedStatement.bind()).getUninterruptibly();
            resultSet.all();
        } catch (com.datastax.driver.core.exceptions.UnavailableException e) {
            throw new InsufficientConsistencyException("Drop table requires all Cassandra nodes to be up and available.", e);
        } catch (Throwable t) {
            throw Throwables.throwUncheckedException(t);
        }
        logTracedQuery(dropQuery, resultSet);
        putMetadataWithoutChangingSettings(tableName, PtBytes.EMPTY_BYTE_ARRAY);
    }

    @Override
    public void createTable(final String tableName, final int maxValueSizeInBytes) {
        createTableInternal(tableName);
    }

    private void createTableInternal(final String tableName) {
        for (String name : getAllTableNamesInternal()) {
            if (name.equalsIgnoreCase(tableName)) {
                return;
            }
        }
        String createQuery = "CREATE TABLE "
                + getFullTableName(tableName)
                + " ( "
                + ROW_NAME
                + " blob, "
                + COL_NAME_COL
                + " blob, "
                + TS_COL
                + " bigint, "
                + VALUE_COL
                + " blob, "
                + "PRIMARY KEY ("
                + ROW_NAME
                + ", "
                + COL_NAME_COL
                + ", "
                + TS_COL
                + ")) "
                + "WITH COMPACT STORAGE AND CLUSTERING ORDER BY ("
                + COL_NAME_COL
                + " ASC, "
                + TS_COL
                + " ASC) "
                + "AND compaction = {'sstable_size_in_mb': '80', 'class': 'org.apache.cassandra.db.compaction.LeveledCompactionStrategy'}";
        PreparedStatement preparedStatement = getPreparedStatement(createQuery);
        preparedStatement.setConsistencyLevel(ConsistencyLevel.ALL);
        BoundStatement boundStatement = preparedStatement.bind();
        if (shouldTraceQuery(tableName)) {
            boundStatement.enableTracing();
        }
        ResultSet resultSet;
        try {
            resultSet = session.executeAsync(preparedStatement.bind()).getUninterruptibly();
            resultSet.all();
        } catch (com.datastax.driver.core.exceptions.UnavailableException e) {
            throw new InsufficientConsistencyException("Create table requires all Cassandra nodes to be up and available.", e);
        } catch (Throwable t) {
            throw Throwables.throwUncheckedException(t);
        }
        logTracedQuery(createQuery, resultSet);
        return;
    }

    @Override
    public Set<String> getAllTableNames() {
        return Sets.filter(getAllTableNamesInternal(), new Predicate<String>() {
            @Override
            public boolean apply(String tableName) {
                return !tableName.startsWith("_")
                        || tableName.startsWith(AtlasDbConstants.NAMESPACE_PREFIX);
            }
        });
    }

    private Set<String> getAllTableNamesInternal() {
        BoundStatement boundStatement = statementCache.getUnchecked(
                "SELECT columnfamily_name FROM system.schema_columnfamilies WHERE keyspace_name = ?").bind();
        boundStatement.setString("keyspace_name", keyspace);
        List<Row> rows = session.executeAsync(boundStatement).getUninterruptibly().all();
        Set<String> tableNames = Sets.newHashSetWithExpectedSize(rows.size());
        for (Row row : rows) {
            tableNames.add(row.getString(0));
        }
        return tableNames;
    }

    @Override
    public byte[] getMetadataForTable(String tableName) {
        Cell cell = getMetadataCell(tableName);
        Value v = get(CassandraConstants.METADATA_TABLE, ImmutableMap.of(cell, Long.MAX_VALUE)).get(
                cell);
        if (v == null) {
            return new byte[0];
        } else {
            return v.getContents();
        }
    }

    private Cell getMetadataCell(String tableName) {
        return Cell.create(tableName.getBytes(), "m".getBytes());
    }

    @Override
    public void putMetadataForTable(final String tableName, final byte[] meta) {
        putMetadataWithoutChangingSettings(tableName, meta);
        setSettingsForTable(tableName, meta);
    }

    private void putMetadataWithoutChangingSettings(final String tableName, final byte[] meta) {
        put(
                CassandraConstants.METADATA_TABLE,
                ImmutableMap.of(getMetadataCell(tableName), meta),
                System.currentTimeMillis());
    }

    private void setSettingsForTable(String tableName, byte[] rawMetadata) {
        boolean dbCompressionRequested = false;
        boolean rangeScanAllowed = false;
        boolean negativeLookups = false;
        double falsePositiveChance = CassandraConstants.DEFAULT_LEVELED_COMPACTION_BLOOM_FILTER_FP_CHANCE;

        if (rawMetadata != null && rawMetadata.length != 0) {
            TableMetadata tableMetadata = TableMetadata.BYTES_HYDRATOR.hydrateFromBytes(rawMetadata);
            dbCompressionRequested = tableMetadata.isDbCompressionRequested();
            rangeScanAllowed = tableMetadata.isRangeScanAllowed();
            negativeLookups = tableMetadata.hasNegativeLookups();
        }
        if (negativeLookups) {
            falsePositiveChance = CassandraConstants.NEGATIVE_LOOKUPS_BLOOM_FILTER_FP_CHANCE;
        }

        StringBuilder sb = new StringBuilder();
        sb.append("ALTER TABLE " + getFullTableName(tableName) + " WITH "
                + "bloom_filter_fp_chance = " + falsePositiveChance + " ");
        long chunkLength = CassandraConstants.INDEX_COMPRESSION_KB;
        if (dbCompressionRequested
                && (!tableName.endsWith(AtlasDbConstants.INDEX_SUFFIX) || rangeScanAllowed)) {
            chunkLength = CassandraConstants.TABLE_COMPRESSION_KB;
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
        final Value value = Value.create(PtBytes.EMPTY_BYTE_ARRAY, Value.INVALID_VALUE_TIMESTAMP);
        putInternal(
                tableName,
                Iterables.transform(cells, new Function<Cell, Map.Entry<Cell, Value>>() {
                    @Override
                    public Entry<Cell, Value> apply(Cell cell) {
                        return Maps.immutableEntry(cell, value);
                    }
                }), false);
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
        Validate.isTrue(TransactionConstants.TRANSACTION_TABLE.equals(tableName));
        putInternal(tableName,
                KeyValueServices.toConstantTimestampValues(values.entrySet(), TRANSACTION_TS),
                true);
    }

    private String getFullTableName(String tableName) {
        return keyspace + ".\"" + tableName + "\"";
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
        // nothing to do, yet...
    }
}
