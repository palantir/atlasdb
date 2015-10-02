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

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.cassandra.thrift.CASResult;
import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.Cassandra.Client;
import org.apache.cassandra.thrift.CfDef;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.Deletion;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.KeyRange;
import org.apache.cassandra.thrift.KeySlice;
import org.apache.cassandra.thrift.KsDef;
import org.apache.cassandra.thrift.Mutation;
import org.apache.cassandra.thrift.NotFoundException;
import org.apache.cassandra.thrift.SchemaDisagreementException;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.thrift.SliceRange;
import org.apache.cassandra.thrift.TimedOutException;
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.commons.lang.Validate;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicates;
import com.google.common.base.Stopwatch;
import com.google.common.base.Strings;
import com.google.common.base.Supplier;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.google.common.collect.Ordering;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.Sets;
import com.google.common.collect.TreeMultimap;
import com.google.common.primitives.UnsignedBytes;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfig;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.ColumnSelection;
import com.palantir.atlasdb.keyvalue.api.InsufficientConsistencyException;
import com.palantir.atlasdb.keyvalue.api.KeyAlreadyExistsException;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.RangeRequests;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.keyvalue.cassandra.CassandraKeyValueServices.AllTimestampsCollector;
import com.palantir.atlasdb.keyvalue.cassandra.CassandraKeyValueServices.StartTsResultsCollector;
import com.palantir.atlasdb.keyvalue.cassandra.CassandraKeyValueServices.ThreadSafeResultVisitor;
import com.palantir.atlasdb.keyvalue.impl.AbstractKeyValueService;
import com.palantir.atlasdb.keyvalue.impl.Cells;
import com.palantir.atlasdb.keyvalue.impl.KeyValueServices;
import com.palantir.atlasdb.schema.UpgradeFailedException;
import com.palantir.atlasdb.table.description.TableMetadata;
import com.palantir.common.annotation.Idempotent;
import com.palantir.common.base.ClosableIterator;
import com.palantir.common.base.ClosableIterators;
import com.palantir.common.base.FunctionCheckedException;
import com.palantir.common.base.Throwables;
import com.palantir.common.concurrent.PTExecutors;
import com.palantir.common.exception.PalantirRuntimeException;
import com.palantir.common.pooling.PoolingContainer;
import com.palantir.util.paging.AbstractPagingIterable;
import com.palantir.util.paging.TokenBackedBasicResultsPage;

/**
 *
 * For each C* KVS, it maintains a list of active nodes, and the client connections attached to each node
 *
 * n1->c1, c2, c3
 * n2->c5, c4, c9
 * n3->[N C* thrift client connections]
 *
 * Where {n1, n2, n3} are the active nodes in the C* cluster. Also each
 * node contains the clients which are attached to the node.
 * if some nodes are down, and the change can be detected through active hosts,
 * and these inactive nodes will be removed afterwards.
 */
public class CassandraKeyValueService extends AbstractKeyValueService {

    private static final Logger log = LoggerFactory.getLogger(CassandraKeyValueService.class);

    private final CassandraKeyValueServiceConfig config;
    private final CassandraClientPoolingManager cassandraClientPoolingManager;
    private final CassandraJMXCompactionManager compactionManager;
    protected final ManyClientPoolingContainer containerPoolToUpdate;
    protected final PoolingContainer<Client> clientPool;
    private final ScheduledExecutorService hostRefreshExecutor = PTExecutors.newScheduledThreadPool(1);
    private final ReentrantLock schemaMutationLock = new ReentrantLock(true);

    private ConsistencyLevel readConsistency = ConsistencyLevel.LOCAL_QUORUM;
    private final ConsistencyLevel writeConsistency = ConsistencyLevel.EACH_QUORUM;
    private final ConsistencyLevel deleteConsistency = ConsistencyLevel.ALL;

    private static final long TRANSACTION_TS = 0L;

    public static CassandraKeyValueService create(CassandraKeyValueServiceConfig config) {
        final CassandraKeyValueService ret = new CassandraKeyValueService(config);
        try {
            ret.initializeFromFreshInstance(ret.containerPoolToUpdate.getCurrentHosts(), config.replicationFactor());
            ret.getPoolingManager().submitHostRefreshTask();
        } catch (Exception e) {
            throw Throwables.throwUncheckedException(e);
        }
        return ret;
    }

    protected CassandraKeyValueService(CassandraKeyValueServiceConfig config) {
        super(PTExecutors.newFixedThreadPool(config.poolSize() * 2, PTExecutors.newNamedThreadFactory(false)));
        this.config = config;
        this.containerPoolToUpdate = ManyClientPoolingContainer.create(config);
        this.clientPool = new RetriablePoolingContainer(this.containerPoolToUpdate);
        cassandraClientPoolingManager = new CassandraClientPoolingManager(containerPoolToUpdate, clientPool, config);
        compactionManager = CassandraJMXCompactionManager.newInstance(config);
    }

    public CassandraClientPoolingManager getPoolingManager() {
        return cassandraClientPoolingManager;
    }

    @Override
    public void initializeFromFreshInstance() {
        // we already did our init in our factory method
    }

    protected void initializeFromFreshInstance(List<String> addrList, int replicationFactor) {
        Map<String, Throwable> errorsByHost = Maps.newHashMap();

        int port = config.port();
        String keyspace = config.keyspace();
        boolean safetyDisabled = config.safetyDisabled();
        int socketTimeoutMillis = config.socketTimeoutMillis();
        int socketQueryTimeoutMillis = config.socketQueryTimeoutMillis();
        for (String addr : addrList) {
            Cassandra.Client client = null;
            try {
                client = CassandraKeyValueServices.getClientInternal(addr, port, config.ssl());
                String partitioner = client.describe_partitioner();
                if (safetyDisabled) {
                    Validate.isTrue(CassandraConstants.PARTITIONER.equals(partitioner)
                                    || CassandraConstants.PARTITIONER2.equals(partitioner),
                            "partitioner is: " + partitioner);
                }
                KsDef ks = null;
                try {
                    ks = client.describe_keyspace(keyspace);
                } catch (NotFoundException e) {
                    // need to create key space
                }

                Set<String> currentHosts = cassandraClientPoolingManager.getCurrentHostsFromServer(client);
                cassandraClientPoolingManager.setHostsToCurrentHostNames(currentHosts);
                if (ks != null) { // ks already exists
                    CassandraVerifier.checkAndSetReplicationFactor(client, ks, false, replicationFactor, safetyDisabled);
                    lowerConsistencyWhenSafe(client, ks, replicationFactor);
                    // Can't call system_update_keyspace to update replication factor if CfDefs are set
                    ks.setCf_defs(ImmutableList.<CfDef>of());
                    client.system_update_keyspace(ks);
                    client.set_keyspace(keyspace);
                    createTableInternal(client, CassandraConstants.METADATA_TABLE);
                    CassandraVerifier.sanityCheckRingConsistency(currentHosts, port, keyspace, config.ssl(), safetyDisabled, socketTimeoutMillis, socketQueryTimeoutMillis);

                    upgradeFromOlderInternalSchema(client);
                    return;
                }
                ks = new KsDef(keyspace, CassandraConstants.NETWORK_STRATEGY,
                        ImmutableList.<CfDef>of());
                CassandraVerifier.checkAndSetReplicationFactor(client, ks, true, replicationFactor, safetyDisabled);
                lowerConsistencyWhenSafe(client, ks, replicationFactor);
                ks.setDurable_writes(true);
                client.system_add_keyspace(ks);
                client.set_keyspace(keyspace);
                createTableInternal(client, CassandraConstants.METADATA_TABLE);
                CassandraVerifier.sanityCheckRingConsistency(currentHosts, port, keyspace, config.ssl(), safetyDisabled, socketTimeoutMillis, socketQueryTimeoutMillis);
                CassandraKeyValueServices.failQuickInInitializationIfClusterAlreadyInInconsistentState(client, safetyDisabled);
                return;
            } catch (TException e) {
                log.warn("failed to connect to host: " + addr, e);
                errorsByHost.put(addr.toString(), e);
            } finally {
                if (client != null) {
                    client.getOutputProtocol().getTransport().close();
                }
            }
        }
        throw new IllegalStateException(CassandraKeyValueServices.buildErrorMessage("Could not connect to any Cassandra hosts", errorsByHost));
    }

    private void upgradeFromOlderInternalSchema(Client client) throws NotFoundException, InvalidRequestException, TException {
        Map<String, byte[]> metadataForTables = getMetadataForTables();
        Map<String, byte[]> tablesToUpgrade = Maps.newHashMapWithExpectedSize(metadataForTables.size());

        String keyspace = config.keyspace();
        boolean safetyDisabled = config.safetyDisabled();
        for (CfDef clusterSideCf : client.describe_keyspace(keyspace).getCf_defs()) {
            String tableName = fromInternalTableName(clusterSideCf.getName());
            if (metadataForTables.containsKey(tableName)) {
                byte[] clusterSideMetadata = metadataForTables.get(tableName);
                CfDef clientSideCf = getCfForTable(tableName, clusterSideMetadata);
                if (!CassandraKeyValueServices.isMatchingCf(clientSideCf, clusterSideCf)) { // mismatch; we have changed how we generate schema since we last persisted
                    log.warn("Upgrading table {} to new internal Cassandra schema", tableName);
                    tablesToUpgrade.put(tableName, clusterSideMetadata);
                }
            } else if (!tableName.equals(CassandraConstants.METADATA_TABLE)) { // only expected case
                // Possible to get here from a race condition with another server starting up and performing schema upgrades concurrent with us doing this check
                throw new RuntimeException(new UpgradeFailedException("Found a table " + tableName + " that did not have persisted AtlasDB metadata. If you recently did a Palantir update, try waiting until schema upgrades are completed on all backend servers and restarting this service."));
            }
        }

        // we are racing another server to do these same operations here, but they are idempotent / safe
        if (!tablesToUpgrade.isEmpty()) {
            putMetadataForTables(tablesToUpgrade);
        } else {
            CassandraKeyValueServices.failQuickInInitializationIfClusterAlreadyInInconsistentState(client, safetyDisabled);
        }
    }

    private void lowerConsistencyWhenSafe(Client client, KsDef ks, int desiredRf) {
        String keyspace = config.keyspace();
        boolean safetyDisabled = config.safetyDisabled();

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
    public Map<Cell, Value> getRows(final String tableName, final Iterable<byte[]> rows, ColumnSelection selection, final long startTs) {
        if (!selection.allColumnsSelected()) {
            return getRowsForSpecificColumns(tableName, rows, selection, startTs);
        }

        int fetchBatchCount = config.fetchBatchCount();
        try {
            int rowCount = 0;
            ImmutableMap.Builder<Cell, Value> result = ImmutableMap.<Cell, Value>builder();
            for (final List<byte[]> batch : Iterables.partition(rows, fetchBatchCount)) {
                rowCount += batch.size();
                result.putAll(clientPool.runWithPooledResource(new FunctionCheckedException<Client, Map<Cell, Value>, Exception>() {
                    @Override
                    public Map<Cell, Value> apply(Client client) throws Exception {
                        // We want to get all the columns in the row so set start and end to empty.
                        SliceRange slice = new SliceRange(ByteBuffer.wrap(PtBytes.EMPTY_BYTE_ARRAY), ByteBuffer.wrap(PtBytes.EMPTY_BYTE_ARRAY), false, Integer.MAX_VALUE);
                        SlicePredicate pred = new SlicePredicate();
                        pred.setSlice_range(slice);

                        List<ByteBuffer> rowNames = Lists.newArrayListWithCapacity(batch.size());
                        for (byte[] r : batch) {
                            rowNames.add(ByteBuffer.wrap(r));
                        }
                        ColumnParent colFam = new ColumnParent(internalTableName(tableName));
                        Map<ByteBuffer, List<ColumnOrSuperColumn>> results = multigetInternal(client, tableName, rowNames, colFam, pred, readConsistency);
                        Map<Cell, Value> ret = Maps.newHashMap();
                        new ValueExtractor(ret).extractResults(results, startTs, ColumnSelection.all());
                        return ret;
                    }

                    @Override
                    public String toString() {
                        return "multiget_slice(" + tableName + ", "
                                + batch.size() + " rows" + ")";
                    }
                }));
            }
            if (rowCount > fetchBatchCount) {
                log.warn("Rebatched in getRows a call to " + tableName + " that attempted to multiget "
                        + rowCount + " rows; this may indicate overly-large batching on a higher level.\n"
                        + CassandraKeyValueServices.getFilteredStackTrace("com.palantir"));
            }
            return result.build();
        } catch (Exception e) {
            throw Throwables.throwUncheckedException(e);
        }
    }

    private Map<Cell, Value> getRowsForSpecificColumns(final String tableName,
                                                       final Iterable<byte[]> rows,
                                                       ColumnSelection selection,
                                                       final long startTs) {
        Preconditions.checkArgument(!selection.allColumnsSelected(), "Must select specific columns");

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
        } catch (Exception e) {
            throw Throwables.throwUncheckedException(e);
        }
    }

    @Override
    public Map<Cell, Value> get(String tableName, Map<Cell, Long> timestampByCell) {
        if (timestampByCell.isEmpty()) {
            log.info("Attempted get on '{}' table with empty cells", tableName);
            return ImmutableMap.of();
        }

        try {
            Long firstTs = timestampByCell.values().iterator().next();
            if (Iterables.all(timestampByCell.values(), Predicates.equalTo(firstTs))) {
                StartTsResultsCollector collector = new StartTsResultsCollector(firstTs);
                loadWithTs(tableName, timestampByCell.keySet(), firstTs, collector, readConsistency);
                return collector.collectedResults;
            }

            SetMultimap<Long, Cell> cellsByTs = Multimaps.invertFrom(
                    Multimaps.forMap(timestampByCell), HashMultimap.<Long, Cell>create());
            Builder<Cell, Value> builder = ImmutableMap.builder();
            for (long ts : cellsByTs.keySet()) {
                StartTsResultsCollector collector = new StartTsResultsCollector(ts);
                loadWithTs(tableName, cellsByTs.get(ts), ts, collector, readConsistency);
                builder.putAll(collector.collectedResults);
            }
            return builder.build();
        } catch (Exception e) {
            throw Throwables.throwUncheckedException(e);
        }
    }

    // NOTE: This code makes a call to cassandra for each column concurrently
    // TODO: after cassandra api change: handle different column select per row
    private void loadWithTs(final String tableName,
                            final Set<Cell> cells,
                            final long startTs,
                            final ThreadSafeResultVisitor v,
                            final ConsistencyLevel consistency) throws Exception {
        final ColumnParent colFam = new ColumnParent(internalTableName(tableName));
        final Multimap<byte[], Cell> cellsByCol = TreeMultimap.create(UnsignedBytes.lexicographicalComparator(), Ordering.natural());
        for (Cell cell : cells) {
            cellsByCol.put(cell.getColumnName(), cell);
        }
        int fetchBatchCount = config.fetchBatchCount();
        List<Future<?>> futures = Lists.newArrayListWithCapacity(cellsByCol.size());
        for (final byte[] col : cellsByCol.keySet()) {
            if (cellsByCol.get(col).size() > fetchBatchCount) {
                log.warn("Re-batching in loadWithTs a call to " + tableName + " that attempted to multiget "
                        + cellsByCol.get(col).size() + " rows; this may indicate overly-large batching on a higher level.\n"
                        + CassandraKeyValueServices.getFilteredStackTrace("com.palantir"));
            }
            for (final List<Cell> partition: Iterables.partition(cellsByCol.get(col), fetchBatchCount)) {
                futures.add(executor.submit(new Callable<Void>() {
                    @Override
                    public Void call() throws Exception {
                        return clientPool.runWithPooledResource(new FunctionCheckedException<Client, Void, Exception>() {
                            @Override
                            public Void apply(Client client) throws Exception {
                                ByteBuffer start = CassandraKeyValueServices.makeCompositeBuffer(col, startTs - 1);
                                ByteBuffer end = CassandraKeyValueServices.makeCompositeBuffer(col, -1);
                                SliceRange slice = new SliceRange(start, end, false, 1);
                                SlicePredicate pred = new SlicePredicate();
                                pred.setSlice_range(slice);

                                List<ByteBuffer> rowNames = Lists.newArrayListWithCapacity(partition.size());
                                for (Cell c : partition) {
                                    rowNames.add(ByteBuffer.wrap(c.getRowName()));
                                }
                                Map<ByteBuffer, List<ColumnOrSuperColumn>> results = multigetInternal(client, tableName, rowNames, colFam, pred, consistency);
                                v.visit(results);
                                return null;
                            }

                            @Override
                            public String toString() {
                                return "multiget_slice(" + colFam + ", "
                                        + partition.size() + " rows" + ")";
                            }
                        });
                    }
                }));
            }
        }
        for (Future<?> f : futures) {
            try {
                f.get();
            } catch (InterruptedException e) {
                throw Throwables.throwUncheckedException(e);
            } catch (ExecutionException e) {
                Throwables.throwIfInstance(e, Error.class);
                Throwables.throwIfInstance(e, Exception.class);
                throw Throwables.throwUncheckedException(e);
            }
        }
    }

    @Override
    public Map<Cell, Long> getLatestTimestamps(String tableName, Map<Cell, Long> timestampByCell) {
        // TODO: optimize by only getting column name after cassandra api change
        return super.getLatestTimestamps(tableName, timestampByCell);
    }

    @Override
    public void put(final String tableName, final Map<Cell, byte[]> values, final long timestamp) {
        try {
            putInternal(tableName, KeyValueServices.toConstantTimestampValues(values.entrySet(), timestamp));
        } catch (Exception e) {
            throw Throwables.throwUncheckedException(e);
        }
    }

    @Override
    public void putWithTimestamps(String tableName, Multimap<Cell, Value> values) {
        try {
            putInternal(tableName, values.entries());
        } catch (Exception e) {
            throw Throwables.throwUncheckedException(e);
        }
    }

    @Override
    protected int getMultiPutBatchCount() {
        return CassandraConstants.PUT_BATCH_SIZE;
    }

    private void putInternal(final String tableName,
                             final Iterable<Map.Entry<Cell, Value>> values) throws Exception {
        putInternal(tableName, values, -1);
    }

    protected void putInternal(final String tableName,
                             final Iterable<Map.Entry<Cell, Value>> values,
                             final int ttl) throws Exception {
        final int mutationBatchCount = config.mutationBatchCount();
        final long mutationBatchSizeBytes = config.mutationBatchSizeBytes();
        clientPool.runWithPooledResource(new FunctionCheckedException<Client, Void, Exception>() {
            @Override
            public Void apply(Client client) throws Exception {
                Function<Entry<Cell, Value>, Long> sizingFunction = new Function<Entry<Cell, Value>, Long>() {
                    @Override
                    public Long apply(Entry<Cell, Value> input) {
                        return input.getValue().getContents().length + 4L + Cells.getApproxSizeOfCell(input.getKey());
                    }
                };

                for (List<Entry<Cell, Value>> partition : partitionByCountAndBytes(values, mutationBatchCount,
                        mutationBatchSizeBytes, tableName, sizingFunction)) {
                    Map<ByteBuffer, Map<String, List<Mutation>>> map = Maps.newHashMap();
                    for (Map.Entry<Cell, Value> e : partition) {
                        Cell cell = e.getKey();
                        Column col = createColumn(cell, e.getValue(), ttl);

                        ColumnOrSuperColumn colOrSup = new ColumnOrSuperColumn();
                        colOrSup.setColumn(col);
                        Mutation m = new Mutation();
                        m.setColumn_or_supercolumn(colOrSup);

                        ByteBuffer rowName = ByteBuffer.wrap(cell.getRowName());

                        Map<String, List<Mutation>> rowPuts = map.get(rowName);
                        if (rowPuts == null) {
                            rowPuts = Maps.<String, List<Mutation>>newHashMap();
                            map.put(rowName, rowPuts);
                        }

                        List<Mutation> tableMutations = rowPuts.get(internalTableName(tableName));
                        if (tableMutations == null) {
                            tableMutations = Lists.<Mutation>newArrayList();
                            rowPuts.put(internalTableName(tableName), tableMutations);
                        }

                        tableMutations.add(m);
                    }
                    batchMutateInternal(client, tableName, map, writeConsistency);
                }
                return null;
            }

            @Override
            public String toString() {
                return "batch_mutate(" + tableName + ", " + Iterables.size(values) + " values, " + ttl + " ttl sec)";
            }
        });
    }

    private Column createColumn(Cell cell, Value value, final int ttl) {
        byte[] contents = value.getContents();
        long timestamp = value.getTimestamp();
        ByteBuffer colName = CassandraKeyValueServices.makeCompositeBuffer(cell.getColumnName(), timestamp);
        Column col = new Column();
        col.setName(colName);
        col.setValue(contents);
        col.setTimestamp(timestamp);

        if (cell.getTtlDurationMillis() > 0) {
            col.setTtl(CassandraKeyValueServices.convertTtl(cell.getTtlDurationMillis(), TimeUnit.MILLISECONDS));
        }

        if (ttl > 0) {
            col.setTtl(ttl);
        }

        return col;
    }

    private void batchMutateInternal(Client client,
                                     String tableName,
                                     Map<ByteBuffer,
                                     Map<String, List<Mutation>>> map,
                                     ConsistencyLevel consistency)
            throws TException, InvalidRequestException, UnavailableException, TimedOutException {
        if (shouldTraceQuery(tableName)) {
            ByteBuffer recv_trace = client.trace_next_query();
            Stopwatch stopwatch = Stopwatch.createStarted();
            client.batch_mutate(map, consistency);
            long duration = stopwatch.elapsed(TimeUnit.MILLISECONDS);
            if (duration > getMinimumDurationToTraceMillis()) {
                log.error("Traced a call to " + tableName + " that took " + duration + " ms."
                        + " It will appear in system_traces with UUID=" + CassandraKeyValueServices.convertCassandraByteBufferUUIDtoString(recv_trace));
            }
        } else {
            client.batch_mutate(map, consistency);
        }
    }

    private Map<ByteBuffer, List<ColumnOrSuperColumn>> multigetInternal(Client client,
                                                                        String tableName,
                                                                        List<ByteBuffer> rowNames,
                                                                        ColumnParent colFam,
                                                                        SlicePredicate pred,
                                                                        ConsistencyLevel consistency)
            throws TException, InvalidRequestException, UnavailableException, TimedOutException {
        Map<ByteBuffer, List<ColumnOrSuperColumn>> results;
        if (shouldTraceQuery(tableName)) {
            ByteBuffer recv_trace = client.trace_next_query();
            Stopwatch stopwatch = Stopwatch.createStarted();
            results = client.multiget_slice(rowNames, colFam, pred, consistency);
            long duration = stopwatch.elapsed(TimeUnit.MILLISECONDS);
            if (duration > getMinimumDurationToTraceMillis()) {
                log.error("Traced a call to " + tableName + " that took " + duration + " ms."
                        + " It will appear in system_traces with UUID="
                        + CassandraKeyValueServices.convertCassandraByteBufferUUIDtoString(recv_trace));
            }
        } else {
            results = client.multiget_slice(rowNames, colFam, pred, consistency);
        }
        return results;
    }

    @Override
    public void truncateTable(final String tableName) {
        try {
            clientPool.runWithPooledResource(new FunctionCheckedException<Client, Void, Exception>() {
                @Override
                public Void apply(Client client) throws Exception {
                    KsDef ks = client.describe_keyspace(config.keyspace());

                    for (CfDef cf : ks.getCf_defs()) {
                        if (cf.getName().equalsIgnoreCase(internalTableName(tableName))) {
                            if (shouldTraceQuery(tableName)) {
                                ByteBuffer recv_trace = client.trace_next_query();
                                Stopwatch stopwatch = Stopwatch.createStarted();
                                client.truncate(internalTableName(tableName));
                                long duration = stopwatch.elapsed(TimeUnit.MILLISECONDS);
                                if (duration > getMinimumDurationToTraceMillis()) {
                                    log.error("Traced a call to " + tableName + " that took " + duration + " ms."
                                            + " It will appear in system_traces with UUID=" + CassandraKeyValueServices.convertCassandraByteBufferUUIDtoString(recv_trace));
                                }
                            } else {
                                client.truncate(internalTableName(tableName));
                            }
                            return null;
                        }
                    }
                    throw new IllegalArgumentException("Attempted to truncate a table that we could not locate in the active schema.");
                }

                @Override
                public String toString() {
                    return "truncate(" + "tableName" + ")";
                }
            });
        } catch (UnavailableException e) {
            throw new InsufficientConsistencyException("Truncate requires all Cassandra nodes to be up and available.", e);
        } catch (Exception e) {
            throw Throwables.throwUncheckedException(e);
        }
    }

    @Override
    public void delete(final String tableName, final Multimap<Cell, Long> keys) {
        try {
            clientPool.runWithPooledResource(new FunctionCheckedException<Client, Void, Exception>() {
                @Override
                public Void apply(Client client) throws Exception {
                    // Delete must delete in the order of timestamp and we don't trust batch_mutate to do it
                    // atomically so we have to potentially do many deletes if there are many timestamps for the
                    // same key.
                    Map<Integer, Map<ByteBuffer,Map<String,List<Mutation>>>> maps = Maps.newTreeMap();
                    for (Cell key : keys.keySet()) {
                        int mapIndex = 0;
                        for (long ts : Ordering.natural().immutableSortedCopy(keys.get(key))) {
                            if (!maps.containsKey(mapIndex)) {
                                maps.put(mapIndex, Maps.<ByteBuffer,Map<String,List<Mutation>>>newHashMap());
                            }
                            Map<ByteBuffer,Map<String,List<Mutation>>> map = maps.get(mapIndex);
                            ByteBuffer colName = CassandraKeyValueServices.makeCompositeBuffer(key.getColumnName(), ts);
                            SlicePredicate pred = new SlicePredicate();
                            pred.setColumn_names(Arrays.asList(colName));
                            Deletion del = new Deletion();
                            del.setPredicate(pred);
                            del.setTimestamp(Long.MAX_VALUE);
                            Mutation m = new Mutation();
                            m.setDeletion(del);
                            ByteBuffer rowName = ByteBuffer.wrap(key.getRowName());
                            if (!map.containsKey(rowName)) {
                                map.put(rowName, Maps.<String, List<Mutation>>newHashMap());
                            }
                            Map<String, List<Mutation>> rowPuts = map.get(rowName);
                            if (!rowPuts.containsKey(internalTableName(tableName))) {
                                rowPuts.put(internalTableName(tableName), Lists.<Mutation>newArrayList());
                            }
                            rowPuts.get(internalTableName(tableName)).add(m);
                            mapIndex++;
                        }
                    }
                    for (Map<ByteBuffer,Map<String,List<Mutation>>> map : maps.values()) {
                        // NOTE: we run with ConsistencyLevel.ALL here instead of ConsistencyLevel.QUORUM
                        // because we want to remove all copies of this data
                        batchMutateInternal(client, tableName, map, deleteConsistency);
                    }
                    return null;
                }

                @Override
                public String toString() {
                    return "batch_mutate(" + tableName + ", " + keys.size() + " keys" + ")";
                }
            });
        } catch (Exception e) {
            throw Throwables.throwUncheckedException(e);
        }
    }

    // update CKVS.isMatchingCf if you update this method
    private CfDef getCfForTable(String tableName, byte[] rawMetadata) {
        Map<String, String> compressionOptions = Maps.newHashMap();
        CfDef cf = CassandraConstants.getStandardCfDef(config.keyspace(), internalTableName(tableName));

        boolean negativeLookups = false;
        double falsePositiveChance = CassandraConstants.DEFAULT_LEVELED_COMPACTION_BLOOM_FILTER_FP_CHANCE;
        int explicitCompressionBlockSizeKB = 0;

        if (rawMetadata != null && rawMetadata.length != 0) {
            TableMetadata tableMetadata = TableMetadata.BYTES_HYDRATOR.hydrateFromBytes(rawMetadata);
            negativeLookups = tableMetadata.hasNegativeLookups();
            explicitCompressionBlockSizeKB = tableMetadata.getExplicitCompressionBlockSizeKB();
        }

        if (explicitCompressionBlockSizeKB != 0) {
            compressionOptions.put(CassandraConstants.CFDEF_COMPRESSION_TYPE_KEY, CassandraConstants.DEFAULT_COMPRESSION_TYPE);
            compressionOptions.put(CassandraConstants.CFDEF_COMPRESSION_CHUNK_LENGTH_KEY, Integer.toString(explicitCompressionBlockSizeKB));
        } else {
            // We don't really need compression here nor anticipate it will garner us any gains
            // (which is why we're doing such a small chunk size), but this is how we can get "free" CRC checking.
            compressionOptions.put(CassandraConstants.CFDEF_COMPRESSION_TYPE_KEY, CassandraConstants.DEFAULT_COMPRESSION_TYPE);
            compressionOptions.put(CassandraConstants.CFDEF_COMPRESSION_CHUNK_LENGTH_KEY, Integer.toString(AtlasDbConstants.MINIMUM_COMPRESSION_BLOCK_SIZE_KB));
        }

        if (negativeLookups) {
            falsePositiveChance = CassandraConstants.NEGATIVE_LOOKUPS_BLOOM_FILTER_FP_CHANCE;
        }

        cf.setBloom_filter_fp_chance(falsePositiveChance);
        cf.setCompression_options(compressionOptions);
        return cf;
    }

    //TODO: after cassandra change: handle multiRanges
    @Override
    @Idempotent
    public Map<RangeRequest, TokenBackedBasicResultsPage<RowResult<Value>, byte[]>> getFirstBatchForRanges(String tableName,
            Iterable<RangeRequest> rangeRequests,
            long timestamp) {
        int concurrency = config.rangesConcurrency();
        return KeyValueServices.getFirstBatchForRangesUsingGetRangeConcurrent(
                executor, this, tableName, rangeRequests, timestamp, concurrency);
    }


    // TODO: after cassandra change: handle reverse ranges
    // TODO: after cassandra change: handle column filtering
    @Override
    @Idempotent
    public ClosableIterator<RowResult<Value>> getRange(String tableName, final RangeRequest rangeRequest, final long timestamp) {
        return getRangeWithPageCreator(tableName, rangeRequest, timestamp, readConsistency, ValueExtractor.SUPPLIER);
    }

    @Override
    @Idempotent
    public ClosableIterator<RowResult<Set<Long>>> getRangeOfTimestamps(String tableName, RangeRequest rangeRequest, long timestamp) {
        return getRangeWithPageCreator(tableName, rangeRequest, timestamp, deleteConsistency, TimestampExtractor.SUPPLIER);
    }

    @Override
    @Idempotent
    public ClosableIterator<RowResult<Set<Value>>> getRangeWithHistory(String tableName, RangeRequest rangeRequest, long timestamp) {
        return getRangeWithPageCreator(tableName, rangeRequest, timestamp, deleteConsistency, HistoryExtractor.SUPPLIER);
    }

    public <T, U> ClosableIterator<RowResult<U>> getRangeWithPageCreator(final String tableName,
                                                                         final RangeRequest rangeRequest,
                                                                         final long timestamp,
                                                                         final ConsistencyLevel consistency,
                                                                         final Supplier<ResultsExtractor<T, U>> resultsExtractor) {
        if (rangeRequest.isReverse()) {
            throw new UnsupportedOperationException();
        }
        final int batchHint = rangeRequest.getBatchHint() == null ? 100 : rangeRequest.getBatchHint();
        SliceRange slice = new SliceRange(ByteBuffer.wrap(PtBytes.EMPTY_BYTE_ARRAY), ByteBuffer.wrap(PtBytes.EMPTY_BYTE_ARRAY), false, Integer.MAX_VALUE);
        final SlicePredicate pred = new SlicePredicate();
        pred.setSlice_range(slice);

        final ColumnParent colFam = new ColumnParent(internalTableName(tableName));
        final ColumnSelection selection = rangeRequest.getColumnNames().isEmpty() ? ColumnSelection.all()
                : ColumnSelection.create(rangeRequest.getColumnNames());
        return ClosableIterators.wrap(
                new AbstractPagingIterable<RowResult<U>, TokenBackedBasicResultsPage<RowResult<U>, byte[]>>() {
                    @Override
                    protected TokenBackedBasicResultsPage<RowResult<U>, byte[]> getFirstPage()
                            throws Exception {
                        return getPage(rangeRequest.getStartInclusive());
                    }

                    @Override
                    protected TokenBackedBasicResultsPage<RowResult<U>, byte[]> getNextPage(
                            TokenBackedBasicResultsPage<RowResult<U>, byte[]> previous)
                            throws Exception {
                        return getPage(previous.getTokenForNextPage());
                    }

                    TokenBackedBasicResultsPage<RowResult<U>, byte[]> getPage(
                            final byte[] startKey) throws Exception {
                        return clientPool.runWithPooledResource(new FunctionCheckedException<Client, TokenBackedBasicResultsPage<RowResult<U>, byte[]>, Exception>() {
                            @Override
                            public TokenBackedBasicResultsPage<RowResult<U>, byte[]> apply(Client client) throws Exception {
                                final byte[] endExclusive = rangeRequest.getEndExclusive();

                                KeyRange keyRange = new KeyRange(batchHint);
                                keyRange.setStart_key(startKey);
                                if (endExclusive.length == 0) {
                                    keyRange.setEnd_key(endExclusive);
                                } else {
                                    // We need the previous name because this is inclusive, not exclusive
                                    keyRange.setEnd_key(RangeRequests.previousLexicographicName(endExclusive));
                                }

                                List<KeySlice> firstPage;
                                try {
                                    if (shouldTraceQuery(tableName)) {
                                        ByteBuffer recv_trace = client.trace_next_query();
                                        Stopwatch stopwatch = Stopwatch.createStarted();
                                        firstPage = client.get_range_slices(colFam, pred, keyRange, consistency);
                                        long duration = stopwatch.elapsed(TimeUnit.MILLISECONDS);
                                        if (duration > getMinimumDurationToTraceMillis()) {
                                            log.error("Traced a call to " + tableName + " that took " + duration + " ms. It will appear in system_traces with UUID=" + CassandraKeyValueServices.convertCassandraByteBufferUUIDtoString(recv_trace));
                                        }
                                    } else {
                                        firstPage = client.get_range_slices(colFam, pred, keyRange, consistency);
                                    }
                                } catch (UnavailableException e) {
                                    if (consistency.equals(ConsistencyLevel.ALL)) {
                                        throw new InsufficientConsistencyException("This operation requires all Cassandra nodes to be up and available.", e);
                                    } else {
                                        throw e;
                                    }
                                }

                                Map<ByteBuffer, List<ColumnOrSuperColumn>> colsByKey = CassandraKeyValueServices.getColsByKey(firstPage);
                                return resultsExtractor.get().getPageFromRangeResults(colsByKey, timestamp, selection, endExclusive);
                            }

                            @Override
                            public String toString() {
                                return "get_range_slices(" + colFam + ")";
                            }
                        });
                    }
                }.iterator());
    }

    @Override
    public void dropTable(final String tableName) {
        try {
            trySchemaMutationLock();
            clientPool.runWithPooledResource(new FunctionCheckedException<Client, Void, Exception>() {
                @Override
                public Void apply(Client client) throws Exception {
                    KsDef ks = client.describe_keyspace(config.keyspace());

                    for (CfDef cf : ks.getCf_defs()) {
                        if (cf.getName().equalsIgnoreCase(internalTableName(tableName))) {
                            client.system_drop_column_family(internalTableName(tableName));
                            putMetadataWithoutChangingSettings(tableName, PtBytes.EMPTY_BYTE_ARRAY);
                            CassandraKeyValueServices.waitForSchemaVersions(client, tableName);
                            return null;
                        }
                    }
                    return null;
                }
            });
        } catch (UnavailableException e) {
            throw new InsufficientConsistencyException("Drop table requires all Cassandra nodes to be up and available.", e);
        } catch (Exception e) {
            throw Throwables.throwUncheckedException(e);
        } finally {
            schemaMutationLock.unlock();
        }
    }

    private static String internalTableName(String tableName) {
        if (tableName.startsWith("_")) {
            return tableName;
        }
        return tableName.replaceFirst("\\.", "_");
    }

    private String fromInternalTableName(String tableName) {
        if (tableName.startsWith("_")) {
            return tableName;
        }
        return tableName.replaceFirst("_", ".");
    }

    /**
     * Main gains here vs. dropTable:
     *    - problems excepting, we will basically be serializing a rapid series of schema changes
     *      through a single host checked out from the client pool, so reduced chance of schema disagreement issues
     *    - client-side in-memory lock to prevent misbehaving callers from shooting themselves in the foot
     *    - one less round trip
     */
    @Override
    public void dropTables(final Set<String> tablesToDrop) {
        try {
            trySchemaMutationLock();
            clientPool.runWithPooledResource(new FunctionCheckedException<Client, Void, Exception>() {
                @Override
                public Void apply(Client client) throws Exception {
                    KsDef ks = client.describe_keyspace(config.keyspace());
                    Set<String> existingTables = Sets.newHashSet();

                    for (CfDef cf : ks.getCf_defs()) {
                        existingTables.add(cf.getName().toLowerCase());
                    }

                    for (String table : tablesToDrop) {
                        CassandraVerifier.sanityCheckTableName(table);
                        String caseInsensitiveTable = table.toLowerCase();

                        if (existingTables.contains(caseInsensitiveTable)) {
                            client.system_drop_column_family(caseInsensitiveTable);
                            putMetadataWithoutChangingSettings(caseInsensitiveTable, PtBytes.EMPTY_BYTE_ARRAY);
                        } else {
                            log.warn(String.format("Ignored call to drop a table (%s) that already existed.", table));
                        }
                    }
                    CassandraKeyValueServices.waitForSchemaVersions(client, "(all tables in a call to dropTables)");
                    return null;
                }
            });
        } catch (UnavailableException e) {
            throw new PalantirRuntimeException("Dropping tables requires all Cassandra nodes to be up and available.");
        } catch (Exception e) {
            throw Throwables.throwUncheckedException(e);
        } finally {
            schemaMutationLock.unlock();
        }
    }

    @Override
    public void createTable(final String tableName, final int maxValueSizeInBytes) {
        CassandraVerifier.sanityCheckTableName(tableName);
        try {
            trySchemaMutationLock();
            clientPool.runWithPooledResource(new FunctionCheckedException<Client, Void, Exception>() {
                @Override
                public Void apply(Client client) throws Exception {
                    createTableInternal(client, tableName);
                    return null;
                }
            });
        } catch (UnavailableException e) {
            throw new InsufficientConsistencyException("Create table requires all Cassandra nodes to be up and available.", e);
        } catch (Exception e) {
            throw Throwables.throwUncheckedException(e);
        } finally {
            schemaMutationLock.unlock();
        }
    }

    private void createTableInternal(Client client, final String tableName) throws InvalidRequestException, SchemaDisagreementException, TException, NotFoundException {
        String keyspace = config.keyspace();
        KsDef ks = client.describe_keyspace(keyspace);
        for (CfDef cf : ks.getCf_defs()) {
            if (cf.getName().equalsIgnoreCase(internalTableName(tableName))) {
                return;
            }
        }
        CfDef cf = CassandraConstants.getStandardCfDef(keyspace, internalTableName(tableName));
        client.system_add_column_family(cf);
        CassandraKeyValueServices.waitForSchemaVersions(client, tableName);
        return;
    }

    /**
     * Main gains here vs. createTable:
     *    - problems excepting, we will basically be serializing a rapid series of schema changes
     *      through a single host checked out from the client pool, so reduced chance of schema disagreement issues
     *    - client-side in-memory lock to prevent misbehaving callers from shooting themselves in the foot
     *    - one less round trip
     */
    @Override
    public void createTables(final Map<String, Integer> tableNamesToMaxValueSizeInBytes) {
        try {
            trySchemaMutationLock();
            clientPool.runWithPooledResource(new FunctionCheckedException<Client, Void, Exception>() {
                @Override
                public Void apply(Client client) throws Exception {
                    KsDef ks = client.describe_keyspace(config.keyspace());
                    Set<String> tablesToCreate = tableNamesToMaxValueSizeInBytes.keySet();
                    Set<String> existingTables = Sets.newHashSet();

                    for (CfDef cf : ks.getCf_defs()) {
                        existingTables.add(cf.getName().toLowerCase());
                    }

                    for (String tableName : tablesToCreate) {
                        CassandraVerifier.sanityCheckTableName(tableName);

                        if (!existingTables.contains(internalTableName(tableName.toLowerCase()))) {
                            CfDef newCf = CassandraConstants.getStandardCfDef(config.keyspace(), internalTableName(tableName));
                            client.system_add_column_family(newCf);
                        } else {
                            log.warn(String.format("Ignored call to create a table (%s) that already existed.", tableName));
                        }
                    }
                    CassandraKeyValueServices.waitForSchemaVersions(client, "(all tables in a call to createTables)");
                    return null;
                }
            });
        } catch (UnavailableException e) {
            throw new PalantirRuntimeException("Creating tables requires all Cassandra nodes to be up and available.");
        } catch (Exception e) {
            throw Throwables.throwUncheckedException(e);
        } finally {
            schemaMutationLock.unlock();
        }
    }

    @Override
    public Set<String> getAllTableNames() {
        try {
            return clientPool.runWithPooledResource(new FunctionCheckedException<Client, Set<String>, Exception>() {
                @Override
                public Set<String> apply(Client client) throws Exception {
                    KsDef ks = client.describe_keyspace(config.keyspace());

                    Set<String> ret = Sets.newHashSet();
                    for (CfDef cf : ks.getCf_defs()) {
                        if (!cf.getName().equals(CassandraConstants.METADATA_TABLE)) {
                            ret.add(fromInternalTableName(cf.getName()));
                        }
                    }
                    return ret;
                }

                @Override
                public String toString() {
                    return "describe_keyspace(" + config.keyspace() + ")";
                }
            });
        } catch (Exception e) {
            throw Throwables.throwUncheckedException(e);
        }
    }

    @Override
    public byte[] getMetadataForTable(String tableName) {
        Cell cell = getMetadataCell(tableName);
        Value v = get(CassandraConstants.METADATA_TABLE, ImmutableMap.of(cell, Long.MAX_VALUE)).get(cell);
        if (v == null) {
            return PtBytes.EMPTY_BYTE_ARRAY;
        } else {
            return v.getContents();
        }
    }

    @Override
    public Map<String, byte[]> getMetadataForTables() {
        Map<String, byte[]> tableToMetadataContents = Maps.newHashMap();
        ClosableIterator<RowResult<Value>> range = getRange(CassandraConstants.METADATA_TABLE, RangeRequest.all(), Long.MAX_VALUE);
        try {
            Set<String> currentlyExistingTables = getAllTableNames();
            while (range.hasNext()) {
                RowResult<Value> valueRow = range.next();
                Iterable<Entry<Cell, Value>> cells = valueRow.getCells();

                for (Entry<Cell, Value> entry : cells) {
                    Value value = entry.getValue();
                    String tableName = new String(entry.getKey().getRowName());
                    if (currentlyExistingTables.contains(tableName)) {
                        byte[] contents;
                        if (value == null) {
                            contents = PtBytes.EMPTY_BYTE_ARRAY;
                        } else {
                            contents = value.getContents();
                        }

                        tableToMetadataContents.put(tableName, contents);
                    } else {
                        log.info("Non-existing table {}: {}", tableName, value);
                    }
                }
            }
        } finally {
            range.close();
        }
        return tableToMetadataContents;
    }

    private Cell getMetadataCell(String tableName) { // would have preferred an explicit charset, but thrift uses default internally
        return Cell.create(tableName.getBytes(Charset.defaultCharset()), "m".getBytes());
    }

    @Override
    public void putMetadataForTable(final String tableName, final byte[] meta) {
        putMetadataForTables(ImmutableMap.of(tableName, meta));
    }

    @Override
    public void putMetadataForTables(final Map<String, byte[]> tableNameToMetadata) {
        final Map<Cell, byte[]> metadataRequestedForUpdate = Maps.newHashMapWithExpectedSize(tableNameToMetadata.size());
        for (Entry<String, byte[]> tableEntry : tableNameToMetadata.entrySet()) {
            metadataRequestedForUpdate.put(getMetadataCell(tableEntry.getKey()), tableEntry.getValue());
        }

        Map<Cell, Long> requestForLatestDbSideMetadata = Maps.transformValues(metadataRequestedForUpdate, Functions.constant(Long.MAX_VALUE));

        // technically we're racing other services from here on, during an update period,
        // but the penalty for not caring is just some superfluous schema mutations and a few dead rows in the metadata table.
        Map<Cell, Value> persistedMetadata = get(CassandraConstants.METADATA_TABLE, requestForLatestDbSideMetadata);
        final Map<Cell, byte[]> newMetadata = Maps.newHashMap();
        final Collection<CfDef> updatedCfs = Lists.newArrayList();
        for(Entry<Cell, byte[]> entry : metadataRequestedForUpdate.entrySet()) {
            Value val = persistedMetadata.get(entry.getKey());
            if (val == null || !Arrays.equals(val.getContents(), entry.getValue())) {
                newMetadata.put(entry.getKey(), entry.getValue());
                updatedCfs.add(getCfForTable(new String(entry.getKey().getRowName()), entry.getValue()));
            }
        }

        if (!newMetadata.isEmpty()) {
            try {
                trySchemaMutationLock();
                clientPool.runWithPooledResource(new FunctionCheckedException<Client, Void, Exception>() {
                    @Override
                    public Void apply(Client client) throws Exception {
                        for (CfDef cf : updatedCfs) {
                            client.system_update_column_family(cf);
                        }

                        CassandraKeyValueServices.waitForSchemaVersions(client, "(all tables in a call to putMetadataForTables)");
                        // Done with actual schema mutation, push the metadata
                        put(CassandraConstants.METADATA_TABLE, newMetadata, System.currentTimeMillis());
                        return null;
                    }
                });
            } catch (Exception e) {
                throw Throwables.throwUncheckedException(e);
            } finally {
                schemaMutationLock.unlock();
            }
        }
    }

    private void putMetadataWithoutChangingSettings(final String tableName, final byte[] meta) {
        put(CassandraConstants.METADATA_TABLE, ImmutableMap.of(getMetadataCell(tableName), meta), System.currentTimeMillis());
    }

    @Override
    public void close() {
        clientPool.shutdownPooling();
        hostRefreshExecutor.shutdown();
        if (compactionManager != null) {
            compactionManager.close();
        }
        super.close();
    }

    @Override
    public void teardown() {
        close();
    }

    @Override
    public void addGarbageCollectionSentinelValues(String tableName, Set<Cell> cells) {
        try {
            final Value value = Value.create(PtBytes.EMPTY_BYTE_ARRAY, Value.INVALID_VALUE_TIMESTAMP);
            putInternal(tableName, Iterables.transform(cells, new Function<Cell, Map.Entry<Cell, Value>>() {
                @Override
                public Entry<Cell, Value> apply(Cell cell) {
                    return Maps.immutableEntry(cell, value);
                }
            }));
        } catch (Exception e) {
            throw Throwables.throwUncheckedException(e);
        }
    }

    @Override
    public Multimap<Cell, Long> getAllTimestamps(String tableName, Set<Cell> cells, long ts) {
        AllTimestampsCollector collector = new AllTimestampsCollector();
        try {
            loadWithTs(tableName, cells, ts, collector, deleteConsistency);
        } catch (UnavailableException e) {
            throw new InsufficientConsistencyException("Get all timestamps requires all Cassandra nodes to be up and available.", e);
        } catch (Exception e) {
            throw Throwables.throwUncheckedException(e);
        }
        return collector.collectedResults;
    }

    @Override
    public void putUnlessExists(final String tableName, final Map<Cell, byte[]> values)
            throws KeyAlreadyExistsException {
        Validate.isTrue(AtlasDbConstants.ATOMIC_TABLES.contains(tableName));
        try {
            clientPool.runWithPooledResource(new FunctionCheckedException<Client, Void, Exception>() {
                @Override
                public Void apply(Client client) throws Exception {
                    for (Map.Entry<Cell, byte[]> e : values.entrySet()) {
                        ByteBuffer rowName = ByteBuffer.wrap(e.getKey().getRowName());
                        byte[] contents = e.getValue();
                        long timestamp = TRANSACTION_TS;
                        byte[] colName = CassandraKeyValueServices.makeCompositeBuffer(e.getKey().getColumnName(), timestamp).array();
                        Column col = new Column();
                        col.setName(colName);
                        col.setValue(contents);
                        col.setTimestamp(timestamp);
                        CASResult casResult = client.cas(
                                rowName,
                                tableName,
                                ImmutableList.<Column>of(),
                                ImmutableList.of(col),
                                ConsistencyLevel.SERIAL,
                                writeConsistency);
                        if (!casResult.isSuccess()) {
                            throw new KeyAlreadyExistsException("This transaction row already exists.", ImmutableList.of(e.getKey()));
                        }
                    }
                    return null;
                }
            });
        } catch (Exception e) {
            throw Throwables.throwUncheckedException(e);
        }

    }

    private void trySchemaMutationLock() throws InterruptedException, TimeoutException {
        if (!schemaMutationLock.tryLock(CassandraConstants.SECONDS_TO_WAIT_FOR_SCHEMA_MUTATION_LOCK, TimeUnit.SECONDS)) {
            throw new TimeoutException("AtlasDB was unable to get a lock on Cassandra system schema mutations for your cluster. Likely cause: performing heavy schema mutations in parallel, or extremely heavy Cassandra cluster load.");
        }
    }

    @Override
    public void compactInternally(String tableName) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(tableName), "tableName:[%s] should not be null or empty", tableName);
        String keyspace = config.keyspace();
        long compactionTimeoutSeconds = config.jmxCompactionTimeoutSeconds();

        try {
            alterGcAndTombstone(keyspace, tableName, 0, 0.0f);
            compactionManager.forceTableCompaction(compactionTimeoutSeconds, keyspace, tableName);
        } catch (TimeoutException e) {
            log.error("Compaction could not finish in {} seconds!", compactionTimeoutSeconds, e);
            log.error(compactionManager.getPendingCompactionStatus());
        } finally {
            alterGcAndTombstone(keyspace, tableName, CassandraConstants.GC_GRACE_SECONDS, CassandraConstants.TOMBSTONE_THRESHOLD_RATIO);
        }
    }

    private void alterGcAndTombstone(final String keyspace, final String tableName, final int gcGraceSeconds, final float tombstone_threshold_ratio) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(keyspace), "keyspace:[%s] should not be null or empty", keyspace);
        Preconditions.checkArgument(!Strings.isNullOrEmpty(tableName), "tableName:[%s] should not be null or empty", tableName);
        Preconditions.checkArgument(gcGraceSeconds >= 0, "gcGraceSeconds:[%s] should not be negative", gcGraceSeconds);
        Preconditions.checkArgument(tombstone_threshold_ratio >= 0.0f && tombstone_threshold_ratio <= 1.0f, "tombstone_threshold_ratio:[%s] should be between [0.0, 1.0]", tombstone_threshold_ratio);

        try {
            trySchemaMutationLock();
            clientPool.runWithPooledResource(new FunctionCheckedException<Client, Void, Exception>() {
                @Override
                public Void apply(Client client) throws NotFoundException, InvalidRequestException, TException {
                    KsDef ks = client.describe_keyspace(keyspace);
                    List<CfDef> cfs = ks.getCf_defs();
                    for (CfDef cf : cfs) {
                        if (cf.getName().equalsIgnoreCase(tableName)) {
                            cf.setGc_grace_seconds(gcGraceSeconds);
                            cf.setCompaction_strategy_options(ImmutableMap.of("tombstone_threshold", String.valueOf(tombstone_threshold_ratio)));
                            client.system_update_column_family(cf);
                            CassandraKeyValueServices.waitForSchemaVersions(client, tableName);
                            log.trace("gc_grace_seconds is set to {} for {}.{}", gcGraceSeconds, keyspace, tableName);
                            log.trace("tombstone_threshold is set to {} for {}.{}", tombstone_threshold_ratio, keyspace, tableName);
                        }
                    }
                    return null;
                }
            });
        } catch (Exception e) {
            log.error("Exception encountered while setting {}.{} gc_grace_seconds to {}", keyspace, tableName, gcGraceSeconds, e);
        } finally {
            schemaMutationLock.unlock();
        }
    }
}
