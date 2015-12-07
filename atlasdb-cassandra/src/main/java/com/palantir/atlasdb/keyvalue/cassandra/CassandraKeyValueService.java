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
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
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
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicates;
import com.google.common.base.Stopwatch;
import com.google.common.base.Strings;
import com.google.common.base.Supplier;
import com.google.common.base.Verify;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.ListMultimap;
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
import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfigManager;
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
import com.palantir.atlasdb.keyvalue.cassandra.jmx.CassandraJmxCompactionManager;
import com.palantir.atlasdb.keyvalue.cassandra.jmx.CassandraJmxCompactionModule;
import com.palantir.atlasdb.keyvalue.impl.AbstractKeyValueService;
import com.palantir.atlasdb.keyvalue.impl.Cells;
import com.palantir.atlasdb.keyvalue.impl.KeyValueServices;
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
 * each dispatch service can have one or many C* KVS.
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

    private static final Function<Entry<Cell, Value>, Long> ENTRY_SIZING_FUNCTION = new Function<Entry<Cell, Value>, Long>() {
        @Override
        public Long apply(Entry<Cell, Value> input) {
            return input.getValue().getContents().length + 4L + Cells.getApproxSizeOfCell(input.getKey());
        }
    };

    private static final int MAX_REQUEST_RETRIES = 10;
    private static final int REQUEST_TRIES_BEFORE_RANDOMIZING = 2;

    private final CassandraKeyValueServiceConfigManager configManager;
    private final CassandraClientPoolingManager cassandraClientPoolingManager;
    private final Optional<CassandraJmxCompactionManager> compactionManager;
    protected final ManyClientPoolingContainer containerPoolToUpdate;
    protected final ManyHostPoolingContainer<Client> clientPool;
    private final ScheduledExecutorService hostRefreshExecutor = PTExecutors.newScheduledThreadPool(1);
    private final ReentrantLock schemaMutationLock = new ReentrantLock(true);

    private ConsistencyLevel readConsistency = ConsistencyLevel.LOCAL_QUORUM;
    private final ConsistencyLevel writeConsistency = ConsistencyLevel.EACH_QUORUM;
    private final ConsistencyLevel deleteConsistency = ConsistencyLevel.ALL;

    private TokenAwareMapper tokenAwareMapper;

    public static CassandraKeyValueService create(CassandraKeyValueServiceConfigManager configManager) {
        Optional<CassandraJmxCompactionManager> compactionManager = new CassandraJmxCompactionModule().createCompactionManager(configManager);
        CassandraKeyValueService ret = new CassandraKeyValueService(configManager, compactionManager);
        ret.init();
        return ret;
    }

    protected CassandraKeyValueService(CassandraKeyValueServiceConfigManager configManager,
                                       Optional<CassandraJmxCompactionManager> compactionManager) {
        super(AbstractKeyValueService.createFixedThreadPool("Atlas Cassandra KVS",
                configManager.getConfig().poolSize() * configManager.getConfig().servers().size()));
        this.configManager = configManager;
        this.containerPoolToUpdate = ManyClientPoolingContainer.create(configManager.getConfig());
        this.clientPool = RetriableManyHostPoolingContainer.create(MAX_REQUEST_RETRIES, REQUEST_TRIES_BEFORE_RANDOMIZING, containerPoolToUpdate);
        this.cassandraClientPoolingManager =
                new PoolResizingCassandraClientPoolingManager(containerPoolToUpdate, clientPool, configManager);
        this.compactionManager = compactionManager;
    }

    protected void init() {
        int replicationFactor = configManager.getConfig().replicationFactor();
        initializeFromFreshInstance(containerPoolToUpdate.getCurrentHosts(), replicationFactor);
        poolingManager().submitHostRefreshTask();
    }

    public CassandraClientPoolingManager poolingManager() {
        return cassandraClientPoolingManager;
    }

    // Resizes the thread pool when hosts are updated, since the number of hosts may have changed
    private class PoolResizingCassandraClientPoolingManager extends CassandraClientPoolingManager {
        public PoolResizingCassandraClientPoolingManager(ManyClientPoolingContainer containerPoolToUpdate,
                                                         PoolingContainer<Client> clientPool,
                                                         CassandraKeyValueServiceConfigManager configManager) {
            super(containerPoolToUpdate, clientPool, configManager);
        }

        @Override
        public void setHostsToCurrentHostNames() throws TException {
            super.setHostsToCurrentHostNames();
            resizeThreadPool();
        }

        private void resizeThreadPool() {
            ThreadPoolExecutor threadPool = (ThreadPoolExecutor) executor;
            CassandraKeyValueServiceConfig config = configManager.getConfig();
            int threadPoolSize = config.poolSize() * config.servers().size();
            threadPool.setCorePoolSize(threadPoolSize);
            threadPool.setMaximumPoolSize(threadPoolSize);
        }
    }

    @Override
    public void initializeFromFreshInstance() {
        // we already did our init in our factory method
    }

    protected void initializeFromFreshInstance(List<String> addrList, int replicationFactor) {
        Map<String, Throwable> errorsByHost = Maps.newHashMap();

        final CassandraKeyValueServiceConfig config = configManager.getConfig();
        int port = config.port();
        boolean safetyDisabled = config.safetyDisabled();
        String keyspace = config.keyspace();
        boolean ssl = config.ssl();
        int socketTimeoutMillis = config.socketTimeoutMillis();
        int socketQueryTimeoutMillis = config.socketQueryTimeoutMillis();

        for (String addr : addrList) {
            Cassandra.Client client = null;
            try {
                client = CassandraClientFactory.getClientInternal(addr, port, ssl, socketTimeoutMillis, socketQueryTimeoutMillis);

                validatePartitioner(client);

                Set<String> currentHosts = cassandraClientPoolingManager.getCurrentHostsFromServer(client);
                cassandraClientPoolingManager.setHostsToCurrentHostNames(currentHosts);

                ensureKeyspaceExistsAndIsUpToDate(replicationFactor, safetyDisabled, keyspace, client);
                client.set_keyspace(keyspace);

                tokenAwareMapper = TokenAwareMapper.create(configManager, clientPool);
                createTableInternal(client, CassandraConstants.METADATA_TABLE);
                CassandraVerifier.sanityCheckRingConsistency(currentHosts, port, keyspace, ssl, safetyDisabled, socketTimeoutMillis, socketQueryTimeoutMillis);
                upgradeFromOlderInternalSchema(client);
                CassandraKeyValueServices.failQuickInInitializationIfClusterAlreadyInInconsistentState(client, config.safetyDisabled());
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

    private void validatePartitioner(Cassandra.Client client) throws TException {
        final CassandraKeyValueServiceConfig config = configManager.getConfig();
        String partitioner = client.describe_partitioner();
        if (!config.safetyDisabled()) {
            Verify.verify(
                    CassandraConstants.ALLOWED_PARTITIONERS.contains(partitioner),
                    "Invalid partitioner. Allowed: %s, but partitioner is: %s",
                    CassandraConstants.ALLOWED_PARTITIONERS,
                    partitioner);
        }
    }

    private void ensureKeyspaceExistsAndIsUpToDate(int replicationFactor,
                                                   boolean safetyDisabled,
                                                   String keyspace,
                                                   Cassandra.Client client) throws InvalidRequestException, TException, SchemaDisagreementException {
        try {
            KsDef ks = client.describe_keyspace(configManager.getConfig().keyspace());
            updateExistingKeyspace(replicationFactor, safetyDisabled, client, ks);
        } catch (NotFoundException e) {
            createKeyspace(replicationFactor, safetyDisabled, keyspace, client);
        }
    }

    private void updateExistingKeyspace(int replicationFactor,
                                        boolean safetyDisabled,
                                        Cassandra.Client client,
                                        KsDef ks) throws InvalidRequestException, SchemaDisagreementException, TException {
        CassandraVerifier.checkAndSetReplicationFactor(client, ks, false, replicationFactor, safetyDisabled);
        lowerConsistencyWhenSafe(client, ks, replicationFactor);
        // Can't call system_update_keyspace to update replication factor if CfDefs are set
        ks.setCf_defs(ImmutableList.<CfDef>of());
        client.system_update_keyspace(ks);
        CassandraKeyValueServices.waitForSchemaVersions(client, "(updating the existing keyspace)");
    }

    private void createKeyspace(int replicationFactor, boolean safetyDisabled, String keyspace, Cassandra.Client client)
            throws InvalidRequestException, SchemaDisagreementException, TException {
        KsDef ks = new KsDef(keyspace, CassandraConstants.NETWORK_STRATEGY, ImmutableList.<CfDef>of());
        CassandraVerifier.checkAndSetReplicationFactor(client, ks, true, replicationFactor, safetyDisabled);
        lowerConsistencyWhenSafe(client, ks, replicationFactor);
        ks.setDurable_writes(true);
        client.system_add_keyspace(ks);
        CassandraKeyValueServices.waitForSchemaVersions(client, "(adding the initial empty keyspace)");
    }

    private void upgradeFromOlderInternalSchema(Client client) throws NotFoundException, InvalidRequestException, TException {
        Map<String, byte[]> metadataForTables = getMetadataForTables();
        Map<String, byte[]> tablesToUpgrade = Maps.newHashMapWithExpectedSize(metadataForTables.size());
        String keyspace = configManager.getConfig().keyspace();

        for (CfDef clusterSideCf : client.describe_keyspace(keyspace).getCf_defs()) {
            String tableName = internalTableName(clusterSideCf.getName());
            if (metadataForTables.containsKey(tableName)) {
                byte[] clusterSideMetadata = metadataForTables.get(tableName);
                CfDef clientSideCf = getCfForTable(clusterSideCf.getName(), clusterSideMetadata);
                if (!CassandraKeyValueServices.isMatchingCf(clientSideCf, clusterSideCf)) { // mismatch; we have changed how we generate schema since we last persisted
                    log.warn("Upgrading table {} to new internal Cassandra schema", tableName);
                    tablesToUpgrade.put(tableName, clusterSideMetadata);
                }
            } else if (!tableName.equals(CassandraConstants.METADATA_TABLE)) { // only expected case
                // Possible to get here from a race condition with another dispatch starting up and performing schema upgrades concurrent with us doing this check
                log.error("Found a table " + tableName + " that did not have persisted Atlas metadata."
                        + "If you recently did a Palantir update, try waiting until schema upgrades are completed on all backend CLIs/dispatches etc and restarting this service."
                        + "If this error re-occurs on subsequent attempted startups, please contact Palantir support.");
            }
        }

        // we are racing another dispatch to do these same operations here, but they are idempotent / safe
        if (!tablesToUpgrade.isEmpty()) {
            putMetadataForTables(tablesToUpgrade);
        }
    }

    private void lowerConsistencyWhenSafe(Client client, KsDef ks, int desiredRf) {
        Set<String> dcs;
        try {
            dcs = CassandraVerifier.sanityCheckDatacenters(client, desiredRf, configManager.getConfig().safetyDisabled());
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

        Set<Entry<InetAddress, List<byte[]>>> rowsByHost =
                partitionByHost(rows, Functions.<byte[]>identity()).entrySet();
        List<Callable<Map<Cell, Value>>> tasks = Lists.newArrayListWithCapacity(rowsByHost.size());
        for (final Map.Entry<InetAddress, List<byte[]>> hostAndRows : rowsByHost) {
            tasks.add(new Callable<Map<Cell, Value>>() {
                @Override
                public Map<Cell, Value> call() {
                    return getRowsForSingleHost(hostAndRows.getKey(), tableName, hostAndRows.getValue(), startTs);
                }
            });
        }
        List<Map<Cell, Value>> perHostResults = runAllTasksCancelOnFailure(tasks);
        Map<Cell, Value> result = Maps.newHashMapWithExpectedSize(Iterables.size(rows));
        for (Map<Cell, Value> perHostResult : perHostResults) {
            result.putAll(perHostResult);
        }
        return result;
    }

    private Map<Cell, Value> getRowsForSingleHost(final InetAddress host,
                                                  final String tableName,
                                                  final List<byte[]> rows,
                                                  final long startTs) {
        try {
            int rowCount = 0;
            final Map<Cell, Value> result = Maps.newHashMap();
            int fetchBatchCount = configManager.getConfig().fetchBatchCount();
            for (final List<byte[]> batch : Lists.partition(rows, fetchBatchCount)) {
                rowCount += batch.size();
                result.putAll(clientPool.runWithPooledResourceOnHost(host, new FunctionCheckedException<Client, Map<Cell, Value>, Exception>() {
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
                        return "multiget_slice(" + tableName + ", " + batch.size() + " rows" + ")";
                    }
                }));
            }
            if (rowCount > fetchBatchCount) {
                log.warn("Rebatched in getRows a call to " + tableName + " that attempted to multiget "
                        + rowCount + " rows; this may indicate overly-large batching on a higher level.\n"
                        + CassandraKeyValueServices.getFilteredStackTrace("com.palantir"));
            }
            return ImmutableMap.copyOf(result);
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

    private void loadWithTs(String tableName,
                            Set<Cell> cells,
                            long startTs,
                            ThreadSafeResultVisitor v,
                            ConsistencyLevel consistency) throws Exception {
        List<Callable<Void>> tasks = Lists.newArrayList();
        for (Map.Entry<InetAddress, List<Cell>> hostAndCells : partitionByHost(cells,
                Cells.getRowFunction()).entrySet()) {
            tasks.addAll(getLoadWithTsTasksForSingleHost(hostAndCells.getKey(),
                    tableName,
                    hostAndCells.getValue(),
                    startTs,
                    v,
                    consistency));
        }
        runAllTasksCancelOnFailure(tasks);
    }

    // TODO: after cassandra api change: handle different column select per row
    private List<Callable<Void>> getLoadWithTsTasksForSingleHost(final InetAddress host,
                                                                 final String tableName,
                                                                 Collection<Cell> cells,
                                                                 final long startTs,
                                                                 final ThreadSafeResultVisitor v,
                                                                 final ConsistencyLevel consistency) throws Exception {
        final ColumnParent colFam = new ColumnParent(internalTableName(tableName));
        TreeMultimap<byte[], Cell> cellsByCol =
                TreeMultimap.create(UnsignedBytes.lexicographicalComparator(), Ordering.natural());
        for (Cell cell : cells) {
            cellsByCol.put(cell.getColumnName(), cell);
        }
        List<Callable<Void>> tasks = Lists.newArrayList();
        int fetchBatchCount = configManager.getConfig().fetchBatchCount();
        for (final byte[] col : cellsByCol.keySet()) {
            if (cellsByCol.get(col).size() > fetchBatchCount) {
                log.warn("Re-batching in getLoadWithTsTasksForSingleHost a call to {} for table {} that attempted to "
                                + "multiget {} rows; this may indicate overly-large batching on a higher level.\n{}",
                        host,
                        tableName,
                        cellsByCol.get(col).size(),
                        CassandraKeyValueServices.getFilteredStackTrace("com.palantir"));
            }
            for (final List<Cell> partition : Lists.partition(ImmutableList.copyOf(cellsByCol.get(col)), fetchBatchCount)) {
                tasks.add(new Callable<Void>() {
                    @Override
                    public Void call() throws Exception {
                        return clientPool.runWithPooledResourceOnHost(host, new FunctionCheckedException<Client, Void, Exception>() {
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
                                return "multiget_slice(" + host + ", " + colFam + ", "
                                        + partition.size() + " rows" + ")";
                            }
                        });
                    }
                });
            }
        }
        return tasks;
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
        return configManager.getConfig().mutationBatchCount();
    }

    private void putInternal(final String tableName,
                             final Iterable<Map.Entry<Cell, Value>> values) throws Exception {
        putInternal(tableName, values, CassandraConstants.NO_TTL);
    }

    protected void putInternal(final String tableName,
                               Iterable<Map.Entry<Cell, Value>> values,
                               final int ttl) throws Exception {
        Map<InetAddress, Map<Cell, Value>> cellsByHost = partitionMapByHost(values);
        List<Callable<Void>> tasks = Lists.newArrayListWithCapacity(cellsByHost.size());
        for (final Map.Entry<InetAddress, Map<Cell, Value>> entry : cellsByHost.entrySet()) {
            tasks.add(new Callable<Void>() {
                @Override
                public Void call() throws Exception {
                    putForSingleHostInternal(entry.getKey(), tableName, entry.getValue().entrySet(), ttl);
                    return null;
                }
            });
        }
        runAllTasksCancelOnFailure(tasks);
    }

    private void putForSingleHostInternal(final InetAddress host,
                                          final String tableName,
                                          final Iterable<Map.Entry<Cell, Value>> values,
                                          final int ttl) throws Exception {
        clientPool.runWithPooledResourceOnHost(host, new FunctionCheckedException<Client, Void, Exception>() {
            @Override
            public Void apply(Client client) throws Exception {
                final CassandraKeyValueServiceConfig config = configManager.getConfig();
                int mutationBatchCount = config.mutationBatchCount();
                int mutationBatchSizeBytes = config.mutationBatchSizeBytes();
                for (List<Entry<Cell, Value>> partition : partitionByCountAndBytes(values, mutationBatchCount,
                        mutationBatchSizeBytes, tableName, ENTRY_SIZING_FUNCTION)) {
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
                return "batch_mutate(" + host + ", " + tableName + ", " + Iterables.size(values) + " values, " + ttl + " ttl sec)";
            }
        });
    }

    // Overridden to batch more intelligently than the default implementation.
    @Override
    public void multiPut(Map<String, ? extends Map<Cell, byte[]>> valuesByTable, final long timestamp) throws KeyAlreadyExistsException {
        List<TableCellAndValue> flattened = Lists.newArrayList();
        for (Map.Entry<String, ? extends Map<Cell, byte[]>> tableAndValues : valuesByTable.entrySet()) {
            for (Map.Entry<Cell, byte[]> entry : tableAndValues.getValue().entrySet()) {
                flattened.add(new TableCellAndValue(tableAndValues.getKey(), entry.getKey(), entry.getValue()));
            }
        }
        Map<InetAddress, List<TableCellAndValue>> partitionedByHost =
                partitionByHost(flattened, TableCellAndValue.EXTRACT_ROW_NAME_FUNCTION);

        List<Callable<Void>> callables = Lists.newArrayList();
        for (Map.Entry<InetAddress, List<TableCellAndValue>> entry : partitionedByHost.entrySet()) {
            callables.addAll(getMultiPutTasksForSingleHost(entry.getKey(), entry.getValue(), timestamp));
        }
        runAllTasksCancelOnFailure(callables);
    }

    private List<Callable<Void>> getMultiPutTasksForSingleHost(final InetAddress host,
                                                               Collection<TableCellAndValue> values,
                                                               final long timestamp) {
        Iterable<List<TableCellAndValue>> partitioned =
                partitionByCountAndBytes(values,
                        getMultiPutBatchCount(),
                        getMultiPutBatchSizeBytes(),
                        extractTableNames(values).toString(),
                        TableCellAndValue.SIZING_FUNCTION);
        List<Callable<Void>> tasks = Lists.newArrayList();
        for (final List<TableCellAndValue> batch : partitioned) {
            final Set<String> tableNames = extractTableNames(batch);
            tasks.add(new Callable<Void>() {
                @Override
                public Void call() throws Exception {
                    String originalName = Thread.currentThread().getName();
                    Thread.currentThread()
                            .setName("Atlas multiPut of " + batch.size() + " cells into " + tableNames + " on " + host);
                    try {
                        multiPutForSingleHostInternal(host, tableNames, batch, timestamp);
                        return null;
                    } finally {
                        Thread.currentThread().setName(originalName);
                    }
                }
            });
        }
        return tasks;
    }

    private Set<String> extractTableNames(Iterable<TableCellAndValue> tableCellAndValues) {
        Set<String> tableNames = Sets.newHashSet();
        for (TableCellAndValue tableCellAndValue : tableCellAndValues) {
            tableNames.add(tableCellAndValue.tableName);
        }
        return tableNames;
    }

    private void multiPutForSingleHostInternal(final InetAddress host,
                                               final Set<String> tableNames,
                                               final List<TableCellAndValue> batch,
                                               long timestamp) throws Exception {
        final Map<ByteBuffer, Map<String, List<Mutation>>> map = convertToMutations(batch, timestamp);
        clientPool.runWithPooledResourceOnHost(host, new FunctionCheckedException<Client, Void, Exception>() {
            @Override
            public Void apply(Client client) throws Exception {
                batchMutateInternal(client, tableNames, map, writeConsistency);
                return null;
            }

            @Override
            public String toString() {
                return "batch_mutate(" + host + ", " + tableNames + ", " + batch.size() + " values)";
            }
        });
    }

    private Map<ByteBuffer, Map<String, List<Mutation>>> convertToMutations(List<TableCellAndValue> batch,
                                                                            long timestamp) {
        Map<ByteBuffer, Map<String, List<Mutation>>> map = Maps.newHashMap();
        for (TableCellAndValue tableCellAndValue : batch) {
            Cell cell = tableCellAndValue.cell;
            Column col = createColumn(cell, Value.create(tableCellAndValue.value, timestamp), CassandraConstants.NO_TTL);

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

            List<Mutation> tableMutations = rowPuts.get(tableCellAndValue.tableName);
            if (tableMutations == null) {
                tableMutations = Lists.<Mutation>newArrayList();
                rowPuts.put(tableCellAndValue.tableName, tableMutations);
            }

            tableMutations.add(m);
        }
        return map;
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
                                     Map<ByteBuffer, Map<String, List<Mutation>>> map,
                                     ConsistencyLevel consistency) throws TException {
        batchMutateInternal(client, ImmutableSet.of(tableName), map, consistency);
    }

    private void batchMutateInternal(Client client,
                                     Set<String> tableNames,
                                     Map<ByteBuffer, Map<String, List<Mutation>>> map,
                                     ConsistencyLevel consistency) throws TException {
        if (shouldTraceQuery(tableNames)) {
            ByteBuffer recv_trace = client.trace_next_query();
            Stopwatch stopwatch = Stopwatch.createStarted();
            client.batch_mutate(map, consistency);
            long duration = stopwatch.elapsed(TimeUnit.MILLISECONDS);
            if (duration > getMinimumDurationToTraceMillis()) {
                log.error("Traced a call to " + tableNames + " that took " + duration + " ms."
                        + " It will appear in system_traces with UUID="
                        + CassandraKeyValueServices.convertCassandraByteBufferUUIDtoString(recv_trace));
            }
        } else {
            client.batch_mutate(map, consistency);
        }
    }

    private boolean shouldTraceQuery(Set<String> tableNames) {
        for (String tableName : tableNames) {
            if (shouldTraceQuery(tableName)) {
                return true;
            }
        }
        return false;
    }

    private Map<ByteBuffer, List<ColumnOrSuperColumn>> multigetInternal(Client client,
                                                                        String tableName,
                                                                        List<ByteBuffer> rowNames,
                                                                        ColumnParent colFam,
                                                                        SlicePredicate pred,
                                                                        ConsistencyLevel consistency) throws TException {

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
        truncateTables(ImmutableSet.of(tableName));
    }

    @Override
    public void truncateTables(final Set<String> tableNames) {
        try {
            trySchemaMutationLock();
            clientPool.runWithPooledResource(new FunctionCheckedException<Client, Void, Exception>() {
                @Override
                public Void apply(Client client) throws Exception {
                    for (String tableName : tableNames) {
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
                    }
                    CassandraKeyValueServices.waitForSchemaVersions(client, "(" + tableNames.size() + " tables in a call to truncateTables)");
                    return null;
                }

                @Override
                public String toString() {
                    return "truncateTables(" + tableNames.size() + " tables)";
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
    public void delete(String tableName, Multimap<Cell, Long> keys) {
        Map<InetAddress, Map<Cell, Collection<Long>>> keysByHost = partitionMapByHost(keys.asMap().entrySet());
        for (Map.Entry<InetAddress, Map<Cell, Collection<Long>>> entry : keysByHost.entrySet()) {
            deleteOnSingleHost(entry.getKey(), tableName, entry.getValue());
        }
    }

    private void deleteOnSingleHost(final InetAddress host,
                                    final String tableName,
                                    final Map<Cell, Collection<Long>> keys) {
        try {
            clientPool.runWithPooledResourceOnHost(host, new FunctionCheckedException<Client, Void, Exception>() {
                @Override
                public Void apply(Client client) throws Exception {
                    // Delete must delete in the order of timestamp and we don't trust batch_mutate to do it
                    // atomically so we have to potentially do many deletes if there are many timestamps for the
                    // same key.
                    Map<Integer, Map<ByteBuffer, Map<String, List<Mutation>>>> maps = Maps.newTreeMap();
                    for (Cell key : keys.keySet()) {
                        int mapIndex = 0;
                        for (long ts : Ordering.natural().immutableSortedCopy(keys.get(key))) {
                            if (!maps.containsKey(mapIndex)) {
                                maps.put(mapIndex, Maps.<ByteBuffer, Map<String, List<Mutation>>>newHashMap());
                            }
                            Map<ByteBuffer, Map<String, List<Mutation>>> map = maps.get(mapIndex);
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
                    for (Map<ByteBuffer, Map<String, List<Mutation>>> map : maps.values()) {
                        // NOTE: we run with ConsistencyLevel.ALL here instead of ConsistencyLevel.QUORUM
                        // because we want to remove all copies of this data
                        batchMutateInternal(client, tableName, map, deleteConsistency);
                    }
                    return null;
                }

                @Override
                public String toString() {
                    return "batch_mutate(" + host + ", " + tableName + ", " + keys.size() + " keys" + ")";
                }
            });
        } catch (Exception e) {
            throw Throwables.throwUncheckedException(e);
        }
    }

    // update CKVS.isMatchingCf if you update this method
    private CfDef getCfForTable(String tableName, byte[] rawMetadata) {
        final CassandraKeyValueServiceConfig config = configManager.getConfig();
        Map<String, String> compressionOptions = Maps.newHashMap();
        CfDef cf = CassandraConstants.getStandardCfDef(config.keyspace(), internalTableName(tableName));

        boolean negativeLookups = false;
        double falsePositiveChance = CassandraConstants.DEFAULT_LEVELED_COMPACTION_BLOOM_FILTER_FP_CHANCE;
        int explicitCompressionBlockSizeKB = 0;
        boolean appendHeavyAndReadLight = false;

        if (!CassandraKeyValueServices.isEmptyOrInvalidMetadata(rawMetadata)) {
            TableMetadata tableMetadata = TableMetadata.BYTES_HYDRATOR.hydrateFromBytes(rawMetadata);
            negativeLookups = tableMetadata.hasNegativeLookups();
            explicitCompressionBlockSizeKB = tableMetadata.getExplicitCompressionBlockSizeKB();
            appendHeavyAndReadLight = tableMetadata.isAppendHeavyAndReadLight();
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

        if (appendHeavyAndReadLight) {
            cf.setCompaction_strategy(CassandraConstants.SIZE_TIERED_COMPACTION_STRATEGY);
            cf.setCompaction_strategy_optionsIsSet(false); // clear out the now nonsensical "keep it at 80MB per sstable" option from LCS
            if (!negativeLookups) {
                falsePositiveChance = CassandraConstants.DEFAULT_SIZE_TIERED_COMPACTION_BLOOM_FILTER_FP_CHANCE;
            } else {
                falsePositiveChance = CassandraConstants.NEGATIVE_LOOKUPS_SIZE_TIERED_BLOOM_FILTER_FP_CHANCE;
            }
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
        int concurrency = configManager.getConfig().rangesConcurrency();
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
                    protected TokenBackedBasicResultsPage<RowResult<U>, byte[]> getFirstPage() throws Exception {
                        return page(rangeRequest.getStartInclusive());
                    }

                    @Override
                    protected TokenBackedBasicResultsPage<RowResult<U>, byte[]> getNextPage(TokenBackedBasicResultsPage<RowResult<U>, byte[]> previous) throws Exception {
                        return page(previous.getTokenForNextPage());
                    }

                    TokenBackedBasicResultsPage<RowResult<U>, byte[]> page(final byte[] startKey) throws Exception {
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
                                            log.error("Traced a call to " + tableName + " that took " + duration + " ms."
                                                    + " It will appear in system_traces with UUID=" + CassandraKeyValueServices.convertCassandraByteBufferUUIDtoString(recv_trace));
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
                    String keyspace = configManager.getConfig().keyspace();
                    KsDef ks = client.describe_keyspace(keyspace);

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
                    KsDef ks = client.describe_keyspace(configManager.getConfig().keyspace());
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
    public void createTable(final String tableName, final byte[] tableMetadata) {
        createTables(ImmutableMap.of(tableName, tableMetadata));
    }

    // for tables internal / implementation specific to this KVS; these also don't get metadata in metadata table, nor do they show up in getTablenames
    private void createTableInternal(Client client, final String tableName) throws InvalidRequestException, SchemaDisagreementException, TException, NotFoundException {
        final CassandraKeyValueServiceConfig config = configManager.getConfig();
        KsDef ks = client.describe_keyspace(config.keyspace());
        for (CfDef cf : ks.getCf_defs()) {
            if (cf.getName().equalsIgnoreCase(internalTableName(tableName))) {
                return;
            }
        }
        CfDef cf = CassandraConstants.getStandardCfDef(config.keyspace(), internalTableName(tableName));
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
    public void createTables(final Map<String, byte[]> tableNamesToTableMetadata) {
        try {
            trySchemaMutationLock();
            clientPool.runWithPooledResource(new FunctionCheckedException<Client, Void, Exception>() {
                @Override
                public Void apply(Client client) throws Exception {
                    KsDef ks = client.describe_keyspace(configManager.getConfig().keyspace());
                    Set<String> tablesToCreate = tableNamesToTableMetadata.keySet();
                    Set<String> existingTables = Sets.newHashSet();

                    for (CfDef cf : ks.getCf_defs()) {
                        existingTables.add(cf.getName().toLowerCase());
                    }

                    for (String table : tablesToCreate) {
                        CassandraVerifier.sanityCheckTableName(table);

                        if (!existingTables.contains(internalTableName(table.toLowerCase()))) {
                            client.system_add_column_family(getCfForTable(table, tableNamesToTableMetadata.get(table)));
                        } else {
                            log.warn(String.format("Ignored call to create a table (%s) that already existed.", table));
                        }
                    }
                    if (!tablesToCreate.isEmpty()) {
                        CassandraKeyValueServices.waitForSchemaVersions(client, "(all tables in a call to createTables)");
                    }
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

        internalPutMetadataForTables(tableNamesToTableMetadata, false);
    }

    @Override
    public Set<String> getAllTableNames() {
        final CassandraKeyValueServiceConfig config = configManager.getConfig();
        try {
            return clientPool.runWithPooledResource(new FunctionCheckedException<Client, Set<String>, Exception>() {
                @Override
                public Set<String> apply(Client client) throws Exception {
                    KsDef ks = client.describe_keyspace(config.keyspace());

                    Set<String> ret = Sets.newHashSet();
                    for (CfDef cf : ks.getCf_defs()) {
                        if (!CassandraConstants.HIDDEN_TABLES.contains(cf.getName())) {
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
            return AtlasDbConstants.EMPTY_TABLE_METADATA;
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
                            contents = AtlasDbConstants.EMPTY_TABLE_METADATA;
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
        internalPutMetadataForTables(tableNameToMetadata, true);
    }

    private void internalPutMetadataForTables(final Map<String, byte[]> tableNameToMetadata, final boolean possiblyNeedToPerformSettingsChanges) {
        final Map<Cell, byte[]> metadataRequestedForUpdate = Maps.newHashMapWithExpectedSize(tableNameToMetadata.size());
        for (Entry<String, byte[]> tableEntry : tableNameToMetadata.entrySet()) {
            metadataRequestedForUpdate.put(getMetadataCell(tableEntry.getKey()), tableEntry.getValue());
        }

        Map<Cell, Long> requestForLatestDbSideMetadata = Maps.transformValues(metadataRequestedForUpdate, Functions.constant(Long.MAX_VALUE));

        // technically we're racing other dispatches from here on, during an update period,
        // but the penalty for not caring is just some superfluous schema mutations and a few dead rows in the metadata table.
        Map<Cell, Value> persistedMetadata = get(CassandraConstants.METADATA_TABLE, requestForLatestDbSideMetadata);
        final Map<Cell, byte[]> newMetadata = Maps.newHashMap();
        final Collection<CfDef> updatedCfs = Lists.newArrayList();
        for (Entry<Cell, byte[]> entry : metadataRequestedForUpdate.entrySet()) {
            Value val = persistedMetadata.get(entry.getKey());
            if (val == null || !Arrays.equals(val.getContents(), entry.getValue())) {
                newMetadata.put(entry.getKey(), entry.getValue());
                updatedCfs.add(getCfForTable(new String(entry.getKey().getRowName()), entry.getValue()));
            }
        }

        if (!newMetadata.isEmpty()) {
            try {
                if (possiblyNeedToPerformSettingsChanges) {
                    trySchemaMutationLock();
                }
                clientPool.runWithPooledResource(new FunctionCheckedException<Client, Void, Exception>() {
                    @Override
                    public Void apply(Client client) throws Exception {
                        if (possiblyNeedToPerformSettingsChanges) {
                            for (CfDef cf : updatedCfs) {
                                client.system_update_column_family(cf);
                            }

                            CassandraKeyValueServices.waitForSchemaVersions(client, "(all tables in a call to putMetadataForTables)");
                        }
                        // Done with actual schema mutation, push the metadata
                        put(CassandraConstants.METADATA_TABLE, newMetadata, 0L);
                        return null;
                    }
                });
            } catch (Exception e) {
                throw Throwables.throwUncheckedException(e);
            } finally {
                if (possiblyNeedToPerformSettingsChanges) {
                    schemaMutationLock.unlock();
                }
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
        if (compactionManager.isPresent()) {
            compactionManager.get().close();
        }
        tokenAwareMapper.shutdown();
        super.close();
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
        try {
            clientPool.runWithPooledResource(new FunctionCheckedException<Client, Void, Exception>() {
                @Override
                public Void apply(Client client) throws Exception {
                    for (Map.Entry<Cell, byte[]> e : values.entrySet()) {
                        ByteBuffer rowName = ByteBuffer.wrap(e.getKey().getRowName());
                        byte[] contents = e.getValue();
                        long timestamp = AtlasDbConstants.TRANSACTION_TS;
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

    private static String internalTableName(String tableName) {
        if (tableName.startsWith("_")) {
            return tableName;
        }
        return tableName.replaceFirst("\\.", "__");
    }

    private String fromInternalTableName(String tableName) {
        if (tableName.startsWith("_")) {
            return tableName;
        }
        return tableName.replaceFirst("__", ".");
    }

    private void trySchemaMutationLock() throws InterruptedException, TimeoutException {
        if (!schemaMutationLock.tryLock(CassandraConstants.SECONDS_TO_WAIT_FOR_SCHEMA_MUTATION_LOCK, TimeUnit.SECONDS)) {
            throw new TimeoutException("AtlasDB was unable to get a lock on Cassandra system schema mutations for your cluster. Likely cause: Dispatch(es) performing heavy schema mutations in parallel, or extremely heavy Cassandra cluster load.");
        }
    }

    @Override
    public void compactInternally(String tableName) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(tableName), "tableName:[%s] should not be null or empty.", tableName);
        CassandraKeyValueServiceConfig config = configManager.getConfig();
        if (!compactionManager.isPresent()) {
            log.warn("No compaction client was configured, but compact was called. If you actually want to clear deleted data immediately " +
                    "from Cassandra, lower your gc_grace_seconds setting and run `nodetool compact {} {}`.", config.keyspace(), tableName);
            return;
        }
        long timeoutInSeconds = config.compactionTimeoutSeconds();
        String keyspace = config.keyspace();
        try {
            alterGcAndTombstone(keyspace, tableName, 0, 0.0f);
            compactionManager.get().performTombstoneCompaction(timeoutInSeconds, keyspace, tableName);
        } catch (TimeoutException e) {
            log.error("Compaction for {}.{} could not finish in {} seconds.", keyspace, tableName, timeoutInSeconds, e);
            log.error(compactionManager.get().getCompactionStatus());
        } catch (InterruptedException e) {
            log.error("Compaction for {}.{} was interupted.", keyspace, tableName);
        } finally {
            alterGcAndTombstone(keyspace, tableName, CassandraConstants.GC_GRACE_SECONDS, CassandraConstants.TOMBSTONE_THRESHOLD_RATIO);
        }
    }

    private void alterGcAndTombstone(final String keyspace, final String tableName, final int gcGraceSeconds, final float tombstoneThresholdRatio) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(keyspace), "keyspace:[%s] should not be null or empty.", keyspace);
        Preconditions.checkArgument(!Strings.isNullOrEmpty(tableName), "tableName:[%s] should not be null or empty.", tableName);
        Preconditions.checkArgument(gcGraceSeconds >= 0, "gc_grace_seconds:[%s] should not be negative.", gcGraceSeconds);
        Preconditions.checkArgument(tombstoneThresholdRatio >= 0.0f && tombstoneThresholdRatio <= 1.0f,
                "tombstone_threshold_ratio:[%s] should be between [0.0, 1.0]", tombstoneThresholdRatio);

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
                            cf.setCompaction_strategy_options(ImmutableMap.of("tombstone_threshold", String.valueOf(tombstoneThresholdRatio)));
                            client.system_update_column_family(cf);
                            CassandraKeyValueServices.waitForSchemaVersions(client, tableName);
                            log.trace("gc_grace_seconds is set to {} for {}.{}", gcGraceSeconds, keyspace, tableName);
                            log.trace("tombstone_threshold_ratio is set to {} for {}.{}", tombstoneThresholdRatio, keyspace, tableName);
                        }
                    }
                    return null;
                }
            });
        } catch (Exception e) {
            log.error("Exception encountered while setting gc_grace_seconds:{} and tombstone_threshold:{} for {}.{}",
                    gcGraceSeconds,
                    tombstoneThresholdRatio,
                    keyspace,
                    tableName,
                    e);
        } finally {
            schemaMutationLock.unlock();
        }
    }

    private <V> Map<InetAddress, Map<Cell, V>> partitionMapByHost(Iterable<Map.Entry<Cell, V>> cells) {
        Map<InetAddress, Map<Cell, V>> cellsByHost = Maps.newHashMap();
        for (Map.Entry<Cell, V> entry : cells) {
            InetAddress host = tokenAwareMapper.getRandomHostForKey(entry.getKey().getRowName());
            if (!cellsByHost.containsKey(host)) {
                cellsByHost.put(host, Maps.<Cell, V>newHashMap());
            }
            cellsByHost.get(host).put(entry.getKey(), entry.getValue());
        }
        return cellsByHost;
    }

    private <V> Map<InetAddress, List<V>> partitionByHost(Iterable<V> iterable, Function<V, byte[]> keyExtractor) {
        ListMultimap<InetAddress, V> valuesByHost = ArrayListMultimap.create();
        for (V value : iterable) {
            InetAddress host = tokenAwareMapper.getRandomHostForKey(keyExtractor.apply(value));
            valuesByHost.put(host, value);
        }
        return Multimaps.asMap(valuesByHost);
    }

    /*
     * Similar to executor.invokeAll, but cancels all remaining tasks if one fails and doesn't spawn new threads if
     * there is only one task
     */
    private <V> List<V> runAllTasksCancelOnFailure(List<Callable<V>> tasks) {
        if (tasks.size() == 1) {
            try {
                //Callable<Void> returns null, so can't use immutable list
                return Collections.singletonList(tasks.get(0).call());
            } catch (Exception e) {
                throw Throwables.throwUncheckedException(e);
            }
        }

        List<Future<V>> futures = Lists.newArrayListWithCapacity(tasks.size());
        for (Callable<V> task : tasks) {
            futures.add(executor.submit(task));
        }
        try {
            List<V> results = Lists.newArrayListWithCapacity(tasks.size());
            for (Future<V> future : futures) {
                results.add(future.get());
            }
            return results;
        } catch (Exception e) {
            throw Throwables.throwUncheckedException(e);
        } finally {
            for (Future<V> future : futures) {
                future.cancel(true);
            }
        }
    }

    private static class TableCellAndValue {
        private final String tableName;
        private final Cell cell;
        private final byte[] value;

        public static Function<TableCellAndValue, byte[]> EXTRACT_ROW_NAME_FUNCTION =
                new Function<TableCellAndValue, byte[]>() {
                    @Override
                    public byte[] apply(TableCellAndValue input) {
                        return input.cell.getRowName();
                    }
                };

        public static Function<TableCellAndValue, Long> SIZING_FUNCTION =
                new Function<CassandraKeyValueService.TableCellAndValue, Long>() {
                    @Override
                    public Long apply(TableCellAndValue input) {
                        return input.value.length + Cells.getApproxSizeOfCell(input.cell);
                    }
                };

        public TableCellAndValue(String tableName, Cell cell, byte[] value) {
            this.tableName = tableName;
            this.cell = cell;
            this.value = value;
        }
    }
}
