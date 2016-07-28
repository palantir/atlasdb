/**
 * Copyright 2015 Palantir Technologies
 * <p>
 * Licensed under the BSD-3 License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://opensource.org/licenses/BSD-3-Clause
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.palantir.atlasdb.keyvalue.cassandra;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.SortedSet;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import org.apache.cassandra.thrift.CASResult;
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
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
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
import com.palantir.atlasdb.config.LeaderConfig;
import com.palantir.atlasdb.config.LockLeader;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.ColumnRangeSelection;
import com.palantir.atlasdb.keyvalue.api.ColumnSelection;
import com.palantir.atlasdb.keyvalue.api.InsufficientConsistencyException;
import com.palantir.atlasdb.keyvalue.api.KeyAlreadyExistsException;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.RangeRequests;
import com.palantir.atlasdb.keyvalue.api.RowColumnRangeIterator;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.keyvalue.cassandra.CassandraKeyValueServices.AllTimestampsCollector;
import com.palantir.atlasdb.keyvalue.cassandra.CassandraKeyValueServices.StartTsResultsCollector;
import com.palantir.atlasdb.keyvalue.cassandra.CassandraKeyValueServices.ThreadSafeResultVisitor;
import com.palantir.atlasdb.keyvalue.cassandra.jmx.CassandraJmxCompaction;
import com.palantir.atlasdb.keyvalue.cassandra.jmx.CassandraJmxCompactionManager;
import com.palantir.atlasdb.keyvalue.impl.AbstractKeyValueService;
import com.palantir.atlasdb.keyvalue.impl.Cells;
import com.palantir.atlasdb.keyvalue.impl.KeyValueServices;
import com.palantir.atlasdb.keyvalue.impl.LocalRowColumnRangeIterator;
import com.palantir.atlasdb.protos.generated.TableMetadataPersistence;
import com.palantir.atlasdb.table.description.TableMetadata;
import com.palantir.common.annotation.Idempotent;
import com.palantir.common.base.ClosableIterator;
import com.palantir.common.base.ClosableIterators;
import com.palantir.common.base.FunctionCheckedException;
import com.palantir.common.base.Throwables;
import com.palantir.common.concurrent.ThreadNamingCallable;
import com.palantir.common.exception.PalantirRuntimeException;
import com.palantir.util.paging.AbstractPagingIterable;
import com.palantir.util.paging.SimpleTokenBackedResultsPage;
import com.palantir.util.paging.TokenBackedBasicResultsPage;

/**
 *
 * each service can have one or many C* KVS.
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

    private final Logger log;

    private static final Function<Entry<Cell, Value>, Long> ENTRY_SIZING_FUNCTION = new Function<Entry<Cell, Value>, Long>() {
        @Override
        public Long apply(Entry<Cell, Value> input) {
            return input.getValue().getContents().length + 4L + Cells.getApproxSizeOfCell(input.getKey());
        }
    };

    private final CassandraKeyValueServiceConfigManager configManager;
    private final Optional<CassandraJmxCompactionManager> compactionManager;
    protected final CassandraClientPool clientPool;
    private SchemaMutationLock schemaMutationLock;
    private final Optional<LeaderConfig> leaderConfig;
    private final HiddenTables hiddenTables;

    private final UniqueSchemaMutationLockTable schemaMutationLockTable;

    private ConsistencyLevel readConsistency = ConsistencyLevel.LOCAL_QUORUM;
    private final ConsistencyLevel writeConsistency = ConsistencyLevel.EACH_QUORUM;
    private final ConsistencyLevel deleteConsistency = ConsistencyLevel.ALL;

    public static CassandraKeyValueService create(CassandraKeyValueServiceConfigManager configManager, Optional<LeaderConfig> leaderConfig) {
        return create(configManager, leaderConfig, LoggerFactory.getLogger(CassandraKeyValueService.class));
    }

    public static CassandraKeyValueService create(CassandraKeyValueServiceConfigManager configManager, Optional<LeaderConfig> leaderConfig, Logger log) {
        Optional<CassandraJmxCompactionManager> compactionManager = CassandraJmxCompaction.createJmxCompactionManager(configManager);
        CassandraKeyValueService ret = new CassandraKeyValueService(log, configManager, compactionManager, leaderConfig);
        ret.init();
        return ret;
    }

    protected CassandraKeyValueService(Logger log,
                                       CassandraKeyValueServiceConfigManager configManager,
                                       Optional<CassandraJmxCompactionManager> compactionManager,
                                       Optional<LeaderConfig> leaderConfig) {
        super(AbstractKeyValueService.createFixedThreadPool("Atlas Cassandra KVS",
                configManager.getConfig().poolSize() * configManager.getConfig().servers().size()));
        this.log = log;
        this.configManager = configManager;
        this.clientPool = new CassandraClientPool(configManager.getConfig());
        this.compactionManager = compactionManager;
        this.leaderConfig = leaderConfig;
        this.hiddenTables = new HiddenTables();

        SchemaMutationLockTables lockTables = new SchemaMutationLockTables(clientPool, configManager.getConfig());
        this.schemaMutationLockTable = new UniqueSchemaMutationLockTable(lockTables, whoIsTheLockCreator());
    }

    private LockLeader whoIsTheLockCreator() {
        return leaderConfig
                .transform((config) -> config.whoIsTheLockLeader())
                .or(LockLeader.I_AM_THE_LOCK_LEADER);
    }

    protected void init() {
        clientPool.runOneTimeStartupChecks();

        boolean supportsCAS = clientPool.runWithRetry(CassandraVerifier.underlyingCassandraClusterSupportsCASOperations);

        schemaMutationLock = new SchemaMutationLock(supportsCAS, configManager, clientPool, writeConsistency, schemaMutationLockTable);

        createTable(AtlasDbConstants.METADATA_TABLE, AtlasDbConstants.EMPTY_TABLE_METADATA);
        lowerConsistencyWhenSafe();
        upgradeFromOlderInternalSchema();
        CassandraKeyValueServices.failQuickInInitializationIfClusterAlreadyInInconsistentState(clientPool, configManager.getConfig());
    }

    @Override
    public void initializeFromFreshInstance() {
        // we already did our init in our factory method
    }

    private void upgradeFromOlderInternalSchema() {
        try {
            Map<TableReference, byte[]> metadataForTables = getMetadataForTables();
            Map<TableReference, byte[]> tablesToUpgrade = Maps.newHashMapWithExpectedSize(metadataForTables.size());

            List<CfDef> knownCfs = clientPool.runWithRetry(client ->
                    client.describe_keyspace(configManager.getConfig().keyspace()).getCf_defs());

            for (CfDef clusterSideCf : knownCfs) {
                TableReference tableRef = fromInternalTableName(clusterSideCf.getName());
                if (metadataForTables.containsKey(tableRef)) {
                    byte[] clusterSideMetadata = metadataForTables.get(tableRef);
                    CfDef clientSideCf = getCfForTable(fromInternalTableName(clusterSideCf.getName()), clusterSideMetadata);
                    if (!CassandraKeyValueServices.isMatchingCf(clientSideCf, clusterSideCf)) { // mismatch; we have changed how we generate schema since we last persisted
                        log.warn("Upgrading table {} to new internal Cassandra schema", tableRef);
                        tablesToUpgrade.put(tableRef, clusterSideMetadata);
                    }
                } else if (!hiddenTables.isHidden(tableRef)) {
                    // Possible to get here from a race condition with another service starting up and performing schema upgrades concurrent with us doing this check
                    log.error("Found a table " + tableRef.getQualifiedName() + " that did not have persisted Atlas metadata. "
                            + "If you recently did a Palantir update, try waiting until schema upgrades are completed on all backend CLIs/services etc and restarting this service. "
                            + "If this error re-occurs on subsequent attempted startups, please contact Palantir support.");
                }
            }

            // we are racing another service to do these same operations here, but they are idempotent / safe
            if (!tablesToUpgrade.isEmpty()) {
                putMetadataForTables(tablesToUpgrade);
            }
        } catch (TException e) {
            log.error("Couldn't upgrade from an older internal Cassandra schema. New table-related settings may not have taken effect.");
        }
    }

    private void lowerConsistencyWhenSafe() {
        Set<String> dcs;
        Map<String, String> strategyOptions;
        CassandraKeyValueServiceConfig config = configManager.getConfig();

        try {
            dcs = clientPool.runWithRetry(client ->
                    CassandraVerifier.sanityCheckDatacenters(client, config.replicationFactor(), config.safetyDisabled()));
            KsDef ksDef = clientPool.runWithRetry(client ->
                    client.describe_keyspace(config.keyspace()));
            strategyOptions = Maps.newHashMap(ksDef.getStrategy_options());

            if (dcs.size() == 1) {
                String dc = dcs.iterator().next();
                if (strategyOptions.get(dc) != null) {
                    int currentRF = Integer.parseInt(strategyOptions.get(dc));
                    if (currentRF == config.replicationFactor()) {
                        if (currentRF == 2 && config.clusterMeetsNormalConsistencyGuarantees()) {
                            log.info("Setting Read Consistency to ONE, as cluster has only one datacenter at RF2.");
                            readConsistency = ConsistencyLevel.ONE;
                        }
                    }
                }
            }
        } catch (InvalidRequestException e) {
            return;
        } catch (TException e) {
            return;
        }
    }

    @Override
    public Map<Cell, Value> getRows(final TableReference tableRef, final Iterable<byte[]> rows, ColumnSelection selection, final long startTs) {
        if (!selection.allColumnsSelected()) {
            return getRowsForSpecificColumns(tableRef, rows, selection, startTs);
        }

        Set<Entry<InetSocketAddress, List<byte[]>>> rowsByHost =
                partitionByHost(rows, Functions.<byte[]>identity()).entrySet();
        List<Callable<Map<Cell, Value>>> tasks = Lists.newArrayListWithCapacity(rowsByHost.size());
        for (final Map.Entry<InetSocketAddress, List<byte[]>> hostAndRows : rowsByHost) {
            tasks.add(ThreadNamingCallable.wrapWithThreadName(
                    () -> CassandraKeyValueService.this.getRowsForSingleHost(hostAndRows.getKey(), tableRef, hostAndRows.getValue(), startTs),
                    "Atlas getRows " + hostAndRows.getValue().size() + " rows from " + tableRef + " on " + hostAndRows.getKey(),
                    ThreadNamingCallable.Type.PREPEND));
        }
        List<Map<Cell, Value>> perHostResults = runAllTasksCancelOnFailure(tasks);
        Map<Cell, Value> result = Maps.newHashMapWithExpectedSize(Iterables.size(rows));
        for (Map<Cell, Value> perHostResult : perHostResults) {
            result.putAll(perHostResult);
        }
        return result;
    }

    private Map<Cell, Value> getRowsForSingleHost(final InetSocketAddress host,
                                                  final TableReference tableRef,
                                                  final List<byte[]> rows,
                                                  final long startTs) {
        try {
            int rowCount = 0;
            final Map<Cell, Value> result = Maps.newHashMap();
            int fetchBatchCount = configManager.getConfig().fetchBatchCount();
            for (final List<byte[]> batch : Lists.partition(rows, fetchBatchCount)) {
                rowCount += batch.size();
                result.putAll(clientPool.runWithRetryOnHost(host, new FunctionCheckedException<Client, Map<Cell, Value>, Exception>() {
                    @Override
                    public Map<Cell, Value> apply(Client client) throws Exception {
                        // We want to get all the columns in the row so set start and end to empty.
                        SliceRange slice = new SliceRange(ByteBuffer.wrap(PtBytes.EMPTY_BYTE_ARRAY), ByteBuffer.wrap(PtBytes.EMPTY_BYTE_ARRAY), false, Integer.MAX_VALUE);
                        SlicePredicate pred = new SlicePredicate();
                        pred.setSlice_range(slice);

                        List<ByteBuffer> rowNames = wrap(rows);

                        Map<ByteBuffer, List<ColumnOrSuperColumn>> results = multigetInternal(client, tableRef, rowNames, pred, readConsistency);
                        Map<Cell, Value> ret = Maps.newHashMap();
                        new ValueExtractor(ret).extractResults(results, startTs, ColumnSelection.all());
                        return ret;
                    }

                    @Override
                    public String toString() {
                        return "multiget_slice(" + tableRef.getQualifiedName() + ", " + batch.size() + " rows" + ")";
                    }
                }));
            }
            if (rowCount > fetchBatchCount) {
                log.warn("Rebatched in getRows a call to " + tableRef.getQualifiedName() + " that attempted to multiget "
                        + rowCount + " rows; this may indicate overly-large batching on a higher level.\n"
                        + CassandraKeyValueServices.getFilteredStackTrace("com.palantir"));
            }
            return ImmutableMap.copyOf(result);
        } catch (Exception e) {
            throw Throwables.throwUncheckedException(e);
        }
    }

    private List<ByteBuffer> wrap(List<byte[]> arrays) {
        List<ByteBuffer> byteBuffers = Lists.newArrayListWithCapacity(arrays.size());
        for (byte[] r : arrays) {
            byteBuffers.add(ByteBuffer.wrap(r));
        }
        return byteBuffers;
    }

    private Map<Cell, Value> getRowsForSpecificColumns(final TableReference tableRef,
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
            loadWithTs(tableRef, cells, startTs, false, collector, readConsistency);
            return collector.collectedResults;
        } catch (Exception e) {
            throw Throwables.throwUncheckedException(e);
        }
    }

    @Override
    public Map<Cell, Value> get(TableReference tableRef, Map<Cell, Long> timestampByCell) {
        if (timestampByCell.isEmpty()) {
            log.info("Attempted get on '{}' table with empty cells", tableRef);
            return ImmutableMap.of();
        }

        try {
            Long firstTs = timestampByCell.values().iterator().next();
            if (Iterables.all(timestampByCell.values(), Predicates.equalTo(firstTs))) {
                StartTsResultsCollector collector = new StartTsResultsCollector(firstTs);
                loadWithTs(tableRef, timestampByCell.keySet(), firstTs, false, collector, readConsistency);
                return collector.collectedResults;
            }

            SetMultimap<Long, Cell> cellsByTs = Multimaps.invertFrom(
                    Multimaps.forMap(timestampByCell), HashMultimap.<Long, Cell>create());
            Builder<Cell, Value> builder = ImmutableMap.builder();
            for (long ts : cellsByTs.keySet()) {
                StartTsResultsCollector collector = new StartTsResultsCollector(ts);
                loadWithTs(tableRef, cellsByTs.get(ts), ts, false, collector, readConsistency);
                builder.putAll(collector.collectedResults);
            }
            return builder.build();
        } catch (Exception e) {
            throw Throwables.throwUncheckedException(e);
        }
    }

    private void loadWithTs(TableReference tableRef,
                            Set<Cell> cells,
                            long startTs,
                            boolean loadAllTs,
                            ThreadSafeResultVisitor v,
                            ConsistencyLevel consistency) throws Exception {
        Map<InetSocketAddress, List<Cell>> hostsAndCells =  partitionByHost(cells, Cells.getRowFunction());
        int totalPartitions = hostsAndCells.keySet().size();

        if (log.isTraceEnabled()) {
            log.trace("Loading {} cells from {} {}starting at timestamp {}, partitioned across {} nodes.",
                    cells.size(), tableRef, loadAllTs ? "for all timestamps " : "", startTs, totalPartitions);
        }

        List<Callable<Void>> tasks = Lists.newArrayList();
        for (Map.Entry<InetSocketAddress, List<Cell>> hostAndCells : hostsAndCells.entrySet()) {
            if (log.isTraceEnabled()) {
                log.trace("Requesting {} cells from {} {}starting at timestamp {} on {}",
                        hostsAndCells.values().size(), tableRef, loadAllTs ? "for all timestamps " : "", startTs, hostAndCells.getKey());
            }

            tasks.addAll(getLoadWithTsTasksForSingleHost(hostAndCells.getKey(),
                    tableRef,
                    hostAndCells.getValue(),
                    startTs,
                    loadAllTs,
                    v,
                    consistency));
        }
        runAllTasksCancelOnFailure(tasks);
    }

    // TODO: after cassandra api change: handle different column select per row
    private List<Callable<Void>> getLoadWithTsTasksForSingleHost(final InetSocketAddress host,
                                                                 final TableReference tableRef,
                                                                 final Collection<Cell> cells,
                                                                 final long startTs,
                                                                 final boolean loadAllTs,
                                                                 final ThreadSafeResultVisitor v,
                                                                 final ConsistencyLevel consistency) throws Exception {
        final ColumnParent colFam = new ColumnParent(internalTableName(tableRef));
        TreeMultimap<byte[], Cell> cellsByCol =
                TreeMultimap.create(UnsignedBytes.lexicographicalComparator(), Ordering.natural());
        for (Cell cell : cells) {
            cellsByCol.put(cell.getColumnName(), cell);
        }
        List<Callable<Void>> tasks = Lists.newArrayList();
        int fetchBatchCount = configManager.getConfig().fetchBatchCount();
        for (Entry<byte[], SortedSet<Cell>> entry : Multimaps.asMap(cellsByCol).entrySet()) {
            final byte[] col = entry.getKey();
            SortedSet<Cell> columnCells = entry.getValue();
            if (columnCells.size() > fetchBatchCount) {
                log.warn("Re-batching in getLoadWithTsTasksForSingleHost a call to {} for table {} that attempted to "
                                + "multiget {} rows; this may indicate overly-large batching on a higher level.\n{}",
                        host,
                        tableRef,
                        columnCells.size(),
                        CassandraKeyValueServices.getFilteredStackTrace("com.palantir"));
            }
            for (final List<Cell> partition : Lists.partition(ImmutableList.copyOf(columnCells), fetchBatchCount)) {
                Callable<Void> multiGetCallable = () -> clientPool.runWithRetryOnHost(host, new FunctionCheckedException<Client, Void, Exception>() {
                    @Override
                    public Void apply(Client client) throws Exception {
                        ByteBuffer start = CassandraKeyValueServices.makeCompositeBuffer(col, startTs - 1);
                        ByteBuffer end = CassandraKeyValueServices.makeCompositeBuffer(col, -1);
                        SliceRange slice = new SliceRange(start, end, false, loadAllTs ? Integer.MAX_VALUE : 1);
                        SlicePredicate predicate = new SlicePredicate();
                        predicate.setSlice_range(slice);

                        List<ByteBuffer> rowNames = Lists.newArrayListWithCapacity(partition.size());
                        for (Cell c : partition) {
                            rowNames.add(ByteBuffer.wrap(c.getRowName()));
                        }

                        if (log.isTraceEnabled()) {
                            log.trace("Requesting {} cells from {} {}starting at timestamp {} on {}",
                                    partition.size(), tableRef, loadAllTs ? "for all timestamps " : "", startTs, host);
                        }

                        Map<ByteBuffer, List<ColumnOrSuperColumn>> results = multigetInternal(client, tableRef, rowNames, predicate, consistency);
                        v.visit(results);
                        return null;
                    }

                    @Override
                    public String toString() {
                        return "multiget_slice(" + host + ", " + colFam + ", " + partition.size() + " cells" + ")";
                    }

                });
                tasks.add(ThreadNamingCallable.wrapWithThreadName(
                        multiGetCallable,
                        "Atlas loadWithTs " + partition.size() + " cells from " + tableRef + " on " + host,
                        ThreadNamingCallable.Type.PREPEND));
            }
        }
        return tasks;
    }

    @Override
    public Map<byte[], RowColumnRangeIterator> getRowsColumnRange(TableReference tableRef,
                                                                  Iterable<byte[]> rows,
                                                                  ColumnRangeSelection columnRangeSelection,
                                                                  long timestamp) {
        Set<Entry<InetSocketAddress, List<byte[]>>> rowsByHost =
                partitionByHost(rows, Functions.<byte[]>identity()).entrySet();
        List<Callable<Map<byte[], RowColumnRangeIterator>>> tasks = Lists.newArrayListWithCapacity(rowsByHost.size());
        for (final Map.Entry<InetSocketAddress, List<byte[]>> hostAndRows : rowsByHost) {
            tasks.add(ThreadNamingCallable.wrapWithThreadName(() ->
                    getRowsColumnRangeIteratorForSingleHost(hostAndRows.getKey(), tableRef,
                            hostAndRows.getValue(), columnRangeSelection, timestamp),
                    "Atlas getRowsColumnRange " + hostAndRows.getValue().size() + " rows from " + tableRef + " on " + hostAndRows.getKey(),
                    ThreadNamingCallable.Type.PREPEND));
        }
        List<Map<byte[], RowColumnRangeIterator>> perHostResults = runAllTasksCancelOnFailure(tasks);
        Map<byte[], RowColumnRangeIterator> result = Maps.newHashMapWithExpectedSize(Iterables.size(rows));
        for (Map<byte[], RowColumnRangeIterator> perHostResult : perHostResults) {
            result.putAll(perHostResult);
        }
        return result;
    }

    private Map<byte[], RowColumnRangeIterator> getRowsColumnRangeIteratorForSingleHost(InetSocketAddress host,
                                                                                        TableReference tableRef,
                                                                                        List<byte[]> rows,
                                                                                        ColumnRangeSelection columnRangeSelection,
                                                                                        long startTs) {
        try {
            RowColumnRangeExtractor.RowColumnRangeResult firstPage =
                    getRowsColumnRangeForSingleHost(host, tableRef, rows, columnRangeSelection, startTs);

            Map<byte[], LinkedHashMap<Cell, Value>> results = firstPage.getResults();
            Map<byte[], Column> rowsToLastCompositeColumns = firstPage.getRowsToLastCompositeColumns();
            Map<byte[], byte[]> incompleteRowsToNextColumns = Maps.newHashMap();
            for (Entry<byte[], Column> e : rowsToLastCompositeColumns.entrySet()) {
                byte[] row = e.getKey();
                byte[] col = CassandraKeyValueServices.decomposeName(e.getValue()).getLhSide();
                // If we read a version of the cell before our start timestamp, it will be the most recent version readable to
                // us and we can continue to the next column. Otherwise we have to continue reading this column.
                LinkedHashMap<Cell, Value> rowResult = results.get(row);
                boolean completedCell = (rowResult != null) && rowResult.containsKey(Cell.create(row, col));
                boolean endOfRange = isEndOfColumnRange(completedCell, col, firstPage.getRowsToRawColumnCount().get(row), columnRangeSelection);
                if (!endOfRange) {
                    byte[] nextCol = getNextColumnRangeColumn(completedCell, col);
                    incompleteRowsToNextColumns.put(row, nextCol);
                }
            }

            Map<byte[], RowColumnRangeIterator> ret = Maps.newHashMapWithExpectedSize(rows.size());
            for (byte[] row : rowsToLastCompositeColumns.keySet()) {
                Iterator<Entry<Cell, Value>> resultIterator;
                LinkedHashMap<Cell, Value> result = results.get(row);
                if (result != null) {
                    resultIterator = result.entrySet().iterator();
                } else {
                    resultIterator = Collections.emptyIterator();
                }
                byte[] nextCol = incompleteRowsToNextColumns.get(row);
                if (nextCol == null) {
                    ret.put(row, new LocalRowColumnRangeIterator(resultIterator));
                } else {
                    ColumnRangeSelection newColumnRange = new ColumnRangeSelection(nextCol,
                            columnRangeSelection.getEndCol(), columnRangeSelection.getBatchHint());
                    ret.put(row, new LocalRowColumnRangeIterator(Iterators.concat(resultIterator, getRowColumnRange(host, tableRef, row, newColumnRange, startTs))));
                }
            }
            // We saw no Cassandra results at all for these rows, so the entire column range is empty for these rows.
            for (byte[] row : firstPage.getEmptyRows()) {
                ret.put(row, new LocalRowColumnRangeIterator(Collections.emptyIterator()));
            }
            return ret;
        } catch (Exception e) {
            throw Throwables.throwUncheckedException(e);
        }
    }

    private RowColumnRangeExtractor.RowColumnRangeResult getRowsColumnRangeForSingleHost(InetSocketAddress host,
                                                             TableReference tableRef,
                                                             List<byte[]> rows,
                                                             ColumnRangeSelection columnRangeSelection,
                                                             long startTs) {
        try {
            return clientPool.runWithRetryOnHost(host, new FunctionCheckedException<Client, RowColumnRangeExtractor.RowColumnRangeResult, Exception>() {
                @Override
                public RowColumnRangeExtractor.RowColumnRangeResult apply(Client client) throws Exception {
                    SlicePredicate pred = getSlicePredicate(columnRangeSelection, startTs);

                    Map<ByteBuffer, List<ColumnOrSuperColumn>> results = multigetInternal(client, tableRef, wrap(rows), pred, readConsistency);

                    RowColumnRangeExtractor extractor = new RowColumnRangeExtractor();
                    extractor.extractResults(results, startTs);

                    return extractor.getRowColumnRangeResult();
                }

                @Override
                public String toString() {
                    return "multiget_slice(" + tableRef.getQualifiedName() + ", " + rows.size() + " rows, " + columnRangeSelection.getBatchHint() + " max columns)";
                }
            });
        } catch (Exception e) {
            throw Throwables.throwUncheckedException(e);
        }
    }

    private SlicePredicate getSlicePredicate(ColumnRangeSelection columnRangeSelection, long startTs) {
        ByteBuffer start = columnRangeSelection.getStartCol().length == 0 ?
                ByteBuffer.wrap(PtBytes.EMPTY_BYTE_ARRAY) :
                CassandraKeyValueServices.makeCompositeBuffer(columnRangeSelection.getStartCol(), startTs - 1);
        ByteBuffer end = columnRangeSelection.getEndCol().length == 0 ?
                ByteBuffer.wrap(PtBytes.EMPTY_BYTE_ARRAY) :
                CassandraKeyValueServices.makeCompositeBuffer(RangeRequests.previousLexicographicName(columnRangeSelection.getEndCol()), -1);
        SliceRange slice = new SliceRange(start, end, false, columnRangeSelection.getBatchHint());
        SlicePredicate pred = new SlicePredicate();
        pred.setSlice_range(slice);
        return pred;
    }

    private Iterator<Entry<Cell, Value>> getRowColumnRange(InetSocketAddress host, TableReference tableRef, final byte[] row,
                                                           final ColumnRangeSelection columnRangeSelection, long startTs) {
        return ClosableIterators.wrap(new AbstractPagingIterable<Entry<Cell, Value>, TokenBackedBasicResultsPage<Entry<Cell, Value>, byte[]>>() {
            @Override
            protected TokenBackedBasicResultsPage<Entry<Cell, Value>, byte[]> getFirstPage() throws Exception {
                return page(columnRangeSelection.getStartCol());
            }

            @Override
            protected TokenBackedBasicResultsPage<Entry<Cell, Value>, byte[]> getNextPage(TokenBackedBasicResultsPage<Entry<Cell, Value>, byte[]> previous) throws Exception {
                return page(previous.getTokenForNextPage());
            }

            TokenBackedBasicResultsPage<Entry<Cell, Value>, byte[]> page(final byte[] startCol) throws Exception {
                return clientPool.runWithRetryOnHost(host, new FunctionCheckedException<Client, TokenBackedBasicResultsPage<Entry<Cell, Value>, byte[]>, Exception>() {
                    @Override
                    public TokenBackedBasicResultsPage<Entry<Cell, Value>, byte[]> apply(Client client) throws Exception {
                        ByteBuffer start = startCol.length == 0 ?
                                ByteBuffer.wrap(PtBytes.EMPTY_BYTE_ARRAY) :
                                CassandraKeyValueServices.makeCompositeBuffer(startCol, startTs - 1);
                        ByteBuffer end = columnRangeSelection.getEndCol().length == 0 ?
                                ByteBuffer.wrap(PtBytes.EMPTY_BYTE_ARRAY) :
                                CassandraKeyValueServices.makeCompositeBuffer(RangeRequests.previousLexicographicName(columnRangeSelection.getEndCol()), -1);
                        SliceRange slice = new SliceRange(start, end, false, columnRangeSelection.getBatchHint());
                        SlicePredicate pred = new SlicePredicate();
                        pred.setSlice_range(slice);

                        ByteBuffer rowByteBuffer = ByteBuffer.wrap(row);

                        Map<ByteBuffer, List<ColumnOrSuperColumn>> results = multigetInternal(client, tableRef, ImmutableList.of(rowByteBuffer), pred, readConsistency);

                        if (results.isEmpty()) {
                            return SimpleTokenBackedResultsPage.create(startCol, ImmutableList.<Entry<Cell, Value>>of(), false);
                        }
                        Map<Cell, Value> ret = Maps.newHashMap();
                        new ValueExtractor(ret).extractResults(results, startTs, ColumnSelection.all());
                        List<ColumnOrSuperColumn> values = Iterables.getOnlyElement(results.values());
                        if (values.isEmpty()) {
                            return SimpleTokenBackedResultsPage.create(startCol, ImmutableList.<Entry<Cell, Value>>of(), false);
                        }
                        ColumnOrSuperColumn lastColumn = values.get(values.size() - 1);
                        byte[] lastCol = CassandraKeyValueServices.decomposeName(lastColumn.getColumn()).getLhSide();
                        // Same idea as the getRows case to handle seeing only newer entries of a column
                        boolean completedCell = ret.get(Cell.create(row, lastCol)) != null;
                        if (isEndOfColumnRange(completedCell, lastCol, values.size(), columnRangeSelection)) {
                            return SimpleTokenBackedResultsPage.create(lastCol, ret.entrySet(), false);
                        }
                        byte[] nextCol = getNextColumnRangeColumn(completedCell, lastCol);
                        return SimpleTokenBackedResultsPage.create(nextCol, ret.entrySet(), true);
                    }

                    @Override
                    public String toString() {
                        return "multiget_slice(" + tableRef.getQualifiedName() + ", single row, " + columnRangeSelection.getBatchHint() + " batch hint)";
                    }
                });
            }

        }.iterator());
    }

    private boolean isEndOfColumnRange(boolean completedCell, byte[] lastCol, int numRawResults,
                                       ColumnRangeSelection columnRangeSelection) {
        return (numRawResults < columnRangeSelection.getBatchHint()) ||
                (completedCell &&
                        (RangeRequests.isLastRowName(lastCol)
                                || Arrays.equals(RangeRequests.nextLexicographicName(lastCol), columnRangeSelection.getEndCol())));
    }

    private byte[] getNextColumnRangeColumn(boolean completedCell, byte[] lastCol) {
        if (!completedCell) {
            return lastCol;
        } else {
            return RangeRequests.nextLexicographicName(lastCol);
        }
    }

    @Override
    public Map<Cell, Long> getLatestTimestamps(TableReference tableRef, Map<Cell, Long> timestampByCell) {
        // TODO: optimize by only getting column name after cassandra api change
        return super.getLatestTimestamps(tableRef, timestampByCell);
    }

    @Override
    public void put(final TableReference tableRef, final Map<Cell, byte[]> values, final long timestamp) {
        try {
            putInternal(tableRef, KeyValueServices.toConstantTimestampValues(values.entrySet(), timestamp));
        } catch (Exception e) {
            throw Throwables.throwUncheckedException(e);
        }
    }

    @Override
    public void putWithTimestamps(TableReference tableRef, Multimap<Cell, Value> values) {
        try {
            putInternal(tableRef, values.entries());
        } catch (Exception e) {
            throw Throwables.throwUncheckedException(e);
        }
    }

    @Override
    protected int getMultiPutBatchCount() {
        return configManager.getConfig().mutationBatchCount();
    }

    private void putInternal(final TableReference tableRef,
                             final Iterable<Map.Entry<Cell, Value>> values) throws Exception {
        putInternal(tableRef, values, CassandraConstants.NO_TTL);
    }

    protected void putInternal(final TableReference tableRef,
                               Iterable<Map.Entry<Cell, Value>> values,
                               final int ttl) throws Exception {
        Map<InetSocketAddress, Map<Cell, Value>> cellsByHost = partitionMapByHost(values);
        List<Callable<Void>> tasks = Lists.newArrayListWithCapacity(cellsByHost.size());
        for (final Map.Entry<InetSocketAddress, Map<Cell, Value>> entry : cellsByHost.entrySet()) {
            tasks.add(ThreadNamingCallable.wrapWithThreadName(() -> {
                        putForSingleHostInternal(entry.getKey(), tableRef, entry.getValue().entrySet(), ttl);
                        return null;
                    },
                    "Atlas putInternal " + entry.getValue().size() + " cell values to " + tableRef + " on " + entry.getKey(),
                    ThreadNamingCallable.Type.PREPEND));
        }
        runAllTasksCancelOnFailure(tasks);
    }

    private void putForSingleHostInternal(final InetSocketAddress host,
                                          final TableReference tableRef,
                                          final Iterable<Map.Entry<Cell, Value>> values,
                                          final int ttl) throws Exception {
        clientPool.runWithRetryOnHost(host, new FunctionCheckedException<Client, Void, Exception>() {
            @Override
            public Void apply(Client client) throws Exception {
                final CassandraKeyValueServiceConfig config = configManager.getConfig();
                int mutationBatchCount = config.mutationBatchCount();
                int mutationBatchSizeBytes = config.mutationBatchSizeBytes();
                for (List<Entry<Cell, Value>> partition : partitionByCountAndBytes(values, mutationBatchCount,
                        mutationBatchSizeBytes, tableRef, ENTRY_SIZING_FUNCTION)) {
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

                        List<Mutation> tableMutations = rowPuts.get(internalTableName(tableRef));
                        if (tableMutations == null) {
                            tableMutations = Lists.<Mutation>newArrayList();
                            rowPuts.put(internalTableName(tableRef), tableMutations);
                        }

                        tableMutations.add(m);
                    }
                    batchMutateInternal(client, tableRef, map, writeConsistency);
                }
                return null;
            }

            @Override
            public String toString() {
                return "batch_mutate(" + host + ", " + tableRef.getQualifiedName() + ", " + Iterables.size(values) + " values, " + ttl + " ttl sec)";
            }
        });
    }

    // Overridden to batch more intelligently than the default implementation.
    @Override
    public void multiPut(Map<TableReference, ? extends Map<Cell, byte[]>> valuesByTable, final long timestamp) throws KeyAlreadyExistsException {
        List<TableCellAndValue> flattened = Lists.newArrayList();
        for (Map.Entry<TableReference, ? extends Map<Cell, byte[]>> tableAndValues : valuesByTable.entrySet()) {
            for (Map.Entry<Cell, byte[]> entry : tableAndValues.getValue().entrySet()) {
                flattened.add(new TableCellAndValue(tableAndValues.getKey(), entry.getKey(), entry.getValue()));
            }
        }
        Map<InetSocketAddress, List<TableCellAndValue>> partitionedByHost =
                partitionByHost(flattened, TableCellAndValue.EXTRACT_ROW_NAME_FUNCTION);

        List<Callable<Void>> callables = Lists.newArrayList();
        for (Map.Entry<InetSocketAddress, List<TableCellAndValue>> entry : partitionedByHost.entrySet()) {
            callables.addAll(getMultiPutTasksForSingleHost(entry.getKey(), entry.getValue(), timestamp));
        }
        runAllTasksCancelOnFailure(callables);
    }

    private List<Callable<Void>> getMultiPutTasksForSingleHost(final InetSocketAddress host,
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
            final Set<TableReference> tableRefs = extractTableNames(batch);
            tasks.add(ThreadNamingCallable.wrapWithThreadName(() -> {
                        multiPutForSingleHostInternal(host, tableRefs, batch, timestamp);
                        return null;
                    },
                    "Atlas multiPut of " + batch.size() + " cells into " + tableRefs + " on " + host,
                    ThreadNamingCallable.Type.PREPEND));
        }
        return tasks;
    }

    private Set<TableReference> extractTableNames(Iterable<TableCellAndValue> tableCellAndValues) {
        Set<TableReference> tableRefs = Sets.newHashSet();
        for (TableCellAndValue tableCellAndValue : tableCellAndValues) {
            tableRefs.add(tableCellAndValue.tableRef);
        }
        return tableRefs;
    }

    private void multiPutForSingleHostInternal(final InetSocketAddress host,
                                               final Set<TableReference> tableRefs,
                                               final List<TableCellAndValue> batch,
                                               long timestamp) throws Exception {
        final Map<ByteBuffer, Map<String, List<Mutation>>> map = convertToMutations(batch, timestamp);
        clientPool.runWithRetryOnHost(host, new FunctionCheckedException<Client, Void, Exception>() {
            @Override
            public Void apply(Client client) throws Exception {
                batchMutateInternal(client, tableRefs, map, writeConsistency);
                return null;
            }

            @Override
            public String toString() {
                return "batch_mutate(" + host + ", " + tableRefs + ", " + batch.size() + " values)";
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

            List<Mutation> tableMutations = rowPuts.get(internalTableName(tableCellAndValue.tableRef));
            if (tableMutations == null) {
                tableMutations = Lists.<Mutation>newArrayList();
                rowPuts.put(internalTableName(tableCellAndValue.tableRef), tableMutations);
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
                                     TableReference tableRef,
                                     Map<ByteBuffer, Map<String, List<Mutation>>> map,
                                     ConsistencyLevel consistency) throws TException {
        batchMutateInternal(client, ImmutableSet.of(tableRef), map, consistency);
    }

    private void batchMutateInternal(final Client client,
                                     final Set<TableReference> tableRefs,
                                     final Map<ByteBuffer, Map<String, List<Mutation>>> map,
                                     final ConsistencyLevel consistency) throws TException {
        run(client, tableRefs, () -> {
            client.batch_mutate(map, consistency);
            return true;
        });
    }

    private boolean shouldTraceQuery(Set<TableReference> tableRefs) {
        for (TableReference tableRef : tableRefs) {
            if (shouldTraceQuery(tableRef)) {
                return true;
            }
        }
        return false;
    }

    private void logFailedCall(Set<TableReference> tableRefs) {
        log.error("A call to table(s) {} failed with an exception.",
                tableRefs.stream().map(TableReference::getQualifiedName).collect(Collectors.joining(", ")));
    }

    private void logTraceResults(long duration, Set<TableReference> tableRefs, ByteBuffer recv_trace, boolean failed) {
        if (failed || duration > getMinimumDurationToTraceMillis()) {
            log.error("Traced a call to {} that {}took {} ms. It will appear in system_traces with UUID={}",
                    tableRefs.stream().map(TableReference::getQualifiedName).collect(Collectors.joining(", ")),
                    failed ? "failed and " : "",
                    duration,
                    CassandraKeyValueServices.convertCassandraByteBufferUUIDtoString(recv_trace));
        }
    }

    private Map<ByteBuffer, List<ColumnOrSuperColumn>> multigetInternal(Client client,
                                                                        TableReference tableRef,
                                                                        List<ByteBuffer> rowNames,
                                                                        SlicePredicate pred,
                                                                        ConsistencyLevel consistencyLevel) throws TException {
        ColumnParent colFam = new ColumnParent(internalTableName(tableRef));
        return run(client, tableRef,
                () -> client.multiget_slice(rowNames, colFam, pred, consistencyLevel));
    }

    @Override
    public void truncateTable(final TableReference tableRef) {
        truncateTables(ImmutableSet.of(tableRef));
    }

    @Override
    public void truncateTables(final Set<TableReference> tablesToTruncate) {
        if (!tablesToTruncate.isEmpty()) {
            try {
                clientPool.runWithRetry(new FunctionCheckedException<Client, Void, Exception>() {
                    @Override
                    public Void apply(Client client) throws Exception {
                        for (TableReference tableRef : tablesToTruncate) {
                            truncateInternal(client, tableRef);
                        }
                        return null;
                    }

                    @Override
                    public String toString() {
                        return "truncateTables(" + tablesToTruncate.size() + " tables)";
                    }
                });
            } catch (UnavailableException e) {
                throw new PalantirRuntimeException("Creating tables requires all Cassandra nodes to be up and available.");
            } catch (Exception e) {
                throw Throwables.throwUncheckedException(e);
            }
        }
    }

    private void truncateInternal(Client client, TableReference tableRef) throws TException {
        for (int tries = 1; tries <= CassandraConstants.MAX_TRUNCATION_ATTEMPTS; tries++) {
            boolean successful = true;
            try {
                run(client, tableRef, () -> {
                    client.truncate(internalTableName(tableRef));
                    return true;
                });
            } catch (TException e) {
                log.error("Cluster was unavailable while we attempted a truncate for table " + tableRef.getQualifiedName() + "; we will try " + (CassandraConstants.MAX_TRUNCATION_ATTEMPTS - tries) + " additional time(s). (" + e.getMessage() + ")");
                if (CassandraConstants.MAX_TRUNCATION_ATTEMPTS - tries == 0) {
                    throw e;
                }
                successful = false;
                try {
                    Thread.sleep(new Random().nextInt((1 << (CassandraConstants.MAX_TRUNCATION_ATTEMPTS - tries)) - 1) * 1000);
                } catch (InterruptedException e1) {
                    Thread.currentThread().interrupt();
                }
            }
            if (successful) {
                break;
            }
        }
    }

    @Override
    public void delete(TableReference tableRef, Multimap<Cell, Long> keys) {
        Map<InetSocketAddress, Map<Cell, Collection<Long>>> keysByHost = partitionMapByHost(keys.asMap().entrySet());
        for (Map.Entry<InetSocketAddress, Map<Cell, Collection<Long>>> entry : keysByHost.entrySet()) {
            deleteOnSingleHost(entry.getKey(), tableRef, entry.getValue());
        }
    }

    private void deleteOnSingleHost(final InetSocketAddress host,
                                    final TableReference tableRef,
                                    final Map<Cell, Collection<Long>> cellVersionsMap) {
        try {
            clientPool.runWithRetryOnHost(host, new FunctionCheckedException<Client, Void, Exception>() {

                int numVersions = 0;
                @Override
                public Void apply(Client client) throws Exception {
                    // Delete must delete in the order of timestamp and we don't trust batch_mutate to do it
                    // atomically so we have to potentially do many deletes if there are many timestamps for the
                    // same key.
                    Map<Integer, Map<ByteBuffer, Map<String, List<Mutation>>>> maps = Maps.newTreeMap();
                    for (Entry<Cell, Collection<Long>> cellVersions : cellVersionsMap.entrySet()) {
                        int mapIndex = 0;
                        for (long ts : Ordering.natural().immutableSortedCopy(cellVersions.getValue())) {
                            if (!maps.containsKey(mapIndex)) {
                                maps.put(mapIndex, Maps.<ByteBuffer, Map<String, List<Mutation>>>newHashMap());
                            }
                            Map<ByteBuffer, Map<String, List<Mutation>>> map = maps.get(mapIndex);
                            ByteBuffer colName = CassandraKeyValueServices.makeCompositeBuffer(cellVersions.getKey().getColumnName(), ts);
                            SlicePredicate pred = new SlicePredicate();
                            pred.setColumn_names(Arrays.asList(colName));
                            Deletion del = new Deletion();
                            del.setPredicate(pred);
                            del.setTimestamp(Long.MAX_VALUE);
                            Mutation m = new Mutation();
                            m.setDeletion(del);
                            ByteBuffer rowName = ByteBuffer.wrap(cellVersions.getKey().getRowName());
                            if (!map.containsKey(rowName)) {
                                map.put(rowName, Maps.<String, List<Mutation>>newHashMap());
                            }
                            Map<String, List<Mutation>> rowPuts = map.get(rowName);
                            if (!rowPuts.containsKey(internalTableName(tableRef))) {
                                rowPuts.put(internalTableName(tableRef), Lists.<Mutation>newArrayList());
                            }
                            rowPuts.get(internalTableName(tableRef)).add(m);
                            mapIndex++;
                            numVersions += cellVersions.getValue().size();
                        }
                    }
                    for (Map<ByteBuffer, Map<String, List<Mutation>>> map : maps.values()) {
                        // NOTE: we run with ConsistencyLevel.ALL here instead of ConsistencyLevel.QUORUM
                        // because we want to remove all copies of this data
                        batchMutateInternal(client, tableRef, map, deleteConsistency);
                    }
                    return null;
                }

                @Override
                public String toString() {
                    return "delete_batch_mutate(" + host + ", " + tableRef.getQualifiedName() + ", " + numVersions + " total versions of " + cellVersionsMap.size() + " keys)";
                }
            });
        } catch (Exception e) {
            throw Throwables.throwUncheckedException(e);
        }
    }

    // update CKVS.isMatchingCf if you update this method
    private CfDef getCfForTable(TableReference tableRef, byte[] rawMetadata) {
        final CassandraKeyValueServiceConfig config = configManager.getConfig();
        Map<String, String> compressionOptions = Maps.newHashMap();
        CfDef cf = CassandraConstants.getStandardCfDef(config.keyspace(), internalTableName(tableRef));

        boolean negativeLookups = false;
        double falsePositiveChance = CassandraConstants.DEFAULT_LEVELED_COMPACTION_BLOOM_FILTER_FP_CHANCE;
        int explicitCompressionBlockSizeKB = 0;
        boolean appendHeavyAndReadLight = false;
        TableMetadataPersistence.CachePriority cachePriority = TableMetadataPersistence.CachePriority.WARM;

        if (!CassandraKeyValueServices.isEmptyOrInvalidMetadata(rawMetadata)) {
            TableMetadata tableMetadata = TableMetadata.BYTES_HYDRATOR.hydrateFromBytes(rawMetadata);
            negativeLookups = tableMetadata.hasNegativeLookups();
            explicitCompressionBlockSizeKB = tableMetadata.getExplicitCompressionBlockSizeKB();
            appendHeavyAndReadLight = tableMetadata.isAppendHeavyAndReadLight();
            cachePriority = tableMetadata.getCachePriority();
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

        switch (cachePriority) {
            case COLDEST:
                break;
            case COLD:
                break;
            case WARM:
                break;
            case HOT:
                break;
            case HOTTEST:
                cf.setPopulate_io_cache_on_flushIsSet(true);
        }

        cf.setBloom_filter_fp_chance(falsePositiveChance);
        cf.setCompression_options(compressionOptions);
        return cf;
    }

    //TODO: after cassandra change: handle multiRanges
    @Override
    @Idempotent
    public Map<RangeRequest, TokenBackedBasicResultsPage<RowResult<Value>, byte[]>> getFirstBatchForRanges(TableReference tableRef,
                                                                                                           Iterable<RangeRequest> rangeRequests,
                                                                                                           long timestamp) {
        int concurrency = configManager.getConfig().rangesConcurrency();
        return KeyValueServices.getFirstBatchForRangesUsingGetRangeConcurrent(
                executor, this, tableRef, rangeRequests, timestamp, concurrency);
    }


    // TODO: after cassandra change: handle reverse ranges
    // TODO: after cassandra change: handle column filtering
    @Override
    @Idempotent
    public ClosableIterator<RowResult<Value>> getRange(TableReference tableRef, final RangeRequest rangeRequest, final long timestamp) {
        return getRangeWithPageCreator(tableRef, rangeRequest, timestamp, readConsistency, ValueExtractor.SUPPLIER);
    }

    /* TODO: Plan of attack
    1. Start with a RangeRequest (starting row, num rows per batch)
    2. Use rangeRequest, only get 1 column per row, and just return the list of row keys
    3. Using this list, and a ColumnRangeSelection, get all the columns/timestamps for that batch of rows (ColumnRangeSelection.batchHint at a time)
    4. Repeat 2 + 3 with the rest of the rows (RangeRequest: starting row_n+1, num rows per batch), until done.
     */
    @Override
    @Idempotent
    public ClosableIterator<RowResult<Set<Long>>> getRangeOfTimestamps(TableReference tableRef, RangeRequest rangeRequest, long timestamp) {
        return getRangeWithPageCreator(tableRef, rangeRequest, timestamp, deleteConsistency, TimestampExtractor.SUPPLIER);
    }

    @Override
    @Idempotent
    public ClosableIterator<RowResult<Set<Value>>> getRangeWithHistory(TableReference tableRef, RangeRequest rangeRequest, long timestamp) {
        return getRangeWithPageCreator(tableRef, rangeRequest, timestamp, deleteConsistency, HistoryExtractor.SUPPLIER);
    }

    public <T, U> ClosableIterator<RowResult<U>> getRangeWithPageCreator(final TableReference tableRef,
                                                                         final RangeRequest rangeRequest,
                                                                         final long timestamp,
                                                                         final ConsistencyLevel consistency,
                                                                         final Supplier<ResultsExtractor<T, U>> resultsExtractor) {
        if (rangeRequest.isReverse()) {
            throw new UnsupportedOperationException();
        }
        if (rangeRequest.isEmptyRange()) {
            return ClosableIterators.wrap(ImmutableList.<RowResult<U>>of().iterator());
        }
        final int batchHint = rangeRequest.getBatchHint() == null ? 100 : rangeRequest.getBatchHint();
        SliceRange slice = new SliceRange(ByteBuffer.wrap(PtBytes.EMPTY_BYTE_ARRAY), ByteBuffer.wrap(PtBytes.EMPTY_BYTE_ARRAY), false, Integer.MAX_VALUE);
        final SlicePredicate pred = new SlicePredicate();
        pred.setSlice_range(slice);

        final ColumnParent colFam = new ColumnParent(internalTableName(tableRef));
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
                        InetSocketAddress host = clientPool.getRandomHostForKey(startKey);
                        return clientPool.runWithRetryOnHost(host, new FunctionCheckedException<Client, TokenBackedBasicResultsPage<RowResult<U>, byte[]>, Exception>() {
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
                                    firstPage = run(client, tableRef,
                                            () -> client.get_range_slices(colFam, pred, keyRange, consistency));
                                } catch (UnavailableException e) {
                                    if (consistency.equals(ConsistencyLevel.ALL)) {
                                        throw new InsufficientConsistencyException("This operation requires all Cassandra nodes to be up and available.", e);
                                    } else {
                                        throw e;
                                    }
                                }

                                Map<ByteBuffer, List<ColumnOrSuperColumn>> colsByKey = CassandraKeyValueServices.getColsByKey(firstPage);
                                TokenBackedBasicResultsPage<RowResult<U>, byte[]> page =
                                        resultsExtractor.get().getPageFromRangeResults(colsByKey, timestamp, selection, endExclusive);
                                if (page.moreResultsAvailable() && firstPage.size() < batchHint) {
                                    // If get_range_slices didn't return the full number of results, there's no
                                    // point to trying to get another page
                                    page = SimpleTokenBackedResultsPage.create(endExclusive, page.getResults(), false);
                                }
                                return page;
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
    public void dropTable(final TableReference tableRef) {
        dropTables(ImmutableSet.of(tableRef));
    }

    /**
     * Main gains here vs. dropTable:
     *    - problems excepting, we will basically be serializing a rapid series of schema changes
     *      through a single host checked out from the client pool, so reduced chance of schema disagreement issues
     *    - client-side in-memory lock to prevent misbehaving callers from shooting themselves in the foot
     *    - one less round trip
     */
    @Override
    public void dropTables(final Set<TableReference> tablesToDrop) {
        schemaMutationLock.runWithLock(() -> dropTablesWithLock(tablesToDrop));
    }

    private void dropTablesWithLock(final Set<TableReference> tablesToDrop) throws Exception {
        try {
            clientPool.runWithRetry(new FunctionCheckedException<Client, Void, Exception>() {
                @Override
                public Void apply(Client client) throws Exception {
                    KsDef ks = client.describe_keyspace(configManager.getConfig().keyspace());
                    Set<TableReference> existingTables = Sets.newHashSet();

                    for (CfDef cf : ks.getCf_defs()) {
                        existingTables.add(fromInternalTableName(cf.getName()));
                    }

                    for (TableReference table : tablesToDrop) {
                        CassandraVerifier.sanityCheckTableName(table);

                        if (existingTables.contains(table)) {
                            client.system_drop_column_family(internalTableName(table));
                            putMetadataWithoutChangingSettings(table, PtBytes.EMPTY_BYTE_ARRAY);
                        } else {
                            log.warn("Ignored call to drop a table ({}) that did not exist.", table);
                        }
                    }
                    CassandraKeyValueServices.waitForSchemaVersions(client, "(all tables in a call to dropTables)", configManager.getConfig().schemaMutationTimeoutMillis());
                    return null;
                }
            });
        } catch (UnavailableException e) {
            throw new PalantirRuntimeException("Dropping tables requires all Cassandra nodes to be up and available.");
        }
    }

    @Override
    public void createTable(final TableReference tableRef, final byte[] tableMetadata) {
        createTables(ImmutableMap.of(tableRef, tableMetadata));
    }

    /**
     * Main gains here vs. createTable:
     *    - problems excepting, we will basically be serializing a rapid series of schema changes
     *      through a single host checked out from the client pool, so reduced chance of schema disagreement issues
     *    - client-side in-memory lock to prevent misbehaving callers from shooting themselves in the foot
     *    - one less round trip
     */
    @Override
    public void createTables(final Map<TableReference, byte[]> tableNamesToTableMetadata) {
        schemaMutationLock.runWithLock(() -> createTablesWithLock(tableNamesToTableMetadata));
        internalPutMetadataForTables(tableNamesToTableMetadata, false);
    }

    private void createTablesWithLock(final Map<TableReference, byte[]> tableNamesToTableMetadata) throws Exception {
        try {
            clientPool.runWithRetry((FunctionCheckedException<Client, Void, Exception>) client -> {
                KsDef ks = client.describe_keyspace(configManager.getConfig().keyspace());
                Set<TableReference> tablesToCreate = tableNamesToTableMetadata.keySet();
                Set<TableReference> existingTablesLowerCased = Sets.newHashSet();

                for (CfDef cf : ks.getCf_defs()) {
                    existingTablesLowerCased.add(fromInternalTableName(cf.getName().toLowerCase()));
                }

                for (TableReference table : tablesToCreate) {
                    CassandraVerifier.sanityCheckTableName(table);

                    TableReference tableRefLowerCased = TableReference.createUnsafe(table.getQualifiedName().toLowerCase());
                    if (!existingTablesLowerCased.contains(tableRefLowerCased)) {
                        client.system_add_column_family(getCfForTable(table, tableNamesToTableMetadata.get(table)));
                    } else {
                        log.warn("Ignored call to create a table ({}) that already existed (case insensitive).", table);
                    }
                }
                if (!tablesToCreate.isEmpty()) {
                    CassandraKeyValueServices.waitForSchemaVersions(client, "(all tables in a call to createTables)", configManager.getConfig().schemaMutationTimeoutMillis());
                }
                return null;
            });
        } catch (UnavailableException e) {
            throw new PalantirRuntimeException("Creating tables requires all Cassandra nodes to be up and available.");
        }
    }

    @Override
    public Set<TableReference> getAllTableNames() {
        return Sets.filter(getAllTablenamesInternal(), tr -> !hiddenTables.isHidden(tr));
    }

    private Set<TableReference> getAllTablenamesInternal() {
        final CassandraKeyValueServiceConfig config = configManager.getConfig();
        try {
            return clientPool.runWithRetry(new FunctionCheckedException<Client, Set<TableReference>, Exception>() {

                @Override
                public Set<TableReference> apply(Client client) throws Exception {
                    return FluentIterable.from(client.describe_keyspace(config.keyspace()).getCf_defs())
                            .transform(new Function<CfDef, TableReference>() {
                                @Override
                                public TableReference apply(CfDef cf) {
                                    return fromInternalTableName(cf.getName());
                                }
                            }).toSet();
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
    public byte[] getMetadataForTable(TableReference tableRef) {
        Cell cell = getMetadataCell(tableRef);
        Value v = get(AtlasDbConstants.METADATA_TABLE, ImmutableMap.of(cell, Long.MAX_VALUE)).get(cell);
        if (v == null) {
            return AtlasDbConstants.EMPTY_TABLE_METADATA;
        } else {
            if (!getAllTablenamesInternal().contains(tableRef)) {
                log.error("While getting metadata, found a table, {}, with stored table metadata " +
                        "but no corresponding existing table in the underlying KVS. " +
                        "This is not necessarily a bug, but warrants further inquiry.", tableRef.getQualifiedName());
                }
            return v.getContents();
        }
    }

    @Override
    public Map<TableReference, byte[]> getMetadataForTables() {
        Map<TableReference, byte[]> tableToMetadataContents = Maps.newHashMap();
        ClosableIterator<RowResult<Value>> range = getRange(AtlasDbConstants.METADATA_TABLE, RangeRequest.all(), Long.MAX_VALUE);
        try {
            while (range.hasNext()) {
                RowResult<Value> valueRow = range.next();
                Iterable<Entry<Cell, Value>> cells = valueRow.getCells();

                for (Entry<Cell, Value> entry : cells) {
                    Value value = entry.getValue();
                    TableReference tableRef = TableReference.createUnsafe(new String(entry.getKey().getRowName()));
                    byte[] contents;
                    if (value == null) {
                        contents = AtlasDbConstants.EMPTY_TABLE_METADATA;
                    } else {
                        contents = value.getContents();
                    }
                    if (!hiddenTables.isHidden(tableRef)) {
                        tableToMetadataContents.put(tableRef, contents);
                    }
                }
            }
        } finally {
            range.close();
        }
        return tableToMetadataContents;
    }

    private Cell getMetadataCell(TableReference tableRef) { // would have preferred an explicit charset, but thrift uses default internally
        return Cell.create(tableRef.getQualifiedName().getBytes(Charset.defaultCharset()), "m".getBytes());
    }

    @Override
    public void putMetadataForTable(final TableReference tableRef, final byte[] meta) {
        putMetadataForTables(ImmutableMap.of(tableRef, meta));
    }

    @Override
    public void putMetadataForTables(final Map<TableReference, byte[]> tableRefToMetadata) {
        internalPutMetadataForTables(tableRefToMetadata, true);
    }

    private void internalPutMetadataForTables(final Map<TableReference, byte[]> unfilteredTableNameToMetadata, final boolean possiblyNeedToPerformSettingsChanges) {
        Map<TableReference, byte[]> tableNameToMetadata = Maps.filterValues(unfilteredTableNameToMetadata, Predicates.not(Predicates.equalTo(AtlasDbConstants.EMPTY_TABLE_METADATA)));
        if (tableNameToMetadata.isEmpty()) {
            return;
        }

        final Map<Cell, byte[]> metadataRequestedForUpdate = Maps.newHashMapWithExpectedSize(tableNameToMetadata.size());
        for (Entry<TableReference, byte[]> tableEntry : tableNameToMetadata.entrySet()) {
            metadataRequestedForUpdate.put(getMetadataCell(tableEntry.getKey()), tableEntry.getValue());
        }

        Map<Cell, Long> requestForLatestDbSideMetadata = Maps.transformValues(metadataRequestedForUpdate, Functions.constant(Long.MAX_VALUE));

        // technically we're racing other services from here on, during an update period,
        // but the penalty for not caring is just some superfluous schema mutations and a few dead rows in the metadata table.
        Map<Cell, Value> persistedMetadata = get(AtlasDbConstants.METADATA_TABLE, requestForLatestDbSideMetadata);
        final Map<Cell, byte[]> newMetadata = Maps.newHashMap();
        final Collection<CfDef> updatedCfs = Lists.newArrayList();
        for (Entry<Cell, byte[]> entry : metadataRequestedForUpdate.entrySet()) {
            Value val = persistedMetadata.get(entry.getKey());
            if (val == null || !Arrays.equals(val.getContents(), entry.getValue())) {
                newMetadata.put(entry.getKey(), entry.getValue());
                updatedCfs.add(getCfForTable(TableReference.createUnsafe(new String(entry.getKey().getRowName())), entry.getValue()));
            }
        }

        if (newMetadata.isEmpty()) {
            return;
        }

        if (possiblyNeedToPerformSettingsChanges) {
            schemaMutationLock.runWithLock(() -> putMetadataForTablesWithLock(possiblyNeedToPerformSettingsChanges, newMetadata, updatedCfs));
        } else {
            try {
                putMetadataForTablesWithLock(possiblyNeedToPerformSettingsChanges, newMetadata, updatedCfs);
            } catch (Exception e) {
                throw Throwables.throwUncheckedException(e);
            }
        }
    }

    private void putMetadataForTablesWithLock(final boolean possiblyNeedToPerformSettingsChanges, final Map<Cell, byte[]> newMetadata, final Collection<CfDef> updatedCfs) throws Exception {
        clientPool.runWithRetry(new FunctionCheckedException<Client, Void, Exception>() {
            @Override
            public Void apply(Client client) throws Exception {
                if (possiblyNeedToPerformSettingsChanges) {
                    for (CfDef cf : updatedCfs) {
                        client.system_update_column_family(cf);
                    }

                    CassandraKeyValueServices.waitForSchemaVersions(client, "(all tables in a call to putMetadataForTables)", configManager.getConfig().schemaMutationTimeoutMillis());
                }
                // Done with actual schema mutation, push the metadata
                put(AtlasDbConstants.METADATA_TABLE, newMetadata, System.currentTimeMillis());
                return null;
            }
        });
    }

    private void putMetadataWithoutChangingSettings(final TableReference tableRef, final byte[] meta) {
        put(AtlasDbConstants.METADATA_TABLE, ImmutableMap.of(getMetadataCell(tableRef), meta), System.currentTimeMillis());
    }

    @Override
    public void close() {
        clientPool.shutdown();
        if (compactionManager.isPresent()) {
            compactionManager.get().close();
        }
        super.close();
    }

    @Override
    public void addGarbageCollectionSentinelValues(TableReference tableRef, Set<Cell> cells) {
        try {
            final Value value = Value.create(PtBytes.EMPTY_BYTE_ARRAY, Value.INVALID_VALUE_TIMESTAMP);
            putInternal(tableRef, Iterables.transform(cells, new Function<Cell, Map.Entry<Cell, Value>>() {
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
    public Multimap<Cell, Long> getAllTimestamps(TableReference tableRef, Set<Cell> cells, long ts) {
        AllTimestampsCollector collector = new AllTimestampsCollector();
        try {
            loadWithTs(tableRef, cells, ts, true, collector, deleteConsistency);
        } catch (UnavailableException e) {
            throw new InsufficientConsistencyException("Get all timestamps requires all Cassandra nodes to be up and available.", e);
        } catch (Exception e) {
            throw Throwables.throwUncheckedException(e);
        }
        return collector.collectedResults;
    }

    @Override
    public void putUnlessExists(final TableReference tableRef, final Map<Cell, byte[]> values)
            throws KeyAlreadyExistsException {
        String tableName = internalTableName(tableRef);
        try {
            clientPool.runWithRetry(new FunctionCheckedException<Client, Void, Exception>() {
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
                        CASResult casResult = run(client, tableRef, () -> client.cas(
                                rowName,
                                tableName,
                                ImmutableList.<Column>of(),
                                ImmutableList.of(col),
                                ConsistencyLevel.SERIAL,
                                writeConsistency));
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

    private interface Action<V> {
        V run() throws TException;
    }

    private <V> V run(Client client, Set<TableReference> tableRefs, Action<V> action) throws TException {
        if (shouldTraceQuery(tableRefs)) {
            return trace(action, client, tableRefs);
        } else {
            try {
                return action.run();
            } catch (TException e) {
                logFailedCall(tableRefs);
                throw e;
            }
        }
    }

    private <V> V run(Client client, TableReference tableRef, Action<V> action) throws TException {
        return run(client, ImmutableSet.of(tableRef), action);
    }

    private <V> V trace(Action<V> action, Client client, Set<TableReference> tableRefs) throws TException {
        ByteBuffer traceId = client.trace_next_query();
        Stopwatch stopwatch = Stopwatch.createStarted();
        boolean failed = false;
        try {
            return action.run();
        } catch (TException e) {
            failed = true;
            logFailedCall(tableRefs);
            throw e;
        } finally {
            long duration = stopwatch.elapsed(TimeUnit.MILLISECONDS);
            logTraceResults(duration, tableRefs, traceId, failed);
        }
    }

    @Override
    public void compactInternally(TableReference tableRef) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(tableRef.getQualifiedName()), "tableRef:[%s] should not be null or empty.", tableRef);
        CassandraKeyValueServiceConfig config = configManager.getConfig();
        if (!compactionManager.isPresent()) {
            log.error("No compaction client was configured, but compact was called. If you actually want to clear deleted data immediately " +
                    "from Cassandra, lower your gc_grace_seconds setting and run `nodetool compact {} {}`.", config.keyspace(), tableRef);
            return;
        }
        long timeoutInSeconds = config.jmx().get().compactionTimeoutSeconds();
        String keyspace = config.keyspace();
        try {
            alterGcAndTombstone(keyspace, tableRef, 0, 0.0f);
            compactionManager.get().performTombstoneCompaction(timeoutInSeconds, keyspace, tableRef);
        } catch (TimeoutException e) {
            log.error("Compaction for {}.{} could not finish in {} seconds.", keyspace, tableRef, timeoutInSeconds, e);
            log.error(compactionManager.get().getCompactionStatus());
        } catch (InterruptedException e) {
            log.error("Compaction for {}.{} was interrupted.", keyspace, tableRef);
        } finally {
            alterGcAndTombstone(keyspace, tableRef, CassandraConstants.GC_GRACE_SECONDS, CassandraConstants.TOMBSTONE_THRESHOLD_RATIO);
        }
    }

    private void alterGcAndTombstone(final String keyspace, final TableReference tableRef, final int gcGraceSeconds, final float tombstoneThresholdRatio) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(keyspace), "keyspace:[%s] should not be null or empty.", keyspace);
        Preconditions.checkArgument(!Strings.isNullOrEmpty(tableRef.getQualifiedName()), "tableRef:[%s] should not be null or empty.", tableRef);
        Preconditions.checkArgument(gcGraceSeconds >= 0, "gc_grace_seconds:[%s] should not be negative.", gcGraceSeconds);
        Preconditions.checkArgument(tombstoneThresholdRatio >= 0.0f && tombstoneThresholdRatio <= 1.0f,
                "tombstone_threshold_ratio:[%s] should be between [0.0, 1.0]", tombstoneThresholdRatio);

        schemaMutationLock.runWithLock(() -> alterGcAndTombstoneWithLock(keyspace, tableRef, gcGraceSeconds, tombstoneThresholdRatio));
    }

    private void alterGcAndTombstoneWithLock(final String keyspace, final TableReference tableRef, final int gcGraceSeconds, final float tombstoneThresholdRatio) {
        try {
            clientPool.runWithRetry((FunctionCheckedException<Client, Void, Exception>) client -> {
                KsDef ks = client.describe_keyspace(keyspace);
                List<CfDef> cfs = ks.getCf_defs();
                for (CfDef cf : cfs) {
                    if (cf.getName().equalsIgnoreCase(internalTableName(tableRef))) {
                        cf.setGc_grace_seconds(gcGraceSeconds);
                        cf.setCompaction_strategy_options(ImmutableMap.of("tombstone_threshold", String.valueOf(tombstoneThresholdRatio)));
                        client.system_update_column_family(cf);
                        CassandraKeyValueServices.waitForSchemaVersions(client, tableRef.getQualifiedName(), configManager.getConfig().schemaMutationTimeoutMillis());
                        log.trace("gc_grace_seconds is set to {} for {}.{}", gcGraceSeconds, keyspace, tableRef);
                        log.trace("tombstone_threshold_ratio is set to {} for {}.{}", tombstoneThresholdRatio, keyspace, tableRef);
                    }
                }
                return null;
            });
        } catch (Exception e) {
            log.error("Exception encountered while setting gc_grace_seconds:{} and tombstone_threshold:{} for {}.{}",
                    gcGraceSeconds,
                    tombstoneThresholdRatio,
                    keyspace,
                    tableRef,
                    e);
        }
    }

    private <V> Map<InetSocketAddress, Map<Cell, V>> partitionMapByHost(Iterable<Map.Entry<Cell, V>> cells) {
        Map<InetSocketAddress, List<Map.Entry<Cell, V>>> partitionedByHost =
                partitionByHost(cells, new Function<Map.Entry<Cell, V>, byte[]>() {
                    @Override
                    public byte[] apply(Entry<Cell, V> entry) {
                        return entry.getKey().getRowName();
                    }
                });
        Map<InetSocketAddress, Map<Cell, V>> cellsByHost = Maps.newHashMap();
        for (Map.Entry<InetSocketAddress, List<Map.Entry<Cell, V>>> hostAndCells : partitionedByHost.entrySet()) {
            Map<Cell, V> cellsForHost = Maps.newHashMapWithExpectedSize(hostAndCells.getValue().size());
            for (Map.Entry<Cell, V> entry : hostAndCells.getValue()) {
                cellsForHost.put(entry.getKey(), entry.getValue());
            }
            cellsByHost.put(hostAndCells.getKey(), cellsForHost);
        }
        return cellsByHost;
    }

    private <V> Map<InetSocketAddress, List<V>> partitionByHost(Iterable<V> iterable, Function<V, byte[]> keyExtractor) {
        // Ensure that the same key goes to the same partition. This is important when writing multiple columns
        // to the same row, since this is a normally a single write in cassandra, whereas splitting the columns
        // into different requests results in multiple writes.
        ListMultimap<ByteBuffer, V> partitionedByKey = ArrayListMultimap.create();
        for (V value : iterable) {
            partitionedByKey.put(ByteBuffer.wrap(keyExtractor.apply(value)), value);
        }
        ListMultimap<InetSocketAddress, V> valuesByHost = ArrayListMultimap.create();
        for (ByteBuffer key : partitionedByKey.keySet()) {
            InetSocketAddress host = clientPool.getRandomHostForKey(key.array());
            valuesByHost.putAll(host, partitionedByKey.get(key));
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
        private final TableReference tableRef;
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

        public TableCellAndValue(TableReference tableRef, Cell cell, byte[] value) {
            this.tableRef = tableRef;
            this.cell = cell;
            this.value = value;
        }
    }
}
