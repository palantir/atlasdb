/*
 * Copyright 2015 Palantir Technologies, Inc. All rights reserved.
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
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.cassandra.thrift.CASResult;
import org.apache.cassandra.thrift.Cassandra.Client;
import org.apache.cassandra.thrift.CfDef;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.Deletion;
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
import com.google.common.base.Preconditions;
import com.google.common.base.Predicates;
import com.google.common.base.Strings;
import com.google.common.base.Supplier;
import com.google.common.collect.ArrayListMultimap;
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
import com.palantir.atlasdb.keyvalue.api.BatchColumnRangeSelection;
import com.palantir.atlasdb.keyvalue.api.CandidateCellForSweeping;
import com.palantir.atlasdb.keyvalue.api.CandidateCellForSweepingRequest;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.CheckAndSetException;
import com.palantir.atlasdb.keyvalue.api.CheckAndSetRequest;
import com.palantir.atlasdb.keyvalue.api.ClusterAvailabilityStatus;
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
import com.palantir.atlasdb.keyvalue.cassandra.paging.CassandraRangePagingIterable;
import com.palantir.atlasdb.keyvalue.cassandra.paging.ColumnFetchMode;
import com.palantir.atlasdb.keyvalue.cassandra.paging.ColumnGetter;
import com.palantir.atlasdb.keyvalue.cassandra.paging.CqlColumnGetter;
import com.palantir.atlasdb.keyvalue.cassandra.paging.RowGetter;
import com.palantir.atlasdb.keyvalue.cassandra.paging.ThriftColumnGetter;
import com.palantir.atlasdb.keyvalue.impl.AbstractKeyValueService;
import com.palantir.atlasdb.keyvalue.impl.Cells;
import com.palantir.atlasdb.keyvalue.impl.GetCandidateCellsForSweepingShim;
import com.palantir.atlasdb.keyvalue.impl.KeyValueServices;
import com.palantir.atlasdb.keyvalue.impl.LocalRowColumnRangeIterator;
import com.palantir.atlasdb.util.AnnotatedCallable;
import com.palantir.atlasdb.util.AnnotationType;
import com.palantir.common.annotation.Idempotent;
import com.palantir.common.base.ClosableIterator;
import com.palantir.common.base.ClosableIterators;
import com.palantir.common.base.FunctionCheckedException;
import com.palantir.common.base.Throwables;
import com.palantir.common.exception.PalantirRuntimeException;
import com.palantir.logsafe.UnsafeArg;
import com.palantir.util.paging.AbstractPagingIterable;
import com.palantir.util.paging.SimpleTokenBackedResultsPage;
import com.palantir.util.paging.TokenBackedBasicResultsPage;

/**
 * Each service can have one or many C* KVS.
 * For each C* KVS, it maintains a list of active nodes, and the client connections attached to each node:
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

    private static final Function<Entry<Cell, Value>, Long> ENTRY_SIZING_FUNCTION = input ->
            input.getValue().getContents().length + 4L + Cells.getApproxSizeOfCell(input.getKey());

    @SuppressWarnings("VisibilityModifier")
    protected final CassandraKeyValueServiceConfigManager configManager;

    private final Optional<CassandraJmxCompactionManager> compactionManager;
    private final CassandraClientPool clientPool;
    private SchemaMutationLock schemaMutationLock;
    private final Optional<LeaderConfig> leaderConfig;
    private final HiddenTables hiddenTables;

    private final SchemaMutationLockTables lockTables;
    private final UniqueSchemaMutationLockTable schemaMutationLockTable;

    private ConsistencyLevel readConsistency = ConsistencyLevel.LOCAL_QUORUM;
    private final ConsistencyLevel writeConsistency = ConsistencyLevel.EACH_QUORUM;
    private final ConsistencyLevel deleteConsistency = ConsistencyLevel.ALL;

    private final TracingQueryRunner queryRunner;
    private final CassandraTables cassandraTables;

    public static CassandraKeyValueService create(
            CassandraKeyValueServiceConfigManager configManager,
            Optional<LeaderConfig> leaderConfig) {
        return create(configManager, leaderConfig, LoggerFactory.getLogger(CassandraKeyValueService.class));
    }

    public static CassandraKeyValueService create(
            CassandraKeyValueServiceConfigManager configManager,
            Optional<LeaderConfig> leaderConfig,
            Logger log) {
        Optional<CassandraJmxCompactionManager> compactionManager =
                CassandraJmxCompaction.createJmxCompactionManager(configManager);
        CassandraKeyValueService ret = new CassandraKeyValueService(
                log,
                configManager,
                compactionManager,
                leaderConfig);
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

        this.lockTables = new SchemaMutationLockTables(clientPool, configManager.getConfig());
        this.schemaMutationLockTable = new UniqueSchemaMutationLockTable(lockTables, whoIsTheLockCreator());

        this.queryRunner = new TracingQueryRunner(log, tracingPrefs);
        this.cassandraTables = new CassandraTables(clientPool, configManager);
    }

    private LockLeader whoIsTheLockCreator() {
        return leaderConfig
                .map((config) -> config.whoIsTheLockLeader())
                .orElse(LockLeader.I_AM_THE_LOCK_LEADER);
    }

    protected void init() {
        if (configManager.getConfig().scyllaDb() && !configManager.getConfig().safetyDisabled()) {
            throw new IllegalArgumentException("Not currently allowing Thrift-based access to ScyllaDB clusters;"
                    + " there appears to be from our tests"
                    + " an existing correctness bug with semi-complex column selections");
        }

        boolean supportsCas = !configManager.getConfig().scyllaDb()
                && clientPool.runWithRetry(CassandraVerifier.underlyingCassandraClusterSupportsCASOperations);

        schemaMutationLock = new SchemaMutationLock(
                supportsCas,
                configManager,
                clientPool,
                queryRunner,
                writeConsistency,
                schemaMutationLockTable,
                new HeartbeatService(
                        clientPool,
                        queryRunner,
                        HeartbeatService.DEFAULT_HEARTBEAT_TIME_PERIOD_MILLIS,
                        schemaMutationLockTable.getOnlyTable(),
                        writeConsistency),
                SchemaMutationLock.DEFAULT_DEAD_HEARTBEAT_TIMEOUT_THRESHOLD_MILLIS);

        createTable(AtlasDbConstants.DEFAULT_METADATA_TABLE, AtlasDbConstants.EMPTY_TABLE_METADATA);
        lowerConsistencyWhenSafe();
        upgradeFromOlderInternalSchema();
        CassandraKeyValueServices.warnUserInInitializationIfClusterAlreadyInInconsistentState(
                clientPool,
                configManager.getConfig());
    }

    private void upgradeFromOlderInternalSchema() {
        try {
            Map<TableReference, byte[]> metadataForTables = getMetadataForTables();
            Map<TableReference, byte[]> tablesToUpgrade = Maps.newHashMapWithExpectedSize(metadataForTables.size());

            List<CfDef> knownCfs = clientPool.runWithRetry(client ->
                    client.describe_keyspace(configManager.getConfig().keyspace()).getCf_defs());

            for (CfDef clusterSideCf : knownCfs) {
                TableReference tableRef = tableReferenceFromCfDef(clusterSideCf);
                if (metadataForTables.containsKey(tableRef)) {
                    byte[] clusterSideMetadata = metadataForTables.get(tableRef);
                    CfDef clientSideCf = getCfForTable(tableRef, clusterSideMetadata);
                    if (!ColumnFamilyDefinitions.isMatchingCf(clientSideCf, clusterSideCf)) {
                        // mismatch; we have changed how we generate schema since we last persisted
                        log.warn("Upgrading table {} to new internal Cassandra schema", tableRef);
                        tablesToUpgrade.put(tableRef, clusterSideMetadata);
                    }
                } else if (!hiddenTables.isHidden(tableRef)) {
                    // Possible to get here from a race condition with another service starting up
                    // and performing schema upgrades concurrent with us doing this check
                    log.error("Found a table {} that did not have persisted"
                            + " AtlasDB metadata. If you recently did a Palantir update, try waiting until"
                            + " schema upgrades are completed on all backend CLIs/services etc and restarting"
                            + " this service. If this error re-occurs on subsequent attempted startups, please"
                            + " contact Palantir support.", tableRef.getQualifiedName());
                }
            }

            // we are racing another service to do these same operations here, but they are idempotent / safe
            if (!tablesToUpgrade.isEmpty()) {
                putMetadataForTables(tablesToUpgrade);
            }
        } catch (TException e) {
            log.error("Couldn't upgrade from an older internal Cassandra schema."
                    + " New table-related settings may not have taken effect.");
        }
    }

    private void lowerConsistencyWhenSafe() {
        Set<String> dcs;
        Map<String, String> strategyOptions;
        CassandraKeyValueServiceConfig config = configManager.getConfig();

        try {
            dcs = clientPool.runWithRetry(client ->
                    CassandraVerifier.sanityCheckDatacenters(
                            client,
                            config.replicationFactor(),
                            config.safetyDisabled()));
            KsDef ksDef = clientPool.runWithRetry(client ->
                    client.describe_keyspace(config.keyspace()));
            strategyOptions = Maps.newHashMap(ksDef.getStrategy_options());

            if (dcs.size() == 1) {
                String dc = dcs.iterator().next();
                if (strategyOptions.get(dc) != null) {
                    int currentRf = Integer.parseInt(strategyOptions.get(dc));
                    if (currentRf == config.replicationFactor()) {
                        if (currentRf == 2 && config.clusterMeetsNormalConsistencyGuarantees()) {
                            log.info("Setting Read Consistency to ONE, as cluster has only one datacenter at RF2.");
                            readConsistency = ConsistencyLevel.ONE;
                        }
                    }
                }
            }
        } catch (TException e) {
            return;
        }
    }

    /**
     * Gets values from the key-value store.
     * <p>
     * Does not require all Cassandra nodes to be up and available, works as long as quorum is achieved.
     *
     * @param tableRef the name of the table to retrieve values from.
     * @param rows set containing the rows to retrieve values for.
     * @param selection specifies the set of columns to fetch.
     * @param startTs specifies the maximum timestamp (exclusive) at which to
     *        retrieve each rows's value.
     *
     * @return map of retrieved values. Values which do not exist (either
     *         because they were deleted or never created in the first place)
     *         are simply not returned.
     *
     * @throws IllegalArgumentException if any of the requests were invalid
     *         (e.g., attempting to retrieve values from a non-existent table).
     */
    @Override
    public Map<Cell, Value> getRows(
            TableReference tableRef,
            Iterable<byte[]> rows,
            ColumnSelection selection,
            long startTs) {
        if (!selection.allColumnsSelected()) {
            return getRowsForSpecificColumns(tableRef, rows, selection, startTs);
        }

        Set<Entry<InetSocketAddress, List<byte[]>>> rowsByHost = partitionByHost(rows, Functions.identity()).entrySet();
        List<Callable<Map<Cell, Value>>> tasks = Lists.newArrayListWithCapacity(rowsByHost.size());
        for (final Map.Entry<InetSocketAddress, List<byte[]>> hostAndRows : rowsByHost) {
            tasks.add(AnnotatedCallable.wrapWithThreadName(AnnotationType.PREPEND,
                    "Atlas getRows " + hostAndRows.getValue().size()
                            + " rows from " + tableRef + " on " + hostAndRows.getKey(),
                    () -> getRowsForSingleHost(hostAndRows.getKey(), tableRef, hostAndRows.getValue(), startTs)));
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
                result.putAll(clientPool.runWithRetryOnHost(host,
                        new FunctionCheckedException<Client, Map<Cell, Value>, Exception>() {
                            @Override
                            public Map<Cell, Value> apply(Client client) throws Exception {
                                // We want to get all the columns in the row so set start and end to empty.
                                SliceRange slice = new SliceRange(
                                        ByteBuffer.wrap(PtBytes.EMPTY_BYTE_ARRAY),
                                        ByteBuffer.wrap(PtBytes.EMPTY_BYTE_ARRAY),
                                        false,
                                        Integer.MAX_VALUE);
                                SlicePredicate pred = new SlicePredicate();
                                pred.setSlice_range(slice);

                                List<ByteBuffer> rowNames = wrap(batch);

                                ColumnParent colFam = new ColumnParent(internalTableName(tableRef));
                                Map<ByteBuffer, List<ColumnOrSuperColumn>> results = multigetInternal(
                                        client,
                                        tableRef,
                                        rowNames,
                                        colFam,
                                        pred,
                                        readConsistency);
                                Map<Cell, Value> ret = Maps.newHashMapWithExpectedSize(batch.size());
                                new ValueExtractor(ret).extractResults(results, startTs, ColumnSelection.all());
                                return ret;
                            }

                            @Override
                            public String toString() {
                                return "multiget_slice(" + tableRef.getQualifiedName() + ", "
                                        + batch.size() + " rows" + ")";
                            }
                        }));
            }
            if (rowCount > fetchBatchCount) {
                log.warn("Rebatched in getRows a call to {} that attempted to multiget {} rows; "
                        + "this may indicate overly-large batching on a higher level.\n{}",
                        tableRef.getQualifiedName(),
                        rowCount,
                        CassandraKeyValueServices.getFilteredStackTrace("com.palantir"));
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

        StartTsResultsCollector collector = new StartTsResultsCollector(startTs);
        loadWithTs(tableRef, cells, startTs, false, collector, readConsistency);
        return collector.getCollectedResults();
    }

    /**
     * Gets values from the key-value store.
     * <p>
     * Does not require all Cassandra nodes to be up and available, works as long as quorum is achieved.
     *
     * @param tableRef the name of the table to retrieve values from.
     * @param timestampByCell specifies, for each row, the maximum timestamp (exclusive) at which to
     *        retrieve that rows's value.
     *
     * @return map of retrieved values. Values which do not exist (either
     *         because they were deleted or never created in the first place)
     *         are simply not returned.
     *
     * @throws IllegalArgumentException if any of the requests were invalid
     *         (e.g., attempting to retrieve values from a non-existent table).
     */
    @Override
    public Map<Cell, Value> get(TableReference tableRef, Map<Cell, Long> timestampByCell) {
        if (timestampByCell.isEmpty()) {
            log.info("Attempted get on '{}' table with empty cells", UnsafeArg.of("tableRef", tableRef));
            return ImmutableMap.of();
        }

        try {
            Long firstTs = timestampByCell.values().iterator().next();
            if (Iterables.all(timestampByCell.values(), Predicates.equalTo(firstTs))) {
                StartTsResultsCollector collector = new StartTsResultsCollector(firstTs);
                loadWithTs(tableRef, timestampByCell.keySet(), firstTs, false, collector, readConsistency);
                return collector.getCollectedResults();
            }

            SetMultimap<Long, Cell> cellsByTs = Multimaps.invertFrom(
                    Multimaps.forMap(timestampByCell), HashMultimap.<Long, Cell>create());
            Builder<Cell, Value> builder = ImmutableMap.builder();
            for (long ts : cellsByTs.keySet()) {
                StartTsResultsCollector collector = new StartTsResultsCollector(ts);
                loadWithTs(tableRef, cellsByTs.get(ts), ts, false, collector, readConsistency);
                builder.putAll(collector.getCollectedResults());
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
                            ThreadSafeResultVisitor visitor,
                            ConsistencyLevel consistency) {
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
                        hostsAndCells.values().size(),
                        tableRef,
                        loadAllTs ? "for all timestamps " : "",
                        startTs,
                        hostAndCells.getKey());
            }

            tasks.addAll(getLoadWithTsTasksForSingleHost(hostAndCells.getKey(),
                    tableRef,
                    hostAndCells.getValue(),
                    startTs,
                    loadAllTs,
                    visitor,
                    consistency));
        }
        runAllTasksCancelOnFailure(tasks);
    }

    // TODO(unknown): after cassandra api change: handle different column select per row
    private List<Callable<Void>> getLoadWithTsTasksForSingleHost(final InetSocketAddress host,
                                                                 final TableReference tableRef,
                                                                 final Collection<Cell> cells,
                                                                 final long startTs,
                                                                 final boolean loadAllTs,
                                                                 final ThreadSafeResultVisitor visitor,
                                                                 final ConsistencyLevel consistency) {
        final ColumnParent colFam = new ColumnParent(internalTableName(tableRef));
        Multimap<byte[], Cell> cellsByCol =
                TreeMultimap.create(UnsignedBytes.lexicographicalComparator(), Ordering.natural());
        for (Cell cell : cells) {
            cellsByCol.put(cell.getColumnName(), cell);
        }
        List<Callable<Void>> tasks = Lists.newArrayList();
        int fetchBatchCount = configManager.getConfig().fetchBatchCount();
        for (Entry<byte[], Collection<Cell>> entry : Multimaps.asMap(cellsByCol).entrySet()) {
            final byte[] col = entry.getKey();
            Collection<Cell> columnCells = entry.getValue();
            if (columnCells.size() > fetchBatchCount) {
                log.warn("Re-batching in getLoadWithTsTasksForSingleHost a call to {} for table {} that attempted to "
                                + "multiget {} rows; this may indicate overly-large batching on a higher level.\n{}",
                        host,
                        tableRef,
                        columnCells.size(),
                        CassandraKeyValueServices.getFilteredStackTrace("com.palantir"));
            }
            for (final List<Cell> partition : Lists.partition(ImmutableList.copyOf(columnCells), fetchBatchCount)) {
                Callable<Void> multiGetCallable = () -> clientPool.runWithRetryOnHost(host,
                        new FunctionCheckedException<Client, Void, Exception>() {
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
                                            partition.size(),
                                            tableRef,
                                            loadAllTs ? "for all timestamps " : "",
                                            startTs,
                                            host);
                                }

                                Map<ByteBuffer, List<ColumnOrSuperColumn>> results =
                                        multigetInternal(client, tableRef, rowNames, colFam, predicate, consistency);
                                visitor.visit(results);
                                return null;
                            }

                            @Override
                            public String toString() {
                                return "multiget_slice(" + host + ", " + colFam + ", "
                                        + partition.size() + " cells" + ")";
                            }

                        });
                tasks.add(AnnotatedCallable.wrapWithThreadName(AnnotationType.PREPEND,
                        "Atlas loadWithTs " + partition.size() + " cells from " + tableRef + " on " + host,
                        multiGetCallable));
            }
        }
        return tasks;
    }

    /**
     * Gets values from the key-value store for the specified rows and column range
     * as separate iterators for each row.
     * <p>
     * Does not require all Cassandra nodes to be up and available, works as long as quorum is achieved.
     *
     * @param tableRef the name of the table to retrieve values from.
     * @param rows set containing the rows to retrieve values for. Behavior is undefined if {@code rows}
     *        contains duplicates (as defined by {@link java.util.Arrays#equals(byte[], byte[])}).
     * @param batchColumnRangeSelection specifies the column range and the per-row batchSize to fetch.
     * @param timestamp specifies the maximum timestamp (exclusive) at which to retrieve each rows's value.
     *
     * @return map of row names to {@link RowColumnRangeIterator}. Each {@link RowColumnRangeIterator} can iterate over
     *         the values that are spanned by the {@code batchColumnRangeSelection} in increasing order by column name.
     *
     * @throws IllegalArgumentException if {@code rows} contains duplicates.
     */
    @Override
    public Map<byte[], RowColumnRangeIterator> getRowsColumnRange(TableReference tableRef,
                                                                  Iterable<byte[]> rows,
                                                                  BatchColumnRangeSelection batchColumnRangeSelection,
                                                                  long timestamp) {
        Set<Entry<InetSocketAddress, List<byte[]>>> rowsByHost =
                partitionByHost(rows, Functions.<byte[]>identity()).entrySet();
        List<Callable<Map<byte[], RowColumnRangeIterator>>> tasks = Lists.newArrayListWithCapacity(rowsByHost.size());
        for (final Map.Entry<InetSocketAddress, List<byte[]>> hostAndRows : rowsByHost) {
            tasks.add(AnnotatedCallable.wrapWithThreadName(AnnotationType.PREPEND,
                    "Atlas getRowsColumnRange " + hostAndRows.getValue().size()
                            + " rows from " + tableRef + " on " + hostAndRows.getKey(),
                    () -> getRowsColumnRangeIteratorForSingleHost(
                            hostAndRows.getKey(),
                            tableRef,
                            hostAndRows.getValue(),
                            batchColumnRangeSelection,
                            timestamp)));
        }
        List<Map<byte[], RowColumnRangeIterator>> perHostResults = runAllTasksCancelOnFailure(tasks);
        Map<byte[], RowColumnRangeIterator> result = Maps.newHashMapWithExpectedSize(Iterables.size(rows));
        for (Map<byte[], RowColumnRangeIterator> perHostResult : perHostResults) {
            result.putAll(perHostResult);
        }
        return result;
    }

    private Map<byte[], RowColumnRangeIterator> getRowsColumnRangeIteratorForSingleHost(
            InetSocketAddress host,
            TableReference tableRef,
            List<byte[]> rows,
            BatchColumnRangeSelection batchColumnRangeSelection,
            long startTs) {
        try {
            RowColumnRangeExtractor.RowColumnRangeResult firstPage =
                    getRowsColumnRangeForSingleHost(host, tableRef, rows, batchColumnRangeSelection, startTs);

            Map<byte[], LinkedHashMap<Cell, Value>> results = firstPage.getResults();
            Map<byte[], Column> rowsToLastCompositeColumns = firstPage.getRowsToLastCompositeColumns();
            Map<byte[], byte[]> incompleteRowsToNextColumns = Maps.newHashMap();
            for (Entry<byte[], Column> e : rowsToLastCompositeColumns.entrySet()) {
                byte[] row = e.getKey();
                byte[] col = CassandraKeyValueServices.decomposeName(e.getValue()).getLhSide();
                // If we read a version of the cell before our start timestamp, it will be the most recent version
                // readable to us and we can continue to the next column. Otherwise we have to continue reading
                // this column.
                Map<Cell, Value> rowResult = results.get(row);
                boolean completedCell = (rowResult != null) && rowResult.containsKey(Cell.create(row, col));
                boolean endOfRange = isEndOfColumnRange(
                        completedCell,
                        col,
                        firstPage.getRowsToRawColumnCount().get(row),
                        batchColumnRangeSelection);
                if (!endOfRange) {
                    byte[] nextCol = getNextColumnRangeColumn(completedCell, col);
                    incompleteRowsToNextColumns.put(row, nextCol);
                }
            }

            Map<byte[], RowColumnRangeIterator> ret = Maps.newHashMapWithExpectedSize(rows.size());
            for (byte[] row : rowsToLastCompositeColumns.keySet()) {
                Iterator<Entry<Cell, Value>> resultIterator;
                Map<Cell, Value> result = results.get(row);
                if (result != null) {
                    resultIterator = result.entrySet().iterator();
                } else {
                    resultIterator = Collections.emptyIterator();
                }
                byte[] nextCol = incompleteRowsToNextColumns.get(row);
                if (nextCol == null) {
                    ret.put(row, new LocalRowColumnRangeIterator(resultIterator));
                } else {
                    BatchColumnRangeSelection newColumnRange = BatchColumnRangeSelection.create(nextCol,
                            batchColumnRangeSelection.getEndCol(), batchColumnRangeSelection.getBatchHint());
                    ret.put(row, new LocalRowColumnRangeIterator(Iterators.concat(
                            resultIterator,
                            getRowColumnRange(host, tableRef, row, newColumnRange, startTs))));
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
                                                             BatchColumnRangeSelection batchColumnRangeSelection,
                                                             long startTs) {
        try {
            return clientPool.runWithRetryOnHost(host,
                    new FunctionCheckedException<Client, RowColumnRangeExtractor.RowColumnRangeResult, Exception>() {
                        @Override
                        public RowColumnRangeExtractor.RowColumnRangeResult apply(Client client) throws Exception {
                            ByteBuffer start = batchColumnRangeSelection.getStartCol().length == 0
                                    ? ByteBuffer.wrap(PtBytes.EMPTY_BYTE_ARRAY)
                                    : CassandraKeyValueServices.makeCompositeBuffer(
                                            batchColumnRangeSelection.getStartCol(),
                                            startTs - 1);
                            ByteBuffer end = batchColumnRangeSelection.getEndCol().length == 0
                                    ? ByteBuffer.wrap(PtBytes.EMPTY_BYTE_ARRAY)
                                    : CassandraKeyValueServices.makeCompositeBuffer(
                                            RangeRequests
                                                    .previousLexicographicName(batchColumnRangeSelection.getEndCol()),
                                            -1);
                            SliceRange slice =
                                    new SliceRange(start, end, false, batchColumnRangeSelection.getBatchHint());
                            SlicePredicate pred = new SlicePredicate();
                            pred.setSlice_range(slice);

                            ColumnParent colFam = new ColumnParent(internalTableName(tableRef));
                            Map<ByteBuffer, List<ColumnOrSuperColumn>> results =
                                    multigetInternal(client, tableRef, wrap(rows), colFam, pred, readConsistency);

                            RowColumnRangeExtractor extractor = new RowColumnRangeExtractor();
                            extractor.extractResults(rows, results, startTs);

                            return extractor.getRowColumnRangeResult();
                        }

                        @Override
                        public String toString() {
                            return "multiget_slice(" + tableRef.getQualifiedName() + ", "
                                    + rows.size() + " rows, " + batchColumnRangeSelection.getBatchHint()
                                    + " max columns)";
                        }
                    });
        } catch (Exception e) {
            throw Throwables.throwUncheckedException(e);
        }
    }

    private Iterator<Entry<Cell, Value>> getRowColumnRange(
            InetSocketAddress host,
            TableReference tableRef,
            byte[] row,
            BatchColumnRangeSelection batchColumnRangeSelection,
            long startTs) {
        return ClosableIterators.wrap(new AbstractPagingIterable<
                Entry<Cell, Value>,
                TokenBackedBasicResultsPage<Entry<Cell, Value>, byte[]>>() {
            @Override
            protected TokenBackedBasicResultsPage<Entry<Cell, Value>, byte[]> getFirstPage() throws Exception {
                return page(batchColumnRangeSelection.getStartCol());
            }

            @Override
            protected TokenBackedBasicResultsPage<Entry<Cell, Value>, byte[]> getNextPage(
                    TokenBackedBasicResultsPage<Entry<Cell, Value>,
                            byte[]> previous) throws Exception {
                return page(previous.getTokenForNextPage());
            }

            TokenBackedBasicResultsPage<Entry<Cell, Value>, byte[]> page(final byte[] startCol) throws Exception {
                return clientPool.runWithRetryOnHost(host, new FunctionCheckedException<
                        Client,
                        TokenBackedBasicResultsPage<Entry<Cell, Value>, byte[]>,
                        Exception>() {
                    @Override
                    public TokenBackedBasicResultsPage<Entry<Cell, Value>, byte[]> apply(Client client)
                            throws Exception {
                        ByteBuffer start = startCol.length == 0
                                ? ByteBuffer.wrap(PtBytes.EMPTY_BYTE_ARRAY)
                                : CassandraKeyValueServices.makeCompositeBuffer(startCol, startTs - 1);
                        ByteBuffer end = batchColumnRangeSelection.getEndCol().length == 0
                                ? ByteBuffer.wrap(PtBytes.EMPTY_BYTE_ARRAY)
                                : CassandraKeyValueServices.makeCompositeBuffer(
                                        RangeRequests.previousLexicographicName(batchColumnRangeSelection.getEndCol()),
                                        -1);
                        SliceRange slice = new SliceRange(start, end, false, batchColumnRangeSelection.getBatchHint());
                        SlicePredicate pred = new SlicePredicate();
                        pred.setSlice_range(slice);

                        ByteBuffer rowByteBuffer = ByteBuffer.wrap(row);

                        ColumnParent colFam = new ColumnParent(internalTableName(tableRef));
                        Map<ByteBuffer, List<ColumnOrSuperColumn>> results = multigetInternal(client, tableRef,
                                ImmutableList.of(rowByteBuffer), colFam, pred, readConsistency);

                        if (results.isEmpty()) {
                            return SimpleTokenBackedResultsPage.create(startCol, ImmutableList.of(), false);
                        }
                        Map<Cell, Value> ret = Maps.newHashMap();
                        new ValueExtractor(ret).extractResults(results, startTs, ColumnSelection.all());
                        List<ColumnOrSuperColumn> values = Iterables.getOnlyElement(results.values());
                        if (values.isEmpty()) {
                            return SimpleTokenBackedResultsPage.create(startCol, ImmutableList.of(), false);
                        }
                        ColumnOrSuperColumn lastColumn = values.get(values.size() - 1);
                        byte[] lastCol = CassandraKeyValueServices.decomposeName(lastColumn.getColumn()).getLhSide();
                        // Same idea as the getRows case to handle seeing only newer entries of a column
                        boolean completedCell = ret.get(Cell.create(row, lastCol)) != null;
                        if (isEndOfColumnRange(completedCell, lastCol, values.size(), batchColumnRangeSelection)) {
                            return SimpleTokenBackedResultsPage.create(lastCol, ret.entrySet(), false);
                        }
                        byte[] nextCol = getNextColumnRangeColumn(completedCell, lastCol);
                        return SimpleTokenBackedResultsPage.create(nextCol, ret.entrySet(), true);
                    }

                    @Override
                    public String toString() {
                        return "multiget_slice(" + tableRef.getQualifiedName()
                                + ", single row, " + batchColumnRangeSelection.getBatchHint() + " batch hint)";
                    }
                });
            }

        }.iterator());
    }

    private boolean isEndOfColumnRange(boolean completedCell, byte[] lastCol, int numRawResults,
                                       BatchColumnRangeSelection columnRangeSelection) {
        return (numRawResults < columnRangeSelection.getBatchHint())
                || (completedCell
                    && (RangeRequests.isLastRowName(lastCol)
                        || Arrays.equals(
                            RangeRequests.nextLexicographicName(lastCol),
                            columnRangeSelection.getEndCol())));
    }

    private byte[] getNextColumnRangeColumn(boolean completedCell, byte[] lastCol) {
        if (!completedCell) {
            return lastCol;
        } else {
            return RangeRequests.nextLexicographicName(lastCol);
        }
    }

    /**
     * Gets timestamp values from the key-value store.
     * <p>
     * Does not require all Cassandra nodes to be up and available, works as long as quorum is achieved.
     *
     * @param tableRef the name of the table to retrieve values from.
     * @param timestampByCell map containing the cells to retrieve timestamps for. The map
     *        specifies, for each key, the maximum timestamp (exclusive) at which to
     *        retrieve that key's value.
     *
     * @return map of retrieved values. cells which do not exist (either
     *         because they were deleted or never created in the first place)
     *         are simply not returned.
     *
     * @throws IllegalArgumentException if any of the requests were invalid
     *         (e.g., attempting to retrieve values from a non-existent table).
     */
    @Override
    public Map<Cell, Long> getLatestTimestamps(TableReference tableRef, Map<Cell, Long> timestampByCell) {
        // TODO(unknown): optimize by only getting column name after cassandra api change
        return super.getLatestTimestamps(tableRef, timestampByCell);
    }

    /**
     * Puts values into the key-value store. This call <i>does not</i> guarantee atomicity across cells.
     * On failure, it is possible that some of the requests have succeeded (without having been rolled
     * back). Similarly, concurrent batched requests may interleave.
     * <p>
     * Does not require all Cassandra nodes to be up and available, works as long as quorum is achieved.
     *
     * @param tableRef the name of the table to put values into.
     * @param values map containing the key-value entries to put.
     * @param timestamp must be non-negative and not equal to {@link Long#MAX_VALUE}
     */
    @Override
    public void put(final TableReference tableRef, final Map<Cell, byte[]> values, final long timestamp) {
        try {
            putInternal(tableRef, KeyValueServices.toConstantTimestampValues(values.entrySet(), timestamp));
        } catch (Exception e) {
            throw Throwables.throwUncheckedException(e);
        }
    }

    /**
     * Puts values into the key-value store with individually specified timestamps. This call <i>does not</i>
     * guarantee atomicity across cells. On failure, it is possible that some of the requests have succeeded
     * (without having been rolled back). Similarly, concurrent batched requests may interleave.
     * <p>
     * Does not require all Cassandra nodes to be up and available, works as long as quorum is achieved.
     *
     * @param tableRef the name of the table to put values into.
     * @param values map containing the key-value entries to put with
     *               non-negative timestamps less than {@link Long#MAX_VALUE}.
     */
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
            tasks.add(AnnotatedCallable.wrapWithThreadName(AnnotationType.PREPEND,
                    "Atlas putInternal " + entry.getValue().size()
                            + " cell values to " + tableRef + " on " + entry.getKey(),
                    () -> {
                        putForSingleHostInternal(entry.getKey(), tableRef, entry.getValue().entrySet(), ttl);
                        return null;
                    }));
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
                        Mutation mutation = new Mutation();
                        mutation.setColumn_or_supercolumn(colOrSup);

                        ByteBuffer rowName = ByteBuffer.wrap(cell.getRowName());

                        Map<String, List<Mutation>> rowPuts = map.get(rowName);
                        if (rowPuts == null) {
                            rowPuts = Maps.newHashMap();
                            map.put(rowName, rowPuts);
                        }

                        List<Mutation> tableMutations = rowPuts.get(internalTableName(tableRef));
                        if (tableMutations == null) {
                            tableMutations = Lists.newArrayList();
                            rowPuts.put(internalTableName(tableRef), tableMutations);
                        }

                        tableMutations.add(mutation);
                    }
                    batchMutateInternal(client, tableRef, map, writeConsistency);
                }
                return null;
            }

            @Override
            public String toString() {
                return "batch_mutate(" + host + ", " + tableRef.getQualifiedName() + ", "
                        + Iterables.size(values) + " values, " + ttl + " ttl sec)";
            }
        });
    }

    /**
     * Puts values into the key-value store. This call <i>does not</i> guarantee atomicity across cells.
     * On failure, it is possible that some of the requests have succeeded (without having been rolled
     * back). Similarly, concurrent batched requests may interleave.
     * <p>
     * Overridden to batch more intelligently than the default implementation.
     * <p>
     * Does not require all Cassandra nodes to be up and available, works as long as quorum is achieved.
     *
     * @param valuesByTable map containing the key-value entries to put by table.
     * @param timestamp must be non-negative and not equal to {@link Long#MAX_VALUE}
     */
    @Override
    public void multiPut(Map<TableReference, ? extends Map<Cell, byte[]>> valuesByTable, long timestamp)
            throws KeyAlreadyExistsException {
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
            tasks.add(AnnotatedCallable.wrapWithThreadName(AnnotationType.PREPEND,
                    "Atlas multiPut of " + batch.size() + " cells into " + tableRefs + " on " + host,
                    () -> multiPutForSingleHostInternal(host, tableRefs, batch, timestamp)
            ));
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

    private Void multiPutForSingleHostInternal(final InetSocketAddress host,
                                               final Set<TableReference> tableRefs,
                                               final List<TableCellAndValue> batch,
                                               long timestamp) throws Exception {
        final Map<ByteBuffer, Map<String, List<Mutation>>> map = convertToMutations(batch, timestamp);
        return clientPool.runWithRetryOnHost(host, new FunctionCheckedException<Client, Void, Exception>() {
            @Override
            public Void apply(Client client) throws Exception {
                return batchMutateInternal(client, tableRefs, map, writeConsistency);
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
            Column col = createColumn(
                    cell,
                    Value.create(tableCellAndValue.value, timestamp),
                    CassandraConstants.NO_TTL);

            ColumnOrSuperColumn colOrSup = new ColumnOrSuperColumn();
            colOrSup.setColumn(col);
            Mutation mutation = new Mutation();
            mutation.setColumn_or_supercolumn(colOrSup);

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

            tableMutations.add(mutation);
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

    private Void batchMutateInternal(Client client,
                                     Set<TableReference> tableRefs,
                                     Map<ByteBuffer, Map<String, List<Mutation>>> map,
                                     ConsistencyLevel consistency) throws TException {
        return queryRunner.run(client, tableRefs, () -> {
            client.batch_mutate(map, consistency);
            return null;
        });
    }

    private Map<ByteBuffer, List<ColumnOrSuperColumn>> multigetInternal(
            Client client,
            TableReference tableRef,
            List<ByteBuffer> rowNames,
            ColumnParent colFam,
            SlicePredicate pred,
            ConsistencyLevel consistency) throws TException {
        return queryRunner.run(client, tableRef, () -> client.multiget_slice(rowNames, colFam, pred, consistency));
    }

    /**
     * Truncate a table in the key-value store.
     * <p>
     * This is preferred to dropping and re-adding a table, as live schema changes can
     * be a complicated topic for distributed databases.
     * <p>
     * Requires all Cassandra nodes to be up and available, otherwise throws an PalantirRuntimeException.
     *
     * @param tableRef the name of the table to truncate.
     *
     * @throws PalantirRuntimeException if not all hosts respond successfully.
     * @throws (? extends RuntimeException) if the table does not exist.
     */
    @Override
    public void truncateTable(final TableReference tableRef) {
        truncateTables(ImmutableSet.of(tableRef));
    }

    /**
     * Truncates tables in the key-value store.
     * <p>
     * This can be slightly faster than repeatedly truncating individual tables.
     * <p>
     * Requires all Cassandra nodes to be up and available, otherwise throws an PalantirRuntimeException.
     *
     * @param tablesToTruncate set od tables to truncate.
     *
     * @throws PalantirRuntimeException if not all hosts respond successfully.
     * @throws (? extends RuntimeException) if the table does not exist.
     */
    @Override
    public void truncateTables(final Set<TableReference> tablesToTruncate) {
        if (!tablesToTruncate.isEmpty()) {
            try {
                runTruncateInternal(tablesToTruncate);
            } catch (UnavailableException e) {
                throw new PalantirRuntimeException("Truncating tables requires all Cassandra nodes"
                        + " to be up and available.");
            } catch (TException e) {
                throw Throwables.throwUncheckedException(e);
            }
        }
    }

    private void runTruncateInternal(final Set<TableReference> tablesToTruncate) throws TException {
        clientPool.run(new FunctionCheckedException<Client, Void, TException>() {
            @Override
            public Void apply(Client client) throws TException {
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
    }

    private void truncateInternal(Client client, TableReference tableRef) throws TException {
        for (int tries = 1; tries <= CassandraConstants.MAX_TRUNCATION_ATTEMPTS; tries++) {
            boolean successful = true;
            try {
                queryRunner.run(client, tableRef, () -> {
                    client.truncate(internalTableName(tableRef));
                    return true;
                });
            } catch (TException e) {
                log.error("Cluster was unavailable while we attempted a truncate for table "
                        + "{}; we will try {} additional time(s).",
                        tableRef.getQualifiedName(),
                        CassandraConstants.MAX_TRUNCATION_ATTEMPTS - tries, e);
                if (CassandraConstants.MAX_TRUNCATION_ATTEMPTS - tries == 0) {
                    throw e;
                }
                successful = false;
                try {
                    Thread.sleep(new Random()
                            .nextInt((1 << (CassandraConstants.MAX_TRUNCATION_ATTEMPTS - tries)) - 1) * 1000);
                } catch (InterruptedException e1) {
                    Thread.currentThread().interrupt();
                }
            }
            if (successful) {
                break;
            }
        }
    }

    /**
     * Deletes values from the key-value store.
     * <p>
     * Requires all Cassandra nodes to be up and available, otherwise throws an PalantirRuntimeException.
     *
     * @param tableRef the name of the table to delete values from.
     * @param keys map containing the keys to delete values for.
     *
     * @throws PalantirRuntimeException if not all hosts respond successfully.
     */
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
                private int numVersions = 0;

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
                            ByteBuffer colName = CassandraKeyValueServices.makeCompositeBuffer(
                                    cellVersions.getKey().getColumnName(),
                                    ts);
                            SlicePredicate pred = new SlicePredicate();
                            pred.setColumn_names(Arrays.asList(colName));
                            Deletion del = new Deletion();
                            del.setPredicate(pred);
                            del.setTimestamp(Long.MAX_VALUE);
                            Mutation mutation = new Mutation();
                            mutation.setDeletion(del);
                            ByteBuffer rowName = ByteBuffer.wrap(cellVersions.getKey().getRowName());
                            if (!map.containsKey(rowName)) {
                                map.put(rowName, Maps.<String, List<Mutation>>newHashMap());
                            }
                            Map<String, List<Mutation>> rowPuts = map.get(rowName);
                            if (!rowPuts.containsKey(internalTableName(tableRef))) {
                                rowPuts.put(internalTableName(tableRef), Lists.<Mutation>newArrayList());
                            }
                            rowPuts.get(internalTableName(tableRef)).add(mutation);
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
                    return "delete_batch_mutate(" + host + ", " + tableRef.getQualifiedName() + ", "
                            + numVersions + " total versions of " + cellVersionsMap.size() + " keys)";
                }
            });
        } catch (UnavailableException e) {
            throw new PalantirRuntimeException("Deleting requires all Cassandra nodes to be up and available.");
        } catch (Exception e) {
            throw Throwables.throwUncheckedException(e);
        }
    }

    protected CfDef getCfForTable(TableReference tableRef, byte[] rawMetadata) {
        return ColumnFamilyDefinitions.getCfDef(configManager.getConfig().keyspace(), tableRef, rawMetadata);
    }

    // TODO(unknown): after cassandra change: handle multiRanges
    @Override
    @Idempotent
    public Map<RangeRequest, TokenBackedBasicResultsPage<RowResult<Value>, byte[]>> getFirstBatchForRanges(
            TableReference tableRef,
            Iterable<RangeRequest> rangeRequests,
            long timestamp) {
        int concurrency = configManager.getConfig().rangesConcurrency();
        return KeyValueServices.getFirstBatchForRangesUsingGetRangeConcurrent(
                executor, this, tableRef, rangeRequests, timestamp, concurrency);
    }


    // TODO(unknown): after cassandra change: handle reverse ranges
    // TODO(unknown): after cassandra change: handle column filtering
    /**
     * For each row in the specified range, returns the most recent version strictly before timestamp.
     * <p>
     * Does not require all Cassandra nodes to be up and available, works as long as quorum is achieved.
     *
     * Remember to close any {@link ClosableIterator}s you get in a finally block.
     *
     * @param rangeRequest the range to load.
     * @param timestamp specifies the maximum timestamp (exclusive) at which to retrieve each row's value.
     */
    @Override
    @Idempotent
    public ClosableIterator<RowResult<Value>> getRange(
            TableReference tableRef,
            RangeRequest rangeRequest,
            long timestamp) {
        return getRangeWithPageCreator(tableRef, rangeRequest, timestamp, readConsistency, ValueExtractor::create);
    }

    /**
     * Gets timestamp values from the key-value store. For each row, this returns all associated
     * timestamps &lt; given_ts.
     * <p>
     * This method has stronger consistency guarantees than regular read requests. This must return all timestamps
     * stored anywhere in the system (because of sweep). Unless all nodes are up and available, this method will
     * throw an InsufficientConsistencyException.
     *
     * @param tableRef the name of the table to read from.
     * @param rangeRequest the range to load.
     * @param timestamp the maximum timestamp to load.
     *
     * @throws InsufficientConsistencyException if not all hosts respond successfully.
     */
    @Override
    @Idempotent
    public ClosableIterator<RowResult<Set<Long>>> getRangeOfTimestamps(
            TableReference tableRef,
            RangeRequest rangeRequest,
            long timestamp) {
        Optional<Integer> timestampsGetterBatchSize = configManager.getConfig().timestampsGetterBatchSize();
        if (timestampsGetterBatchSize.isPresent()) {
            return getTimestampsInBatchesWithPageCreator(
                    tableRef,
                    rangeRequest,
                    timestampsGetterBatchSize.get(),
                    timestamp,
                    deleteConsistency);
        } else {
            return getRangeWithPageCreator(
                    tableRef,
                    rangeRequest,
                    timestamp,
                    deleteConsistency,
                    TimestampExtractor::new);
        }
    }

    @Override
    public ClosableIterator<List<CandidateCellForSweeping>> getCandidateCellsForSweeping(TableReference tableRef,
            CandidateCellForSweepingRequest request) {
        return new GetCandidateCellsForSweepingShim(this).getCandidateCellsForSweeping(tableRef, request);
    }

    private ClosableIterator<RowResult<Set<Long>>> getTimestampsInBatchesWithPageCreator(
            TableReference tableRef,
            RangeRequest rangeRequest,
            int columnBatchSize,
            long timestamp,
            ConsistencyLevel consistency) {
        RowGetter rowGetter = new RowGetter(clientPool, queryRunner, consistency, tableRef, ColumnFetchMode.FETCH_ONE);

        CqlExecutor cqlExecutor = new CqlExecutor(clientPool, consistency);
        ColumnGetter columnGetter = new CqlColumnGetter(cqlExecutor, tableRef, columnBatchSize);

        return getRangeWithPageCreator(rowGetter, columnGetter, rangeRequest, TimestampExtractor::new, timestamp);
    }

    private <T> ClosableIterator<RowResult<T>> getRangeWithPageCreator(
            TableReference tableRef,
            RangeRequest rangeRequest,
            long timestamp,
            ConsistencyLevel consistency,
            Supplier<ResultsExtractor<T>> resultsExtractor) {
        RowGetter rowGetter = new RowGetter(clientPool, queryRunner, consistency, tableRef, ColumnFetchMode.FETCH_ALL);
        ColumnGetter columnGetter = new ThriftColumnGetter();

        return getRangeWithPageCreator(rowGetter, columnGetter, rangeRequest, resultsExtractor, timestamp);
    }

    private <T> ClosableIterator<RowResult<T>> getRangeWithPageCreator(
            RowGetter rowGetter,
            ColumnGetter columnGetter,
            RangeRequest rangeRequest,
            Supplier<ResultsExtractor<T>> resultsExtractor,
            long timestamp) {
        if (rangeRequest.isReverse()) {
            throw new UnsupportedOperationException();
        }
        if (rangeRequest.isEmptyRange()) {
            return ClosableIterators.wrap(ImmutableList.<RowResult<T>>of().iterator());
        }

        CassandraRangePagingIterable<T> rowResults = new CassandraRangePagingIterable<>(
                rowGetter,
                columnGetter,
                rangeRequest,
                resultsExtractor,
                timestamp
        );

        return ClosableIterators.wrap(rowResults.iterator());
    }

    /**
     * Drop the table, and also delete its table metadata.
     * <p>
     * Requires all Cassandra nodes to be up and available, otherwise throws an IllegalStateException. If a
     * quorum of Cassandra nodes are up, the table will be dropped in the KVS before the exception is thrown, but
     * the metadata table will not be updated.
     *
     * @param tableRef the name of the table to drop.
     *
     * @throws IllegalStateException if not all hosts respond successfully, or if their schema versions do
     * not come to agreement in 1 minute.
     */
    @Override
    public void dropTable(final TableReference tableRef) {
        dropTables(ImmutableSet.of(tableRef));
    }

    /**
     * Drop the tables, and also delete their table metadata.
     * <p>
     * Requires all Cassandra nodes to be up and available, otherwise throws an IllegalStateException. If a
     * quorum of Cassandra nodes are up, the tables will be dropped in the KVS before the exception is thrown, but
     * the metadata table will not be updated.
     * <p>
     * Main gains here vs. dropTable:
     *    - problems excepting, we will basically be serializing a rapid series of schema changes
     *      through a single host checked out from the client pool, so reduced chance of schema disagreement issues
     *    - client-side in-memory lock to prevent misbehaving callers from shooting themselves in the foot
     *    - one less round trip
     *
     * @param tablesToDrop the set of tables to drop.
     *
     * @throws IllegalStateException if not all hosts respond successfully, or if their schema versions do
     * not come to agreement in 1 minute.
     */
    @Override
    public void dropTables(final Set<TableReference> tablesToDrop) {
        schemaMutationLock.runWithLock(() -> dropTablesInternal(tablesToDrop));
    }

    private void dropTablesInternal(final Set<TableReference> tablesToDrop) throws Exception {
        try {
            clientPool.runWithRetry((FunctionCheckedException<Client, Void, Exception>) client -> {
                KsDef ks = client.describe_keyspace(configManager.getConfig().keyspace());
                Set<TableReference> existingTables = Sets.newHashSet();

                existingTables.addAll(ks.getCf_defs().stream()
                        .map(this::tableReferenceFromCfDef)
                        .collect(Collectors.toList()));

                for (TableReference table : tablesToDrop) {
                    CassandraVerifier.sanityCheckTableName(table);
                    if (existingTables.contains(table)) {
                        client.system_drop_column_family(internalTableName(table));
                        putMetadataWithoutChangingSettings(table, PtBytes.EMPTY_BYTE_ARRAY);
                    } else {
                        log.warn("Ignored call to drop a table ({}) that did not exist.", table);
                    }
                }
                CassandraKeyValueServices.waitForSchemaVersions(
                        client,
                        "(all tables in a call to dropTables)",
                        configManager.getConfig().schemaMutationTimeoutMillis());
                return null;
            });
        } catch (UnavailableException e) {
            throw new PalantirRuntimeException("Dropping tables requires all Cassandra nodes to be up and available.");
        }
    }

    private TableReference tableReferenceFromCfDef(CfDef cf) {
        return fromInternalTableName(cf.getName());
    }

    /**
     * Creates a table with the specified name. If the table already exists, no action is performed
     * (the table is left in its current state).
     * <p>
     * Requires all Cassandra nodes to be up and available, otherwise throws an IllegalStateException. If a
     * quorum of Cassandra nodes are up, the table will be created in the KVS before the exception is thrown, but
     * the metadata table will not be updated.
     *
     * @param tableRef the name of the table to create.
     * @param tableMetadata the metadata of the table to create.
     *
     * @throws IllegalStateException if not all hosts respond successfully.
     */
    @Override
    public void createTable(final TableReference tableRef, final byte[] tableMetadata) {
        createTables(ImmutableMap.of(tableRef, tableMetadata));
    }

    /**
     * Creates a table with the specified name. If the table already exists, no action is performed
     * (the table is left in its current state).
     * <p>
     * Requires all Cassandra nodes to be up and available, otherwise throws an IllegalStateException. If a
     * quorum of Cassandra nodes are up, the table will be created in the KVS before the exception is thrown, but
     * the metadata table will not be updated.
     * <p>
     * Main gains here vs. createTable:
     *    - problems excepting, we will basically be serializing a rapid series of schema changes
     *      through a single host checked out from the client pool, so reduced chance of schema disagreement issues
     *    - client-side in-memory lock to prevent misbehaving callers from shooting themselves in the foot
     *    - one less round trip
     * <p>
     * createTables(existingTable, newMetadata) can perform a metadata-only update. Additionally, it is possible
     * that this metadata-only update performs a schema mutation by altering the CFDef (e. g., user changes metadata
     * of existing table to have new compression block size). This does not require the schema mutation lock, as it
     * does not alter the CfId
     *
     * @param tableNamesToTableMetadata a mapping of names of tables to create to their respective metadata.
     *
     * @throws IllegalStateException if not all hosts respond successfully.
     */
    @Override
    public void createTables(final Map<TableReference, byte[]> tableNamesToTableMetadata) {
        Map<TableReference, byte[]> tablesToActuallyCreate = filterOutExistingTables(tableNamesToTableMetadata);
        Map<TableReference, byte[]> tablesToUpdateMetadataFor = filterOutNoOpMetadataChanges(tableNamesToTableMetadata);

        boolean onlyMetadataChangesAreForNewTables =
                tablesToUpdateMetadataFor.keySet().equals(tablesToActuallyCreate.keySet());
        boolean putMetadataWillNeedASchemaChange = !onlyMetadataChangesAreForNewTables;

        if (!tablesToActuallyCreate.isEmpty()) {
            schemaMutationLock.runWithLock(() -> {
                createTablesInternal(tablesToActuallyCreate);
            });
        }
        internalPutMetadataForTables(tablesToUpdateMetadataFor, putMetadataWillNeedASchemaChange);
    }

    private Map<TableReference, byte[]> filterOutNoOpMetadataChanges(
            final Map<TableReference, byte[]> tableNamesToTableMetadata) {
        Map<TableReference, byte[]> existingTableMetadata = getMetadataForTables();
        Map<TableReference, byte[]> tableMetadataUpdates = Maps.newHashMap();

        for (Entry<TableReference, byte[]> entry : tableNamesToTableMetadata.entrySet()) {
            TableReference tableReference = entry.getKey();
            byte[] newMetadata = entry.getValue();

            // if no existing table or if existing table's metadata is different
            if (!Arrays.equals(existingTableMetadata.get(tableReference), newMetadata)) {
                Set<TableReference> matchingTables = Sets.filter(existingTableMetadata.keySet(), existingTableRef ->
                        existingTableRef.getQualifiedName().equalsIgnoreCase(tableReference.getQualifiedName()));

                // completely new table, not an update
                if (matchingTables.isEmpty()) {
                    tableMetadataUpdates.put(tableReference, newMetadata);
                } else { // existing case-insensitive table, maybe an update
                    if (Arrays.equals(
                            existingTableMetadata.get(Iterables.getOnlyElement(matchingTables)), newMetadata)) {
                        log.debug("Case-insensitive matched table already existed with same metadata,"
                                + " skipping update to {}", tableReference);
                    } else { // existing table has different metadata, so we should perform an update
                        tableMetadataUpdates.put(tableReference, newMetadata);
                    }
                }
            } else {
                log.debug("Table already existed with same metadata, skipping update to {}", tableReference);
            }
        }

        return tableMetadataUpdates;
    }

    private Map<TableReference, byte[]> filterOutExistingTables(
            final Map<TableReference, byte[]> tableNamesToTableMetadata) {
        Map<TableReference, byte[]> filteredTables = Maps.newHashMap();
        try {
            Set<TableReference> existingTablesLowerCased = cassandraTables.getExistingLowerCased().stream()
                    .map(AbstractKeyValueService::fromInternalTableName)
                    .collect(Collectors.toSet());

            for (Entry<TableReference, byte[]> tableAndMetadataPair : tableNamesToTableMetadata.entrySet()) {
                TableReference table = tableAndMetadataPair.getKey();
                byte[] metadata = tableAndMetadataPair.getValue();

                CassandraVerifier.sanityCheckTableName(table);

                TableReference tableRefLowerCased = TableReference.createLowerCased(table);
                if (!existingTablesLowerCased.contains(tableRefLowerCased)) {
                    filteredTables.put(table, metadata);
                } else {
                    log.debug("Filtering out existing table ({}) that already existed (case insensitive).", table);
                }
            }
        } catch (Exception e) {
            Throwables.throwUncheckedException(e);
        }

        return filteredTables;
    }

    private void createTablesInternal(final Map<TableReference, byte[]> tableNamesToTableMetadata) throws Exception {
        clientPool.runWithRetry(client -> {
            for (Entry<TableReference, byte[]> tableEntry : tableNamesToTableMetadata.entrySet()) {
                try {
                    client.system_add_column_family(ColumnFamilyDefinitions.getCfDef(
                            configManager.getConfig().keyspace(),
                            tableEntry.getKey(),
                            tableEntry.getValue()));
                } catch (UnavailableException e) {
                    throw new PalantirRuntimeException(
                            "Creating tables requires all Cassandra nodes to be up and available.");
                } catch (TException thriftException) {
                    if (thriftException.getMessage() != null
                            && !thriftException.getMessage().contains("already existing table")) {
                        Throwables.throwUncheckedException(thriftException);
                    }
                }
            }

            CassandraKeyValueServices.waitForSchemaVersions(
                    client,
                    "(a call to createTables, filtered down to create: " + tableNamesToTableMetadata.keySet() + ")",
                    configManager.getConfig().schemaMutationTimeoutMillis());
            return null;
        });
    }

    /**
     * Return the list of tables stored in this key value service.
     * <p>
     * Does not require all Cassandra nodes to be up and available, works as long as quorum is achieved.
     * <p>
     * This will not contain the names of any hidden tables (e. g., the _metadata table).
     *
     * @return a set of TableReferences (table names) for all the visible tables
     */
    @Override
    public Set<TableReference> getAllTableNames() {
        return getTableReferencesWithoutFiltering()
                .filter(tr -> !hiddenTables.isHidden(tr))
                .collect(Collectors.toSet());
    }

    /**
     * Gets the metadata for a given table. Also useful for checking to see if a table exists.
     * <p>
     * Does not require all Cassandra nodes to be up and available, works as long as quorum is achieved.
     *
     * @param tableRef the name of the table to get metadatafor.
     *
     * @return a byte array representing the metadata for the table. Array is empty if no table
     * with the given name exists. Consider {@link TableMetadata#BYTES_HYDRATOR} for hydrating.
     */
    @Override
    public byte[] getMetadataForTable(TableReference tableRef) {
        // This can be turned into not-a-full-table-scan if someone makes an upgrade task
        // that makes sure we only write the metadata keys based on lowercased table names

        java.util.Optional<Entry<TableReference, byte[]>> match =
                getMetadataForTables().entrySet().stream().filter(
                        entry -> matchingIgnoreCase(entry.getKey(), tableRef))
                .findFirst();

        if (!match.isPresent()) {
            log.debug("Couldn't find table metadata for {}", tableRef);
            return AtlasDbConstants.EMPTY_TABLE_METADATA;
        } else {
            log.debug("Found table metadata for {} at matching name {}", tableRef, match.get().getKey());
            return match.get().getValue();
        }
    }

    private boolean matchingIgnoreCase(TableReference t1, TableReference t2) {
        if (t1 != null) {
            return t1.getQualifiedName().toLowerCase().equals(t2.getQualifiedName().toLowerCase());
        } else {
            if (t2 == null) {
                return true;
            }
            return false;
        }
    }

    /**
     * Gets the metadata for all non-hidden tables.
     * <p>
     * Does not require all Cassandra nodes to be up and available, works as long as quorum is achieved.
     *
     * @return a mapping of table names to their respective metadata in form of a byte array.  Consider
     * {@link TableMetadata#BYTES_HYDRATOR} for hydrating.
     */
    @Override
    public Map<TableReference, byte[]> getMetadataForTables() {
        Map<TableReference, byte[]> tableToMetadataContents = Maps.newHashMap();

        // we don't even have a metadata table yet. Return empty map.
        if (!getAllTableReferencesWithoutFiltering().contains(AtlasDbConstants.DEFAULT_METADATA_TABLE)) {
            log.trace("getMetadata called with no _metadata table present");
            return tableToMetadataContents;
        }

        try (ClosableIterator<RowResult<Value>> range =
                getRange(AtlasDbConstants.DEFAULT_METADATA_TABLE, RangeRequest.all(), Long.MAX_VALUE)) {
            while (range.hasNext()) {
                RowResult<Value> valueRow = range.next();
                Iterable<Entry<Cell, Value>> cells = valueRow.getCells();

                for (Entry<Cell, Value> entry : cells) {
                    Value value = entry.getValue();
                    TableReference tableRef = tableReferenceFromBytes(entry.getKey().getRowName());
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
        }
        return tableToMetadataContents;
    }

    private Set<TableReference> getAllTableReferencesWithoutFiltering() {
        return getTableReferencesWithoutFiltering()
                .collect(Collectors.toSet());
    }

    private Stream<TableReference> getTableReferencesWithoutFiltering() {
        return cassandraTables.getExisting().stream()
                .map(AbstractKeyValueService::fromInternalTableName);
    }

    private static Cell getMetadataCell(TableReference tableRef) {
        // would have preferred an explicit charset, but thrift uses default internally
        return Cell.create(
                tableReferenceToBytes(tableRef),
                "m".getBytes(StandardCharsets.UTF_8));
    }

    @SuppressWarnings("checkstyle:RegexpSinglelineJava")
    private static TableReference tableReferenceFromBytes(byte[] name) {
        return TableReference.createUnsafe(new String(name, Charset.defaultCharset()));
    }

    @SuppressWarnings("checkstyle:RegexpSinglelineJava")
    private static byte[] tableReferenceToBytes(TableReference tableRef) {
        return tableRef.getQualifiedName().getBytes(Charset.defaultCharset());
    }

    /**
     * Records the specified metadata for a given table.
     * <p>
     * Requires all Cassandra nodes to be up and available, otherwise throws an IllegalStateException.
     *
     * @param tableRef the name of the table to record metadata for.
     * @param meta a byte array representing the metadata to record.
     *
     * @throws IllegalStateException if not all hosts respond successfully.
     */
    @Override
    public void putMetadataForTable(final TableReference tableRef, final byte[] meta) {
        putMetadataForTables(ImmutableMap.of(tableRef, meta));
    }

    /**
     * For each specified table records the respective metadata.
     * <p>
     * Requires all Cassandra nodes to be up and available, otherwise throws an IllegalStateException.
     *
     * @param tableRefToMetadata a mapping from each table's name to the respective byte array representing
     * the metadata to record.
     *
     * @throws IllegalStateException if not all hosts respond successfully.
     */
    @Override
    public void putMetadataForTables(final Map<TableReference, byte[]> tableRefToMetadata) {
        internalPutMetadataForTables(tableRefToMetadata, true);
    }

    @SuppressWarnings("checkstyle:RegexpSinglelineJava")
    private void internalPutMetadataForTables(
            Map<TableReference, byte[]> unfilteredTableNameToMetadata,
            boolean possiblyNeedToPerformSettingsChanges) {
        Map<TableReference, byte[]> tableNameToMetadata = Maps.filterValues(
                unfilteredTableNameToMetadata,
                Predicates.not(Predicates.equalTo(AtlasDbConstants.EMPTY_TABLE_METADATA)));
        if (tableNameToMetadata.isEmpty()) {
            return;
        }

        Map<Cell, byte[]> metadataRequestedForUpdate = Maps.newHashMapWithExpectedSize(tableNameToMetadata.size());
        for (Entry<TableReference, byte[]> tableEntry : tableNameToMetadata.entrySet()) {
            metadataRequestedForUpdate.put(getMetadataCell(tableEntry.getKey()), tableEntry.getValue());
        }

        Map<Cell, Long> requestForLatestDbSideMetadata = Maps.transformValues(
                metadataRequestedForUpdate,
                Functions.constant(Long.MAX_VALUE));

        // technically we're racing other services from here on, during an update period,
        // but the penalty for not caring is just some superfluous schema mutations and a
        // few dead rows in the metadata table.
        Map<Cell, Value> persistedMetadata = get(
                AtlasDbConstants.DEFAULT_METADATA_TABLE,
                requestForLatestDbSideMetadata);
        final Map<Cell, byte[]> newMetadata = Maps.newHashMap();
        final Collection<CfDef> updatedCfs = Lists.newArrayList();
        for (Entry<Cell, byte[]> entry : metadataRequestedForUpdate.entrySet()) {
            Value val = persistedMetadata.get(entry.getKey());
            if (val == null || !Arrays.equals(val.getContents(), entry.getValue())) {
                newMetadata.put(entry.getKey(), entry.getValue());
                updatedCfs.add(getCfForTable(
                        tableReferenceFromBytes(entry.getKey().getRowName()),
                        entry.getValue()));
            }
        }

        if (newMetadata.isEmpty()) {
            return;
        }

        try {
            putMetadataAndMaybeAlterTables(possiblyNeedToPerformSettingsChanges, newMetadata, updatedCfs);
        } catch (Exception e) {
            throw Throwables.throwUncheckedException(e);
        }
    }

    private void putMetadataAndMaybeAlterTables(
            boolean possiblyNeedToPerformSettingsChanges,
            Map<Cell, byte[]> newMetadata,
            Collection<CfDef> updatedCfs) throws Exception {
        clientPool.runWithRetry(client -> {
            if (possiblyNeedToPerformSettingsChanges) {
                for (CfDef cf : updatedCfs) {
                    client.system_update_column_family(cf);
                }

                CassandraKeyValueServices.waitForSchemaVersions(
                        client,
                        "(all tables in a call to putMetadataForTables)",
                        configManager.getConfig().schemaMutationTimeoutMillis());
            }
            // Done with actual schema mutation, push the metadata
            put(AtlasDbConstants.DEFAULT_METADATA_TABLE, newMetadata, System.currentTimeMillis());
            return null;
        });
    }

    private void putMetadataWithoutChangingSettings(final TableReference tableRef, final byte[] meta) {
        long ts = System.currentTimeMillis();

        Multimap<Cell, Long> oldVersions = getAllTimestamps(
                AtlasDbConstants.DEFAULT_METADATA_TABLE,
                ImmutableSet.of(getMetadataCell(tableRef)),
                ts);

        put(AtlasDbConstants.DEFAULT_METADATA_TABLE,
                ImmutableMap.of(getMetadataCell(tableRef), meta),
                ts);

        delete(AtlasDbConstants.DEFAULT_METADATA_TABLE, oldVersions);
    }


    @Override
    public void deleteRange(final TableReference tableRef, final RangeRequest range) {
        if (range.equals(RangeRequest.all())) {
            try {
                runTruncateInternal(ImmutableSet.of(tableRef));
            } catch (TException e) {
                log.info("Tried to make a deleteRange({}, RangeRequest.all())"
                        + " into a more garbage-cleanup friendly truncate(), but this failed.", tableRef, e);

                super.deleteRange(tableRef, range);
            }
        } else {
            super.deleteRange(tableRef, range);
        }
    }

    /**
     * Performs non-destructive cleanup when the KVS is no longer needed.
     */
    @Override
    public void close() {
        clientPool.shutdown();
        if (compactionManager.isPresent()) {
            compactionManager.get().close();
        }
        super.close();
    }


    /**
     * Adds a value with timestamp = Value.INVALID_VALUE_TIMESTAMP to each of the given cells. If
     * a value already exists at that time stamp, nothing is written for that cell.
     * <p>
     * Does not require all Cassandra nodes to be up and available, works as long as quorum is achieved.
     *
     * @param tableRef the name of the table to add the value to.
     * @param cells a set of cells to store the values in.
     */
    @Override
    public void addGarbageCollectionSentinelValues(TableReference tableRef, Iterable<Cell> cells) {
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

    /**
     * Gets timestamp values from the key-value store. For each cell, this returns all associated
     * timestamps &lt; given_ts.
     * <p>
     * This method has stronger consistency guarantees than regular read requests. This must return
     * all timestamps stored anywhere in the system (because of sweep). Unless all nodes are up and available, this
     * method will throw a PalantirRuntimeException.
     *
     * @param tableRef the name of the table to retrieve timestamps from.
     * @param cells set containg cells to retrieve timestamps for.
     * @param ts maximum timestamp to get (exclusive).
     * @return multimap of timestamps by cell
     *
     * @throws PalantirRuntimeException if not all hosts respond successfully.
     */
    @Override
    public Multimap<Cell, Long> getAllTimestamps(TableReference tableRef, Set<Cell> cells, long ts) {
        AllTimestampsCollector collector = new AllTimestampsCollector();
        loadWithTs(tableRef, cells, ts, true, collector, deleteConsistency);
        return collector.getCollectedResults();
    }

    /**
     * Puts values into the key-value store. This call <i>does not</i> guarantee
     * atomicity across cells. On failure, it is possible that some of the requests will
     * have succeeded (without having been rolled back). Similarly, concurrent batched requests may
     * interleave.  However, concurrent writes to the same Cell will not both report success.
     * One of them will throw {@link KeyAlreadyExistsException}.
     * <p>
     * Does not require all Cassandra nodes to be up and available, works as long as quorum is achieved.
     *
     * @param tableRef the name of the table to put values into.
     * @param values map containing the key-value entries to put.
     *
     * @throws KeyAlreadyExistsException If you are putting a Cell with the same timestamp as one that already exists.
     */
    @Override
    public void putUnlessExists(final TableReference tableRef, final Map<Cell, byte[]> values)
            throws KeyAlreadyExistsException {
        try {
            clientPool.runWithRetry(client -> {
                for (Entry<Cell, byte[]> e : values.entrySet()) {
                    CheckAndSetRequest request = CheckAndSetRequest.newCell(tableRef, e.getKey(), e.getValue());
                    CASResult casResult = executeCheckAndSet(client, request);
                    if (!casResult.isSuccess()) {
                        throw new KeyAlreadyExistsException(
                                String.format("The row in table %s already exists.", tableRef.getQualifiedName()),
                                ImmutableList.of(e.getKey()));
                    }
                }
                return null;
            });
        } catch (Exception e) {
            throw Throwables.throwUncheckedException(e);
        }
    }

    /**
     * Performs a check-and-set into the key-value store.
     * Please see {@link CheckAndSetRequest} for information about how to create this request,
     * and {@link com.palantir.atlasdb.keyvalue.api.KeyValueService} for more detailed documentation.
     * <p>
     * Does not require all Cassandra nodes to be up and available, works as long as quorum is achieved.
     *
     * @param request the request, including table, cell, old value and new value.
     * @throws CheckAndSetException if the stored value for the cell was not as expected.
     */
    @Override
    public void checkAndSet(final CheckAndSetRequest request) throws CheckAndSetException {
        try {
            clientPool.runWithRetry(client -> {
                CASResult casResult = executeCheckAndSet(client, request);

                if (!casResult.isSuccess()) {
                    List<byte[]> currentValues = casResult.current_values.stream()
                            .map(Column::getValue)
                            .collect(Collectors.toList());

                    throw new CheckAndSetException(
                            request.cell(),
                            request.table(),
                            request.oldValue().orElse(null),
                            currentValues);
                }
                return null;
            });
        } catch (TException e) {
            throw Throwables.throwUncheckedException(e);
        }
    }

    private CASResult executeCheckAndSet(Client client, CheckAndSetRequest request)
            throws TException {
        TableReference table = request.table();
        Cell cell = request.cell();
        long timestamp = AtlasDbConstants.TRANSACTION_TS;

        ByteBuffer rowName = ByteBuffer.wrap(cell.getRowName());
        byte[] colName = CassandraKeyValueServices
                .makeCompositeBuffer(cell.getColumnName(), timestamp)
                .array();

        List<Column> oldColumns;
        java.util.Optional<byte[]> oldValue = request.oldValue();
        if (oldValue.isPresent()) {
            oldColumns = ImmutableList.of(makeColumn(colName, oldValue.get(), timestamp));
        } else {
            oldColumns = ImmutableList.of();
        }

        Column newColumn = makeColumn(colName, request.newValue(), timestamp);
        return queryRunner.run(client, table, () -> client.cas(
                rowName,
                internalTableName(table),
                oldColumns,
                ImmutableList.of(newColumn),
                ConsistencyLevel.SERIAL,
                writeConsistency));
    }

    private Column makeColumn(byte[] colName, byte[] contents, long timestamp) {
        Column newColumn = new Column();
        newColumn.setName(colName);
        newColumn.setValue(contents);
        newColumn.setTimestamp(timestamp);
        return newColumn;
    }

    /**
     * Does whatever can be done to compact or cleanup a table. Intended to be called after many
     * deletions are performed.
     *
     * @param tableRef the name of the table to compact.
     */
    @Override
    public void compactInternally(TableReference tableRef) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(tableRef.getQualifiedName()),
                "tableRef:[%s] should not be null or empty.", tableRef);
        CassandraKeyValueServiceConfig config = configManager.getConfig();
        if (!compactionManager.isPresent()) {
            log.error("No compaction client was configured, but compact was called."
                    + " If you actually want to clear deleted data immediately"
                    + " from Cassandra, lower your gc_grace_seconds setting and"
                    + " run `nodetool compact {} {}`.", config.keyspace(), internalTableName(tableRef));
            return;
        }
        long timeoutInSeconds = config.jmx().get().compactionTimeoutSeconds();
        String keyspace = config.keyspace();
        try {
            alterGcAndTombstone(keyspace, tableRef, 0, 0.0f);
            compactionManager.get().performTombstoneCompaction(timeoutInSeconds, keyspace, tableRef);
        } catch (TimeoutException e) {
            log.error("Compaction for {}.{} could not finish in {} seconds.", keyspace, tableRef, timeoutInSeconds, e);
            log.error("Compaction status: {}", compactionManager.get().getCompactionStatus());
        } catch (InterruptedException e) {
            log.error("Compaction for {}.{} was interrupted.", keyspace, tableRef);
        } finally {
            alterGcAndTombstone(
                    keyspace,
                    tableRef,
                    CassandraConstants.GC_GRACE_SECONDS,
                    CassandraConstants.TOMBSTONE_THRESHOLD_RATIO);
        }
    }

    @Override
    public ClusterAvailabilityStatus getClusterAvailabilityStatus() {
        ClusterAvailabilityStatus clusterStatus = getStatusByRunningOperationsOnEachHost();
        if (isClusterQuorumAvaialble(clusterStatus) && !doesConfigReplicationFactorMatchWithCluster()) {
            return ClusterAvailabilityStatus.TERMINAL;
        }
        return clusterStatus;
    }

    private boolean isClusterQuorumAvaialble(ClusterAvailabilityStatus clusterStatus) {
        return clusterStatus.equals(ClusterAvailabilityStatus.ALL_AVAILABLE)
                || clusterStatus.equals(ClusterAvailabilityStatus.QUORUM_AVAILABLE);
    }

    private boolean doesConfigReplicationFactorMatchWithCluster() {
        return clientPool.run(client -> {
            try {
                CassandraVerifier.currentRfOnKeyspaceMatchesDesiredRf(client, configManager.getConfig(), false);
                return true;
            } catch (Exception e) {
                log.warn("The config and Cassandra cluster do not agree on the replication factor.", e);
                return false;
            }
        });
    }

    private ClusterAvailabilityStatus getStatusByRunningOperationsOnEachHost() {
        int countUnreachableNodes = 0;
        for (InetSocketAddress host : clientPool.currentPools.keySet()) {
            try {
                clientPool.runOnHost(host, CassandraVerifier.healthCheck);
                if (!partitionerIsValid(host)) {
                    return ClusterAvailabilityStatus.TERMINAL;
                }
            } catch (Exception e) {
                countUnreachableNodes++;
            }
        }
        return getNodeAvailabilityStatus(countUnreachableNodes);
    }

    private boolean partitionerIsValid(InetSocketAddress host) {
        try {
            clientPool.runOnHost(host, clientPool.validatePartitioner);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    private ClusterAvailabilityStatus getNodeAvailabilityStatus(int countUnreachableNodes) {
        if (countUnreachableNodes == 0) {
            return ClusterAvailabilityStatus.ALL_AVAILABLE;
        } else if (isQuorumAvailable(countUnreachableNodes)) {
            return ClusterAvailabilityStatus.QUORUM_AVAILABLE;
        } else {
            return ClusterAvailabilityStatus.NO_QUORUM_AVAILABLE;
        }
    }

    private boolean isQuorumAvailable(int countUnreachableNodes) {
        int replicationFactor = configManager.getConfig().replicationFactor();
        return countUnreachableNodes < (replicationFactor + 1) / 2;
    }

    private void alterGcAndTombstone(
            String keyspace,
            TableReference tableRef,
            int gcGraceSeconds,
            float tombstoneThresholdRatio) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(keyspace),
                "keyspace:[%s] should not be null or empty.", keyspace);
        Preconditions.checkArgument(!Strings.isNullOrEmpty(tableRef.getQualifiedName()),
                "tableRef:[%s] should not be null or empty.", tableRef);
        Preconditions.checkArgument(gcGraceSeconds >= 0,
                "gc_grace_seconds:[%s] should not be negative.", gcGraceSeconds);
        Preconditions.checkArgument(tombstoneThresholdRatio >= 0.0f && tombstoneThresholdRatio <= 1.0f,
                "tombstone_threshold_ratio:[%s] should be between [0.0, 1.0]", tombstoneThresholdRatio);

        schemaMutationLock.runWithLock(() ->
                alterGcAndTombstoneInternal(keyspace, tableRef, gcGraceSeconds, tombstoneThresholdRatio));
    }

    private void alterGcAndTombstoneInternal(
            String keyspace,
            TableReference tableRef,
            int gcGraceSeconds,
            float tombstoneThresholdRatio) {
        try {
            clientPool.runWithRetry((FunctionCheckedException<Client, Void, Exception>) client -> {
                KsDef ks = client.describe_keyspace(keyspace);
                List<CfDef> cfs = ks.getCf_defs();
                for (CfDef cf : cfs) {
                    if (cf.getName().equalsIgnoreCase(internalTableName(tableRef))) {
                        cf.setGc_grace_seconds(gcGraceSeconds);
                        cf.setCompaction_strategy_options(ImmutableMap.of(
                                "tombstone_threshold",
                                String.valueOf(tombstoneThresholdRatio)));
                        client.system_update_column_family(cf);
                        CassandraKeyValueServices.waitForSchemaVersions(
                                client,
                                tableRef.getQualifiedName(),
                                configManager.getConfig().schemaMutationTimeoutMillis());
                        log.trace("gc_grace_seconds is set to {} for {}.{}",
                                gcGraceSeconds, keyspace, tableRef);
                        log.trace("tombstone_threshold_ratio is set to {} for {}.{}",
                                tombstoneThresholdRatio, keyspace, tableRef);
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

    public CassandraClientPool getClientPool() {
        return clientPool;
    }

    public TracingQueryRunner getTracingQueryRunner() {
        return queryRunner;
    }

    public CassandraTables getCassandraTables() {
        return cassandraTables;
    }

    /**
     * Does not require all Cassandra nodes to be up and available, works as long as quorum is achieved.
     */
    public void cleanUpSchemaMutationLockTablesState() throws Exception {
        Set<TableReference> tables = lockTables.getAllLockTables();
        java.util.Optional<TableReference> tableToKeep = tables.stream().findFirst();
        if (!tableToKeep.isPresent()) {
            log.info("No lock tables to clean up.");
            return;
        }
        tables.remove(tableToKeep.get());
        if (tables.size() > 0) {
            dropTablesInternal(tables);
            log.info("Dropped tables [{}]", UnsafeArg.of("table names", tables));
        }
        schemaMutationLock.cleanLockState();
        log.info("Reset the schema mutation lock in table [{}]",
                UnsafeArg.of("table name", tableToKeep.get().toString()));
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

    private <V> Map<InetSocketAddress, List<V>> partitionByHost(
            Iterable<V> iterable,
            Function<V, byte[]> keyExtractor) {
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
        public static final Function<TableCellAndValue, byte[]> EXTRACT_ROW_NAME_FUNCTION =
                input -> input.cell.getRowName();

        public static final Function<TableCellAndValue, Long> SIZING_FUNCTION =
                input -> input.value.length + Cells.getApproxSizeOfCell(input.cell);

        private final TableReference tableRef;
        private final Cell cell;
        private final byte[] value;

        TableCellAndValue(TableReference tableRef, Cell cell, byte[] value) {
            this.tableRef = tableRef;
            this.cell = cell;
            this.value = value;
        }
    }
}
