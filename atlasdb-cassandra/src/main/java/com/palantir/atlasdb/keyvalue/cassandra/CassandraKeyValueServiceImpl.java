/*
 * (c) Copyright 2018 Palantir Technologies Inc. All rights reserved.
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
package com.palantir.atlasdb.keyvalue.cassandra;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.cassandra.thrift.CfDef;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.KsDef;
import org.apache.cassandra.thrift.Mutation;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.InstrumentedExecutorService;
import com.codahale.metrics.MetricRegistry;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicates;
import com.google.common.base.Supplier;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.UncheckedExecutionException;
import com.palantir.async.initializer.AsyncInitializer;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfig;
import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceRuntimeConfig;
import com.palantir.atlasdb.cassandra.CassandraMutationTimestampProvider;
import com.palantir.atlasdb.cassandra.CassandraMutationTimestampProviders;
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
import com.palantir.atlasdb.keyvalue.api.ImmutableCandidateCellForSweepingRequest;
import com.palantir.atlasdb.keyvalue.api.InsufficientConsistencyException;
import com.palantir.atlasdb.keyvalue.api.KeyAlreadyExistsException;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.RangeRequests;
import com.palantir.atlasdb.keyvalue.api.RowColumnRangeIterator;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.keyvalue.cassandra.CassandraKeyValueServices.StartTsResultsCollector;
import com.palantir.atlasdb.keyvalue.cassandra.cas.CheckAndSetResult;
import com.palantir.atlasdb.keyvalue.cassandra.cas.CheckAndSetRunner;
import com.palantir.atlasdb.keyvalue.cassandra.paging.CassandraRangePagingIterable;
import com.palantir.atlasdb.keyvalue.cassandra.paging.ColumnGetter;
import com.palantir.atlasdb.keyvalue.cassandra.paging.RowGetter;
import com.palantir.atlasdb.keyvalue.cassandra.paging.ThriftColumnGetter;
import com.palantir.atlasdb.keyvalue.cassandra.sweep.CandidateRowForSweeping;
import com.palantir.atlasdb.keyvalue.cassandra.sweep.CandidateRowsForSweepingIterator;
import com.palantir.atlasdb.keyvalue.cassandra.thrift.MutationMap;
import com.palantir.atlasdb.keyvalue.cassandra.thrift.Mutations;
import com.palantir.atlasdb.keyvalue.cassandra.thrift.SlicePredicates;
import com.palantir.atlasdb.keyvalue.cassandra.thrift.SlicePredicates.Limit;
import com.palantir.atlasdb.keyvalue.cassandra.thrift.SlicePredicates.Range;
import com.palantir.atlasdb.keyvalue.impl.AbstractKeyValueService;
import com.palantir.atlasdb.keyvalue.impl.Cells;
import com.palantir.atlasdb.keyvalue.impl.IterablePartitioner;
import com.palantir.atlasdb.keyvalue.impl.KeyValueServices;
import com.palantir.atlasdb.keyvalue.impl.LocalRowColumnRangeIterator;
import com.palantir.atlasdb.logging.LoggingArgs;
import com.palantir.atlasdb.table.description.TableMetadata;
import com.palantir.atlasdb.util.AnnotatedCallable;
import com.palantir.atlasdb.util.AnnotationType;
import com.palantir.atlasdb.util.AtlasDbMetrics;
import com.palantir.atlasdb.util.MetricsManager;
import com.palantir.atlasdb.util.MetricsManagers;
import com.palantir.common.annotation.Idempotent;
import com.palantir.common.base.ClosableIterator;
import com.palantir.common.base.ClosableIterators;
import com.palantir.common.base.FunctionCheckedException;
import com.palantir.common.base.Throwables;
import com.palantir.common.exception.AtlasDbDependencyException;
import com.palantir.common.exception.PalantirRuntimeException;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.UnsafeArg;
import com.palantir.util.paging.AbstractPagingIterable;
import com.palantir.util.paging.SimpleTokenBackedResultsPage;
import com.palantir.util.paging.TokenBackedBasicResultsPage;

import okio.ByteString;

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
@SuppressWarnings({"FinalClass", "Not final for mocking in tests"})
public class CassandraKeyValueServiceImpl extends AbstractKeyValueService implements CassandraKeyValueService {
    @VisibleForTesting
    class InitializingWrapper extends AsyncInitializer implements AutoDelegate_CassandraKeyValueService {
        @Override
        public CassandraKeyValueServiceImpl delegate() {
            checkInitialized();
            return CassandraKeyValueServiceImpl.this;
        }

        @Override
        protected void tryInitialize() {
            CassandraKeyValueServiceImpl.this.tryInitialize();
        }

        @Override
        public boolean supportsCheckAndSet() {
            return CassandraKeyValueServiceImpl.this.supportsCheckAndSet();
        }

        @Override
        public boolean shouldTriggerCompactions() {
            return CassandraKeyValueServiceImpl.this.shouldTriggerCompactions();
        }

        @Override
        public CassandraClientPool getClientPool() {
            return CassandraKeyValueServiceImpl.this.getClientPool();
        }

        @Override
        protected String getInitializingClassName() {
            return "CassandraKeyValueService";
        }

        @Override
        public void close() {
            cancelInitialization(CassandraKeyValueServiceImpl.this::close);
        }
    }

    private static final long SCHEMA_MUTATION_TASK_TIMEOUT_SECONDS = 120;

    private final Logger log;

    private final MetricsManager metricsManager;
    private final CassandraKeyValueServiceConfig config;
    private final CassandraClientPool clientPool;

    private SchemaMutationLock schemaMutationLock;
    private final ExecutorService schemaMutationExecutor;
    private final UniqueSchemaMutationLockTable schemaMutationLockTable;

    private final Optional<LeaderConfig> leaderConfig;

    private ConsistencyLevel readConsistency = ConsistencyLevel.LOCAL_QUORUM;
    private final ConsistencyLevel writeConsistency = ConsistencyLevel.EACH_QUORUM;
    private final ConsistencyLevel deleteConsistency = ConsistencyLevel.ALL;

    private final TracingQueryRunner queryRunner;
    private final WrappingQueryRunner wrappingQueryRunner;
    private final CellLoader cellLoader;
    private final TaskRunner taskRunner;
    private final CellValuePutter cellValuePutter;
    private final CassandraTableDropper cassandraTableDropper;
    private final CheckAndSetRunner checkAndSetRunner;

    private final CassandraTables cassandraTables;

    private final InitializingWrapper wrapper = new InitializingWrapper();

    private final CassandraMutationTimestampProvider mutationTimestampProvider;

    public static CassandraKeyValueService createForTesting(
            CassandraKeyValueServiceConfig config,
            Optional<LeaderConfig> leaderConfig) {
        return create(MetricsManagers.createForTests(),
                config, leaderConfig, CassandraMutationTimestampProviders.legacyModeForTestsOnly());
    }

    public static CassandraKeyValueService create(
            MetricsManager metricsManager,
            CassandraKeyValueServiceConfig config,
            Optional<LeaderConfig> leaderConfig,
            CassandraMutationTimestampProvider mutationTimestampProvider) {
        return create(
                metricsManager,
                config,
                CassandraKeyValueServiceRuntimeConfig::getDefault,
                leaderConfig,
                mutationTimestampProvider,
                AtlasDbConstants.DEFAULT_INITIALIZE_ASYNC);
    }

    public static CassandraKeyValueService create(
            MetricsManager metricsManager,
            CassandraKeyValueServiceConfig config,
            Optional<LeaderConfig> leaderConfig,
            CassandraMutationTimestampProvider mutationTimestampProvider,
            CassandraClientPool clientPool) {
        return create(metricsManager,
                config,
                clientPool,
                leaderConfig,
                mutationTimestampProvider,
                LoggerFactory.getLogger(CassandraKeyValueService.class),
                AtlasDbConstants.DEFAULT_INITIALIZE_ASYNC);
    }

    public static CassandraKeyValueService create(
            MetricsManager metricsManager,
            CassandraKeyValueServiceConfig config,
            java.util.function.Supplier<CassandraKeyValueServiceRuntimeConfig> runtimeConfig,
            Optional<LeaderConfig> leaderConfig,
            CassandraMutationTimestampProvider mutationTimestampProvider,
            boolean initializeAsync) {
        return create(metricsManager,
                config,
                runtimeConfig,
                leaderConfig,
                mutationTimestampProvider,
                LoggerFactory.getLogger(CassandraKeyValueService.class),
                initializeAsync);
    }

    @VisibleForTesting
    static CassandraKeyValueService create(
            MetricsManager metricsManager,
            CassandraKeyValueServiceConfig config,
            Optional<LeaderConfig> leaderConfig,
            CassandraMutationTimestampProvider mutationTimestampProvider,
            Logger log) {
        return create(metricsManager,
                config,
                CassandraKeyValueServiceRuntimeConfig::getDefault,
                leaderConfig,
                mutationTimestampProvider,
                log,
                AtlasDbConstants.DEFAULT_INITIALIZE_ASYNC);
    }

    private static CassandraKeyValueService create(
            MetricsManager metricsManager,
            CassandraKeyValueServiceConfig config,
            java.util.function.Supplier<CassandraKeyValueServiceRuntimeConfig> runtimeConfig,
            Optional<LeaderConfig> leaderConfig,
            CassandraMutationTimestampProvider mutationTimestampProvider,
            Logger log,
            boolean initializeAsync) {
        CassandraClientPool clientPool = CassandraClientPoolImpl.create(metricsManager,
                config,
                runtimeConfig,
                initializeAsync);
        try {
            return create(metricsManager, config, clientPool, leaderConfig,
                    mutationTimestampProvider, log, initializeAsync);
        } catch (Exception e) {
            log.warn("Error occurred in creating Cassandra KVS. Now attempting to shut down client pool...", e);
            try {
                clientPool.shutdown();
                log.info("Cassandra client pool shut down.");
            } catch (RuntimeException internalException) {
                log.info("An error occurred whilst shutting down the Cassandra client pool", internalException);
                throw internalException;
            }
            throw Throwables.rewrapAndThrowUncheckedException(e);
        }
    }

    private static CassandraKeyValueService create(
            MetricsManager metricsManager,
            CassandraKeyValueServiceConfig config,
            CassandraClientPool clientPool,
            Optional<LeaderConfig> leaderConfig,
            CassandraMutationTimestampProvider mutationTimestampProvider,
            Logger log,
            boolean initializeAsync) {
        CassandraKeyValueServiceImpl keyValueService = new CassandraKeyValueServiceImpl(
                log,
                metricsManager,
                config,
                clientPool,
                leaderConfig,
                mutationTimestampProvider);
        keyValueService.wrapper.initialize(initializeAsync);
        return keyValueService.wrapper.isInitialized() ? keyValueService : keyValueService.wrapper;
    }

    private CassandraKeyValueServiceImpl(Logger log,
            MetricsManager metricsManager, CassandraKeyValueServiceConfig config,
            CassandraClientPool clientPool,
            Optional<LeaderConfig> leaderConfig,
            CassandraMutationTimestampProvider mutationTimestampProvider) {
        super(createInstrumentedFixedThreadPool(config, metricsManager.getRegistry()));
        this.log = log;
        this.metricsManager = metricsManager;
        this.config = config;
        this.clientPool = clientPool;
        this.leaderConfig = leaderConfig;
        this.mutationTimestampProvider = mutationTimestampProvider;

        SchemaMutationLockTables lockTables = new SchemaMutationLockTables(clientPool, config);
        this.schemaMutationLockTable = new UniqueSchemaMutationLockTable(lockTables, whoIsTheLockCreator());
        this.schemaMutationExecutor = createFixedThreadPool("schemaMutation", 1);

        this.queryRunner = new TracingQueryRunner(log, tracingPrefs);
        this.wrappingQueryRunner = new WrappingQueryRunner(queryRunner);
        this.cassandraTables = new CassandraTables(clientPool, config);
        this.taskRunner = new TaskRunner(executor);
        this.cellLoader = new CellLoader(config, clientPool, wrappingQueryRunner, taskRunner);
        this.cellValuePutter = new CellValuePutter(
                config,
                clientPool,
                taskRunner,
                wrappingQueryRunner,
                writeConsistency,
                mutationTimestampProvider::getSweepSentinelWriteTimestamp);
        this.cassandraTableDropper = new CassandraTableDropper(config, clientPool, cellLoader, cellValuePutter,
                wrappingQueryRunner, deleteConsistency);
        this.checkAndSetRunner = new CheckAndSetRunner(queryRunner);
    }

    private static ExecutorService createInstrumentedFixedThreadPool(CassandraKeyValueServiceConfig config,
            MetricRegistry registry) {
        return new InstrumentedExecutorService(
                createFixedThreadPool("Atlas Cassandra KVS",
                        config.poolSize() * config.servers().size()),
                registry,
                MetricRegistry.name(CassandraKeyValueService.class, "executorService"));
    }

    @Override
    public boolean isInitialized() {
        return wrapper.isInitialized();
    }

    protected void initialize(boolean asyncInitialize) {
        wrapper.initialize(asyncInitialize);
    }

    private void tryInitialize() {
        boolean supportsCas = clientPool.runWithRetry(
                CassandraVerifier.underlyingCassandraClusterSupportsCASOperations);

        schemaMutationLock = new SchemaMutationLock(
                supportsCas,
                config,
                clientPool,
                queryRunner,
                writeConsistency,
                schemaMutationLockTable::getOnlyTable,
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
                config);
    }

    private LockLeader whoIsTheLockCreator() {
        return leaderConfig
                .map(LeaderConfig::whoIsTheLockLeader)
                .orElse(LockLeader.I_AM_THE_LOCK_LEADER);
    }

    @VisibleForTesting
    void upgradeFromOlderInternalSchema() {
        try {
            Map<TableReference, byte[]> metadataForTables = getMetadataForTables();
            final Collection<CfDef> updatedCfs = Lists.newArrayListWithExpectedSize(metadataForTables.size());

            List<CfDef> knownCfs = clientPool.runWithRetry(client ->
                    client.describe_keyspace(config.getKeyspaceOrThrow()).getCf_defs());

            for (CfDef clusterSideCf : knownCfs) {
                TableReference tableRef = CassandraKeyValueServices.tableReferenceFromCfDef(clusterSideCf);
                Optional<byte[]> relevantMetadata = lookupClusterSideMetadata(metadataForTables, tableRef);
                if (relevantMetadata.isPresent()) {
                    byte[] clusterSideMetadata = relevantMetadata.get();
                    CfDef clientSideCf = getCfForTable(tableRef, clusterSideMetadata,
                            config.gcGraceSeconds());
                    if (!ColumnFamilyDefinitions.isMatchingCf(clientSideCf, clusterSideCf)) {
                        // mismatch; we have changed how we generate schema since we last persisted
                        log.warn("Upgrading table {} to new internal Cassandra schema",
                                LoggingArgs.tableRef(tableRef));
                        updatedCfs.add(clientSideCf);
                    }
                } else if (!HiddenTables.isHidden(tableRef)) {
                    // Possible to get here from a race condition with another service starting up
                    // and performing schema upgrades concurrent with us doing this check
                    log.error("Found a table {} that did not have persisted"
                            + " AtlasDB metadata. If you recently did a Palantir update, try waiting until"
                            + " schema upgrades are completed on all backend CLIs/services etc and restarting"
                            + " this service. If this error re-occurs on subsequent attempted startups, please"
                            + " contact Palantir support.", LoggingArgs.tableRef(tableRef));
                }
            }

            // we are racing another service to do these same operations here, but they are idempotent / safe
            Map<Cell, byte[]> emptyMetadataUpdate = ImmutableMap.of();
            if (!updatedCfs.isEmpty()) {
                putMetadataAndMaybeAlterTables(true, emptyMetadataUpdate, updatedCfs);
                log.info("New table-related settings were applied on startup!!");
            } else {
                log.info("No tables are being upgraded on startup. No updated table-related settings found.");
            }
        } catch (TException e) {
            log.error("Couldn't upgrade from an older internal Cassandra schema."
                    + " New table-related settings may not have taken effect.");
        }
    }

    private Optional<byte[]> lookupClusterSideMetadata(
            Map<TableReference, byte[]> metadataForTables,
            TableReference tableRef) {
        // TODO (jkong): Replace with or() once we can use Java 9
        byte[] directMatch = metadataForTables.get(tableRef);
        if (directMatch != null) {
            return Optional.of(directMatch);
        }

        // No direct match but need to look for entries that match ignoring case.
        return Maps.filterEntries(metadataForTables, entry -> matchingIgnoreCase(entry.getKey(), tableRef))
                .values().stream().findAny();
    }

    private void lowerConsistencyWhenSafe() {
        Set<String> dcs;
        Map<String, String> strategyOptions;

        try {
            dcs = clientPool.runWithRetry(client ->
                    CassandraVerifier.sanityCheckDatacenters(
                            client,
                            config));
            KsDef ksDef = clientPool.runWithRetry(client ->
                    client.describe_keyspace(config.getKeyspaceOrThrow()));
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

        Set<Entry<InetSocketAddress, List<byte[]>>> rowsByHost = HostPartitioner.partitionByHost(clientPool, rows,
                Functions.identity()).entrySet();
        List<Callable<Map<Cell, Value>>> tasks = Lists.newArrayListWithCapacity(rowsByHost.size());
        for (final Map.Entry<InetSocketAddress, List<byte[]>> hostAndRows : rowsByHost) {
            tasks.add(AnnotatedCallable.wrapWithThreadName(AnnotationType.PREPEND,
                    "Atlas getRows " + hostAndRows.getValue().size()
                            + " rows from " + tableRef + " on " + hostAndRows.getKey(),
                    () -> getRowsForSingleHost(hostAndRows.getKey(), tableRef, hostAndRows.getValue(), startTs)));
        }
        List<Map<Cell, Value>> perHostResults = taskRunner.runAllTasksCancelOnFailure(tasks);
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
            int fetchBatchCount = config.fetchBatchCount();
            for (final List<byte[]> batch : Lists.partition(rows, fetchBatchCount)) {
                rowCount += batch.size();
                result.putAll(clientPool.runWithRetryOnHost(host,
                        new FunctionCheckedException<CassandraClient, Map<Cell, Value>, Exception>() {
                            @Override
                            public Map<Cell, Value> apply(CassandraClient client) throws Exception {
                                // We want to get all the columns in the row so set start and end to empty.
                                SlicePredicate pred = SlicePredicates.create(Range.ALL, Limit.NO_LIMIT);

                                List<ByteBuffer> rowNames = wrap(batch);

                                Map<ByteBuffer, List<ColumnOrSuperColumn>> results = wrappingQueryRunner.multiget(
                                        "getRows", client, tableRef, rowNames, pred, readConsistency);
                                Map<Cell, Value> ret = Maps.newHashMapWithExpectedSize(batch.size());
                                new ValueExtractor(metricsManager, ret)
                                        .extractResults(results, startTs, ColumnSelection.all());
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
                        LoggingArgs.tableRef(tableRef),
                        SafeArg.of("rowCount", rowCount),
                        SafeArg.of("stacktrace", CassandraKeyValueServices.getFilteredStackTrace("com.palantir")));
            }
            return ImmutableMap.copyOf(result);
        } catch (Exception e) {
            throw Throwables.unwrapAndThrowAtlasDbDependencyException(e);
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

        StartTsResultsCollector collector = new StartTsResultsCollector(metricsManager, startTs);
        cellLoader.loadWithTs("getRows", tableRef, cells, startTs, false, collector, readConsistency);
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
            log.info("Attempted get on '{}' table with empty cells", LoggingArgs.tableRef(tableRef));
            return ImmutableMap.of();
        }

        try {
            Long firstTs = timestampByCell.values().iterator().next();
            if (Iterables.all(timestampByCell.values(), Predicates.equalTo(firstTs))) {
                return get("get", tableRef, timestampByCell.keySet(), firstTs);
            }

            SetMultimap<Long, Cell> cellsByTs = Multimaps.invertFrom(
                    Multimaps.forMap(timestampByCell), HashMultimap.<Long, Cell>create());
            Builder<Cell, Value> builder = ImmutableMap.builder();
            for (long ts : cellsByTs.keySet()) {
                StartTsResultsCollector collector = new StartTsResultsCollector(metricsManager, ts);
                cellLoader.loadWithTs("get", tableRef, cellsByTs.get(ts), ts, false, collector, readConsistency);
                builder.putAll(collector.getCollectedResults());
            }
            return builder.build();
        } catch (Exception e) {
            throw Throwables.unwrapAndThrowAtlasDbDependencyException(e);
        }
    }

    private Map<Cell, Value> get(String kvsMethodName, TableReference tableRef, Set<Cell> cells,
            long maxTimestampExclusive) {
        StartTsResultsCollector collector = new StartTsResultsCollector(metricsManager, maxTimestampExclusive);
        cellLoader.loadWithTs(kvsMethodName, tableRef, cells, maxTimestampExclusive, false, collector, readConsistency);
        return collector.getCollectedResults();
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
        Set<Entry<InetSocketAddress, List<byte[]>>> rowsByHost = HostPartitioner.partitionByHost(clientPool, rows,
                Functions.identity()).entrySet();
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
        List<Map<byte[], RowColumnRangeIterator>> perHostResults = taskRunner.runAllTasksCancelOnFailure(tasks);
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
            throw Throwables.unwrapAndThrowAtlasDbDependencyException(e);
        }
    }

    private RowColumnRangeExtractor.RowColumnRangeResult getRowsColumnRangeForSingleHost(InetSocketAddress host,
                                                             TableReference tableRef,
                                                             List<byte[]> rows,
                                                             BatchColumnRangeSelection batchColumnRangeSelection,
                                                             long startTs) {
        try {
            return clientPool.runWithRetryOnHost(host,
                    new FunctionCheckedException<CassandraClient, RowColumnRangeExtractor.RowColumnRangeResult,
                            Exception>() {
                        @Override
                        public RowColumnRangeExtractor.RowColumnRangeResult apply(CassandraClient client)
                                throws Exception {
                            Range range = createColumnRange(
                                    batchColumnRangeSelection.getStartCol(),
                                    batchColumnRangeSelection.getEndCol(), startTs);
                            Limit limit = Limit.of(batchColumnRangeSelection.getBatchHint());
                            SlicePredicate pred = SlicePredicates.create(range, limit);

                            Map<ByteBuffer, List<ColumnOrSuperColumn>> results = wrappingQueryRunner.multiget(
                                    "getRowsColumnRange", client, tableRef, wrap(rows), pred, readConsistency);

                            RowColumnRangeExtractor extractor = new RowColumnRangeExtractor(metricsManager);
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
            throw Throwables.unwrapAndThrowAtlasDbDependencyException(e);
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
                        CassandraClient,
                        TokenBackedBasicResultsPage<Entry<Cell, Value>, byte[]>,
                        Exception>() {
                    @Override
                    public TokenBackedBasicResultsPage<Entry<Cell, Value>, byte[]> apply(CassandraClient client)
                            throws Exception {
                        Range range = createColumnRange(startCol, batchColumnRangeSelection.getEndCol(), startTs);
                        Limit limit = Limit.of(batchColumnRangeSelection.getBatchHint());
                        SlicePredicate pred = SlicePredicates.create(range, limit);

                        ByteBuffer rowByteBuffer = ByteBuffer.wrap(row);

                        Map<ByteBuffer, List<ColumnOrSuperColumn>> results = wrappingQueryRunner.multiget(
                                "getRowsColumnRange", client, tableRef, ImmutableList.of(rowByteBuffer), pred,
                                readConsistency);

                        if (results.isEmpty()) {
                            return SimpleTokenBackedResultsPage.create(startCol, ImmutableList.of(), false);
                        }
                        List<ColumnOrSuperColumn> values = Iterables.getOnlyElement(results.values());
                        if (values.isEmpty()) {
                            return SimpleTokenBackedResultsPage.create(startCol, ImmutableList.of(), false);
                        }
                        RowColumnRangeExtractor extractor = new RowColumnRangeExtractor(metricsManager);
                        extractor.extractResults(ImmutableList.of(row), results, startTs);
                        RowColumnRangeExtractor.RowColumnRangeResult decoded = extractor.getRowColumnRangeResult();
                        Map<Cell, Value> ret = decoded.getResults().get(row);

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

    private Range createColumnRange(byte[] startColOrEmpty, byte[] endColExlusiveOrEmpty, long startTs) {
        ByteBuffer start = startColOrEmpty.length == 0
                ? Range.UNBOUND_START
                : Range.startOfColumn(startColOrEmpty, startTs);
        ByteBuffer end = endColExlusiveOrEmpty.length == 0
                ? Range.UNBOUND_END
                : Range.endOfColumnIncludingSentinels(RangeRequests.previousLexicographicName(endColExlusiveOrEmpty));
        return Range.of(start, end);
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
            cellValuePutter.put("put", tableRef,
                    KeyValueServices.toConstantTimestampValues(values.entrySet(), timestamp));
        } catch (Exception e) {
            throw Throwables.unwrapAndThrowAtlasDbDependencyException(e);
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
            cellValuePutter.put("putWithTimestamps", tableRef, values.entries());
        } catch (Exception e) {
            throw Throwables.unwrapAndThrowAtlasDbDependencyException(e);
        }
    }

    @Override
    protected int getMultiPutBatchCount() {
        return config.mutationBatchCount();
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
        Map<InetSocketAddress, List<TableCellAndValue>> partitionedByHost = HostPartitioner.partitionByHost(clientPool,
                flattened, TableCellAndValue.EXTRACT_ROW_NAME_FUNCTION);

        List<Callable<Void>> callables = Lists.newArrayList();
        for (Map.Entry<InetSocketAddress, List<TableCellAndValue>> entry : partitionedByHost.entrySet()) {
            callables.addAll(getMultiPutTasksForSingleHost(entry.getKey(), entry.getValue(), timestamp));
        }
        taskRunner.runAllTasksCancelOnFailure(callables);
    }

    private List<Callable<Void>> getMultiPutTasksForSingleHost(final InetSocketAddress host,
                                                               Collection<TableCellAndValue> values,
                                                               final long timestamp) {
        Iterable<List<TableCellAndValue>> partitioned =
                IterablePartitioner.partitionByCountAndBytes(values,
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
        final MutationMap mutationMap = convertToMutations(batch, timestamp);
        return clientPool.runWithRetryOnHost(host, new FunctionCheckedException<CassandraClient, Void, Exception>() {
            @Override
            public Void apply(CassandraClient client) throws Exception {
                return wrappingQueryRunner.batchMutate("multiPut", client, tableRefs, mutationMap,
                        writeConsistency);
            }

            @Override
            public String toString() {
                return "batch_mutate(" + host + ", " + tableRefs + ", " + batch.size() + " values)";
            }
        });
    }

    private MutationMap convertToMutations(List<TableCellAndValue> batch, long timestamp) {
        MutationMap mutationMap = new MutationMap();
        for (TableCellAndValue tableCellAndValue : batch) {
            Cell cell = tableCellAndValue.cell;
            Column col = CassandraKeyValueServices.createColumn(
                    cell,
                    Value.create(tableCellAndValue.value, timestamp));
            ColumnOrSuperColumn colOrSup = new ColumnOrSuperColumn();
            colOrSup.setColumn(col);
            Mutation mutation = new Mutation();
            mutation.setColumn_or_supercolumn(colOrSup);

            mutationMap.addMutationForCell(cell, tableCellAndValue.tableRef, mutation);
        }
        return mutationMap;
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
                throw new InsufficientConsistencyException("Truncating tables requires all Cassandra nodes"
                        + " to be up and available.");
            } catch (TException e) {
                throw Throwables.unwrapAndThrowAtlasDbDependencyException(e);
            }
        }
    }

    private void runTruncateInternal(final Set<TableReference> tablesToTruncate) throws TException {
        clientPool.run(new FunctionCheckedException<CassandraClient, Void, TException>() {
            @Override
            public Void apply(CassandraClient client) throws TException {
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

    private void truncateInternal(CassandraClient client, TableReference tableRef) throws TException {
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
                        UnsafeArg.of("table", tableRef.getQualifiedName()),
                        SafeArg.of("retries", CassandraConstants.MAX_TRUNCATION_ATTEMPTS - tries),
                        e);
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
        new CellDeleter(clientPool,
                wrappingQueryRunner,
                deleteConsistency,
                mutationTimestampProvider.getDeletionTimestampOperatorForBatchDelete()).delete(tableRef, keys);
    }

    @VisibleForTesting
    CfDef getCfForTable(TableReference tableRef, byte[] rawMetadata, int gcGraceSeconds) {
        return ColumnFamilyDefinitions
                .getCfDef(config.getKeyspaceOrThrow(), tableRef, gcGraceSeconds, rawMetadata);
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
        return getRangeWithPageCreator(tableRef, rangeRequest, timestamp, readConsistency,
                () -> ValueExtractor.create(metricsManager));
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
        CandidateCellForSweepingRequest request = ImmutableCandidateCellForSweepingRequest.builder()
                .startRowInclusive(rangeRequest.getStartInclusive())
                .maxTimestampExclusive(timestamp)
                .shouldCheckIfLatestValueIsEmpty(false)
                .shouldDeleteGarbageCollectionSentinels(true)
                .build();
        return getCandidateRowsForSweeping("getRangeOfTimestamps", tableRef, request)
                .flatMap(rows -> rows)
                .map(row -> row.toRowResult())
                .stopWhen(rowResult -> !rangeRequest.inRange(rowResult.getRowName()));
    }

    @Override
    public ClosableIterator<List<CandidateCellForSweeping>> getCandidateCellsForSweeping(
            TableReference tableRef,
            CandidateCellForSweepingRequest request) {
        return getCandidateRowsForSweeping("getCandidateCellsForSweeping", tableRef, request)
                .map(rows -> rows.stream()
                        .map(CandidateRowForSweeping::cells)
                        .flatMap(List::stream)
                        .collect(Collectors.toList()));
    }

    private ClosableIterator<List<CandidateRowForSweeping>> getCandidateRowsForSweeping(
            String kvsMethodName,
            TableReference tableRef,
            CandidateCellForSweepingRequest request) {
        RowGetter rowGetter = new RowGetter(clientPool, queryRunner, ConsistencyLevel.ALL, tableRef);
        return new CandidateRowsForSweepingIterator(
                (iteratorTableRef, cells, maxTimestampExclusive) ->
                        get(kvsMethodName, iteratorTableRef, cells, maxTimestampExclusive),
                newInstrumentedCqlExecutor(),
                rowGetter,
                tableRef,
                request,
                config);
    }

    private CqlExecutor newInstrumentedCqlExecutor() {
        return AtlasDbMetrics.instrument(metricsManager.getRegistry(),
                CqlExecutor.class,
                new CqlExecutorImpl(clientPool, ConsistencyLevel.ALL));
    }

    private <T> ClosableIterator<RowResult<T>> getRangeWithPageCreator(
            TableReference tableRef,
            RangeRequest rangeRequest,
            long startTs,
            ConsistencyLevel consistency,
            Supplier<ResultsExtractor<T>> resultsExtractor) {
        SlicePredicate predicate;
        if (rangeRequest.getColumnNames().size() == 1) {
            byte[] colName = rangeRequest.getColumnNames().iterator().next();
            predicate = SlicePredicates.latestVersionForColumn(colName, startTs);
        } else {
            // TODO(nziebart): optimize fetching multiple columns by performing a parallel range request for
            // each column. note that if no columns are specified, it's a special case that means all columns
            predicate = SlicePredicates.create(Range.ALL, Limit.NO_LIMIT);
        }
        RowGetter rowGetter = new RowGetter(clientPool, queryRunner, consistency, tableRef);
        ColumnGetter columnGetter = new ThriftColumnGetter();

        return getRangeWithPageCreator(rowGetter, predicate, columnGetter, rangeRequest, resultsExtractor, startTs);
    }

    private <T> ClosableIterator<RowResult<T>> getRangeWithPageCreator(
            RowGetter rowGetter,
            SlicePredicate slicePredicate,
            ColumnGetter columnGetter,
            RangeRequest rangeRequest,
            Supplier<ResultsExtractor<T>> resultsExtractor,
            long startTs) {
        if (rangeRequest.isReverse()) {
            throw new UnsupportedOperationException();
        }
        if (rangeRequest.isEmptyRange()) {
            return ClosableIterators.wrap(ImmutableList.<RowResult<T>>of().iterator());
        }

        CassandraRangePagingIterable<T> rowResults = new CassandraRangePagingIterable<>(
                rowGetter,
                slicePredicate,
                columnGetter,
                rangeRequest,
                resultsExtractor,
                startTs
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
     * @throws AtlasDbDependencyException if not all hosts respond successfully, or if their schema versions do
     * not come to agreement in 1 minute.
     * @throws UncheckedExecutionException if there are multiple schema mutation lock tables.
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
     * @throws AtlasDbDependencyException if not all hosts respond successfully, or if their schema versions do
     * not come to agreement in 1 minute.
     * @throws UncheckedExecutionException if there are multiple schema mutation lock tables.
     */
    @Override
    public void dropTables(final Set<TableReference> tablesToDrop) {
        runSchemaMutationTask(() ->
                schemaMutationLock.runWithLock(() -> cassandraTableDropper.dropTables(tablesToDrop)));
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
     * @throws AtlasDbDependencyException if not all hosts respond successfully.
     * @throws UncheckedExecutionException if there are multiple schema mutation lock tables.
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
     * @throws AtlasDbDependencyException if not all hosts respond successfully.
     * @throws UncheckedExecutionException if there are multiple schema mutation lock tables.
     */
    @Override
    public void createTables(final Map<TableReference, byte[]> tableNamesToTableMetadata) {
        Map<TableReference, byte[]> tablesToActuallyCreate = filterOutExistingTables(tableNamesToTableMetadata);
        Map<TableReference, byte[]> tablesToUpdateMetadataFor = filterOutNoOpMetadataChanges(tableNamesToTableMetadata);

        boolean onlyMetadataChangesAreForNewTables =
                tablesToUpdateMetadataFor.keySet().equals(tablesToActuallyCreate.keySet());
        boolean putMetadataWillNeedASchemaChange = !onlyMetadataChangesAreForNewTables;

        if (!tablesToActuallyCreate.isEmpty()) {
            LoggingArgs.SafeAndUnsafeTableReferences safeAndUnsafe = LoggingArgs.tableRefs(
                    tablesToActuallyCreate.keySet());
            log.info("Grabbing schema mutation lock to create tables {} and {}",
                    safeAndUnsafe.safeTableRefs(), safeAndUnsafe.unsafeTableRefs());

            runSchemaMutationTask(() ->
                    schemaMutationLock.runWithLock(() -> createTablesInternal(tablesToActuallyCreate)));
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
            if (metadataIsDifferent(existingTableMetadata.get(tableReference), newMetadata)) {
                Set<TableReference> matchingTables = Sets.filter(existingTableMetadata.keySet(), existingTableRef ->
                        existingTableRef.getQualifiedName().equalsIgnoreCase(tableReference.getQualifiedName()));

                // completely new table, not an update
                if (matchingTables.isEmpty()) {
                    tableMetadataUpdates.put(tableReference, newMetadata);
                } else { // existing case-insensitive table, maybe an update
                    if (Arrays.equals(
                            existingTableMetadata.get(Iterables.getOnlyElement(matchingTables)), newMetadata)) {
                        log.debug("Case-insensitive matched table already existed with same metadata,"
                                + " skipping update to {}", LoggingArgs.tableRef(tableReference));
                    } else { // existing table has different metadata, so we should perform an update
                        tableMetadataUpdates.put(tableReference, newMetadata);
                    }
                }
            } else {
                log.debug("Table already existed with same metadata, skipping update to {}",
                        LoggingArgs.tableRef(tableReference));
            }
        }

        return tableMetadataUpdates;
    }

    private Map<TableReference, byte[]> filterOutExistingTables(
            final Map<TableReference, byte[]> tableNamesToTableMetadata) {
        Map<TableReference, byte[]> filteredTables = Maps.newHashMap();
        try {
            Set<TableReference> existingTablesLowerCased = cassandraTables.getExistingLowerCased().stream()
                    .map(TableReference::fromInternalTableName)
                    .collect(Collectors.toSet());

            for (Entry<TableReference, byte[]> tableAndMetadataPair : tableNamesToTableMetadata.entrySet()) {
                TableReference table = tableAndMetadataPair.getKey();
                byte[] metadata = tableAndMetadataPair.getValue();

                CassandraVerifier.sanityCheckTableName(table);

                TableReference tableRefLowerCased = TableReference.createLowerCased(table);
                if (!existingTablesLowerCased.contains(tableRefLowerCased)) {
                    filteredTables.put(table, metadata);
                } else {
                    log.debug("Filtering out existing table ({}) that already existed (case insensitive).",
                            LoggingArgs.tableRef(table));
                }
            }
        } catch (Exception e) {
            throw Throwables.unwrapAndThrowAtlasDbDependencyException(e);
        }

        return filteredTables;
    }

    private void runSchemaMutationTask(Runnable task) {
        try {
            Futures.getUnchecked(schemaMutationExecutor.submit(task));
        } catch (UncheckedExecutionException e) {
            // If this wraps an AtlasDbDependencyException, rewrap and throw
            if (e.getCause() instanceof AtlasDbDependencyException) {
                throw Throwables.unwrapAndThrowAtlasDbDependencyException(e);
            }
            throw e;
        }
    }

    private void createTablesInternal(final Map<TableReference, byte[]> tableNamesToTableMetadata) throws Exception {
        clientPool.runWithRetry(client -> {
            for (Entry<TableReference, byte[]> tableEntry : tableNamesToTableMetadata.entrySet()) {
                try {
                    client.system_add_column_family(ColumnFamilyDefinitions.getCfDef(
                            config.getKeyspaceOrThrow(),
                            tableEntry.getKey(),
                            config.gcGraceSeconds(),
                            tableEntry.getValue()));
                } catch (UnavailableException e) {
                    throw new InsufficientConsistencyException(
                            "Creating tables requires all Cassandra nodes to be up and available.");
                } catch (TException thriftException) {
                    if (thriftException.getMessage() != null
                            && !thriftException.getMessage().contains("already existing table")) {
                        throw Throwables.unwrapAndThrowAtlasDbDependencyException(thriftException);
                    }
                }
            }

            CassandraKeyValueServices.waitForSchemaVersions(
                    config,
                    client,
                    "(a call to createTables, filtered down to create: " + tableNamesToTableMetadata.keySet() + ")",
                    true);
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
                .filter(tr -> !HiddenTables.isHidden(tr))
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
        // try and get with a single-key lookup
        String lowerCaseTableName = tableRef.getQualifiedName().toLowerCase();
        Map<Cell, Value> rows = getRows(AtlasDbConstants.DEFAULT_METADATA_TABLE,
                ImmutableSet.of(lowerCaseTableName.getBytes()),
                ColumnSelection.all(),
                Long.MAX_VALUE);

        if (!rows.isEmpty()) {
            return Iterables.getOnlyElement(rows.values()).getContents();
        }

        // if unsuccessful with fast code-path, we need to check if this table exists but was written at a key
        // before we started enforcing only writing lower-case canonicalised versions of keys
        java.util.Optional<Entry<TableReference, byte[]>> match =
                getMetadataForTables().entrySet().stream().filter(
                        entry -> matchingIgnoreCase(entry.getKey(), tableRef))
                .findFirst();

        if (!match.isPresent()) {
            log.debug("Couldn't find table metadata for {}", LoggingArgs.tableRef(tableRef));
            return AtlasDbConstants.EMPTY_TABLE_METADATA;
        } else {
            log.debug("Found table metadata for {} at matching name {}", LoggingArgs.tableRef(tableRef),
                    LoggingArgs.tableRef("matchingTable", match.get().getKey()));
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
     * Note that this method does not guarantee that the case of {@link TableReference}s returned matches
     * that found in the schema. It is likely to be difficult to reconcile this without schema information.
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
                    TableReference tableRef = CassandraKeyValueServices.tableReferenceFromBytes(
                            entry.getKey().getRowName());
                    byte[] contents;
                    if (value == null) {
                        contents = AtlasDbConstants.EMPTY_TABLE_METADATA;
                    } else {
                        contents = value.getContents();
                    }
                    if (!HiddenTables.isHidden(tableRef)) {
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
                .map(TableReference::fromInternalTableName);
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
            Map<TableReference, byte[]> tableRefToMetadata, boolean possiblyNeedToPerformSettingsChanges) {
        if (tableRefToMetadata.isEmpty()) {
            return;
        }

        Map<TableReference, Cell> tableRefToNewCell = Maps.transformEntries(
                tableRefToMetadata, (tableRef, metadata) -> CassandraKeyValueServices.getMetadataCell(tableRef));
        Map<TableReference, Cell> tableRefToOldCell = Maps.transformEntries(
                tableRefToMetadata, (tableRef, metadata) -> CassandraKeyValueServices.getOldMetadataCell(tableRef));

        // technically we're racing other nodes from here on, during an update period,
        // but the penalty for not caring is just some superfluous schema mutations and a
        // few dead rows in the metadata table.
        Map<Cell, Value> existingMetadataAtNewName = get(AtlasDbConstants.DEFAULT_METADATA_TABLE,
                tableRefToNewCell.values().stream()
                        .collect(Collectors.toMap(Functions.identity(), Functions.constant(Long.MAX_VALUE))));

        Map<Cell, Value> existingMetadataAtOldName = get(AtlasDbConstants.DEFAULT_METADATA_TABLE,
                tableRefToOldCell.values().stream()
                        .collect(Collectors.toMap(Functions.identity(), Functions.constant(Long.MAX_VALUE))));

        final Map<Cell, byte[]> updatedMetadata = Maps.newHashMap();
        final Set<CfDef> updatedCfs = Sets.newHashSet();

        tableRefToNewCell.forEach((tableRef, newCell) -> {
            if (existingMetadataAtNewName.containsKey(newCell)) {
                if (metadataIsDifferent(
                        existingMetadataAtNewName.get(newCell).getContents(), tableRefToMetadata.get(tableRef))) {
                    // found existing metadata at new name, but we're performing an update
                    updatedMetadata.put(newCell, tableRefToMetadata.get(tableRef));
                    updatedCfs.add(getCfForTable(tableRef, tableRefToMetadata.get(tableRef), config.gcGraceSeconds()));
                }
            } else if (existingMetadataAtOldName.containsKey(tableRefToOldCell.get(tableRef))) {
                if (metadataIsDifferent(
                        existingMetadataAtOldName.get(tableRefToOldCell.get(tableRef)).getContents(),
                        tableRefToMetadata.get(tableRef))) {
                    // found existing metadata at old name, but we're performing an update
                    updatedMetadata.put(tableRefToOldCell.get(tableRef), tableRefToMetadata.get(tableRef));
                    updatedCfs.add(getCfForTable(tableRef, tableRefToMetadata.get(tableRef), config.gcGraceSeconds()));
                }
            } else {
                // didn't find an existing metadata at old or new names, this is completely new;
                // thus, let's write it out with the new format
                updatedMetadata.put(tableRefToNewCell.get(tableRef), tableRefToMetadata.get(tableRef));
                updatedCfs.add(getCfForTable(tableRef, tableRefToMetadata.get(tableRef), config.gcGraceSeconds()));
            }
        });

        if (!updatedMetadata.isEmpty()) {
            putMetadataAndMaybeAlterTables(possiblyNeedToPerformSettingsChanges, updatedMetadata, updatedCfs);
        }
    }

    private boolean metadataIsDifferent(byte[] existingMetadata, byte[] requestMetadata) {
        return !Arrays.equals(existingMetadata, requestMetadata);
    }

    private void putMetadataAndMaybeAlterTables(
            boolean possiblyNeedToPerformSettingsChanges,
            Map<Cell, byte[]> newMetadata,
            Collection<CfDef> updatedCfs) {
        try {
            clientPool.runWithRetry(client -> {
                if (possiblyNeedToPerformSettingsChanges) {
                    for (CfDef cf : updatedCfs) {
                        client.system_update_column_family(cf);
                    }

                    CassandraKeyValueServices.waitForSchemaVersions(
                            config,
                            client,
                            "(all tables in a call to putMetadataForTables)");
                }
                // Done with actual schema mutation, push the metadata
                put(AtlasDbConstants.DEFAULT_METADATA_TABLE, newMetadata, System.currentTimeMillis());
                return null;
            });
        } catch (Exception e) {
            throw Throwables.unwrapAndThrowAtlasDbDependencyException(e);
        }
    }

    @Override
    public void deleteRange(final TableReference tableRef, final RangeRequest range) {
        if (range.equals(RangeRequest.all())) {
            try {
                runTruncateInternal(ImmutableSet.of(tableRef));
            } catch (TException e) {
                log.info("Tried to make a deleteRange({}, RangeRequest.all())"
                                + " into a more garbage-cleanup friendly truncate(), but this failed.",
                        LoggingArgs.tableRef(tableRef), e);

                super.deleteRange(tableRef, range);
            }
        } else if (isForSingleRow(range.getStartInclusive(), range.getEndExclusive())) {
            try {
                long timestamp = mutationTimestampProvider.getRemoveTimestamp();
                byte[] row = range.getStartInclusive();
                clientPool.runWithRetry(client -> {
                    client.remove("deleteRange", tableRef, row, timestamp, deleteConsistency);
                    return null;
                });
            } catch (UnavailableException e) {
                throw new InsufficientConsistencyException(
                        "Deleting requires all Cassandra nodes to be up and available.", e);
            } catch (TException e) {
                throw Throwables.unwrapAndThrowAtlasDbDependencyException(e);
            }
        } else {
            super.deleteRange(tableRef, range);
        }
    }

    private static boolean isForSingleRow(byte[] startInclusive, byte[] endExclusive) {
        if (startInclusive.length == 0 || endExclusive.length == 0) {
            return false;
        }
        return Arrays.equals(endExclusive, RangeRequests.nextLexicographicName(startInclusive));
    }

    @Override
    public void deleteAllTimestamps(TableReference tableRef, Map<Cell, Long> maxTimestampExclusiveByCell,
            boolean deleteSentinels) {
        if (maxTimestampExclusiveByCell.isEmpty()) {
            return;
        }

        Map<InetSocketAddress, Map<Cell, Long>> keysByHost = HostPartitioner.partitionMapByHost(
                clientPool, maxTimestampExclusiveByCell.entrySet());

        // this is required by the interface of the CassandraMutationTimestampProvider, although it exists for tests
        long maxTimestampForAllCells = maxTimestampExclusiveByCell.values().stream()
                .mapToLong(x -> x).max().getAsLong();
        long rangeTombstoneCassandraTimestamp =
                mutationTimestampProvider.getRangeTombstoneTimestamp(maxTimestampForAllCells);
        for (Map.Entry<InetSocketAddress, Map<Cell, Long>> entry : keysByHost.entrySet()) {
            deleteAllTimestampsOnSingleHost(
                    tableRef,
                    entry.getKey(),
                    entry.getValue(),
                    deleteSentinels,
                    rangeTombstoneCassandraTimestamp);
        }
    }

    public void deleteAllTimestampsOnSingleHost(
            TableReference tableRef,
            InetSocketAddress host,
            Map<Cell, Long> maxTimestampExclusiveByCell,
            boolean deleteSentinels,
            long rangeTombstoneCassandraTs) {
        if (maxTimestampExclusiveByCell.isEmpty()) {
            return;
        }

        try {
            clientPool.runWithRetryOnHost(host, new FunctionCheckedException<CassandraClient, Void, Exception>() {

                @Override
                public Void apply(CassandraClient client) throws Exception {
                    insertRangeTombstones(client, maxTimestampExclusiveByCell, tableRef,
                            deleteSentinels, rangeTombstoneCassandraTs);
                    return null;
                }

                @Override
                public String toString() {
                    return "delete_timestamp_ranges_batch_mutate(" + host + ", " + tableRef.getQualifiedName() + ", "
                            + maxTimestampExclusiveByCell.size() + " column timestamp ranges)";
                }
            });
        } catch (UnavailableException e) {
            throw new InsufficientConsistencyException("Deleting requires all Cassandra nodes to be up and available.",
                    e);
        } catch (Exception e) {
            throw Throwables.unwrapAndThrowAtlasDbDependencyException(e);
        }
    }

    private void insertRangeTombstones(CassandraClient client, Map<Cell, Long> maxTimestampExclusiveByCell,
            TableReference tableRef, boolean deleteSentinel, long rangeTombstoneCassandraTs) throws TException {
        MutationMap mutationMap = new MutationMap();

        maxTimestampExclusiveByCell.forEach((cell, maxTimestampExclusive) -> {
            Mutation mutation = getMutation(cell, maxTimestampExclusive, deleteSentinel, rangeTombstoneCassandraTs);

            mutationMap.addMutationForCell(cell, tableRef, mutation);
        });

        wrappingQueryRunner.batchMutate("deleteAllTimestamps", client, ImmutableSet.of(tableRef), mutationMap,
                deleteConsistency);
    }

    private Mutation getMutation(Cell cell, long maxTimestampExclusive,
            boolean deleteSentinel, long rangeTombstoneCassandraTimestamp) {
        if (deleteSentinel) {
            return Mutations.rangeTombstoneIncludingSentinelForColumn(cell.getColumnName(), maxTimestampExclusive,
                    rangeTombstoneCassandraTimestamp);
        }
        return Mutations.rangeTombstoneForColumn(
                cell.getColumnName(),
                maxTimestampExclusive,
                rangeTombstoneCassandraTimestamp);
    }

    /**
     * Performs non-destructive cleanup when the KVS is no longer needed.
     */
    @Override
    public void close() {
        schemaMutationExecutor.shutdown();
        try {
            schemaMutationExecutor.awaitTermination(SCHEMA_MUTATION_TASK_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            // Continue with further transaction manager clean-up
            Thread.currentThread().interrupt();
        }
        clientPool.shutdown();
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
            cellValuePutter.putWithOverriddenTimestamps("addGarbageCollectionSentinelValues",
                    tableRef, Iterables.transform(cells, cell -> Maps.immutableEntry(cell, value)));
        } catch (Exception e) {
            throw Throwables.unwrapAndThrowAtlasDbDependencyException(e);
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
        return cellLoader.getAllTimestamps(tableRef, cells, ts, deleteConsistency);
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
            Optional<KeyAlreadyExistsException> failure = clientPool.runWithRetry(client -> {
                for (Entry<Cell, byte[]> e : values.entrySet()) {
                    CheckAndSetRequest request = CheckAndSetRequest.newCell(tableRef, e.getKey(), e.getValue());
                    CheckAndSetResult casResult = checkAndSetRunner.executeCheckAndSet(client, request);
                    if (!casResult.successful()) {
                        return Optional.of(new KeyAlreadyExistsException(
                                String.format("The row in table %s already exists.", tableRef.getQualifiedName()),
                                ImmutableList.of(e.getKey())));
                    }
                }
                return Optional.empty();
            });
            failure.ifPresent(exception -> {
                throw exception;
            });
        } catch (KeyAlreadyExistsException e) {
            throw e;
        } catch (Exception e) {
            throw Throwables.unwrapAndThrowAtlasDbDependencyException(e);
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
            CheckAndSetResult casResult = clientPool.runWithRetry(
                    client -> checkAndSetRunner.executeCheckAndSet(client, request));
            if (!casResult.successful()) {
                List<byte[]> currentValues = casResult.existingValues()
                        .stream()
                        .map(ByteString::toByteArray)
                        .collect(Collectors.toList());

                throw new CheckAndSetException(
                        request.cell(),
                        request.table(),
                        request.oldValue().orElse(null),
                        currentValues);
            }
        } catch (CheckAndSetException e) {
            throw e;
        } catch (Exception e) {
            throw Throwables.unwrapAndThrowAtlasDbDependencyException(e);
        }
    }

    @Override
    public void compactInternally(TableReference tableRef) {
        log.info("Called compactInternally on {}, but this is a no-op for Cassandra KVS."
                + "Cassandra should eventually decide to compact this table for itself.",
                LoggingArgs.tableRef(tableRef));
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
                CassandraVerifier.currentRfOnKeyspaceMatchesDesiredRf(client, config);
                return true;
            } catch (Exception e) {
                log.warn("The config and Cassandra cluster do not agree on the replication factor.", e);
                return false;
            }
        });
    }

    private ClusterAvailabilityStatus getStatusByRunningOperationsOnEachHost() {
        int countUnreachableNodes = 0;
        for (InetSocketAddress host : clientPool.getCurrentPools().keySet()) {
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
            clientPool.runOnHost(host, clientPool.getValidatePartitioner());
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
        int replicationFactor = config.replicationFactor();
        return countUnreachableNodes < (replicationFactor + 1) / 2;
    }

    @Override
    public CassandraClientPool getClientPool() {
        return clientPool;
    }

    @Override
    public TracingQueryRunner getTracingQueryRunner() {
        return queryRunner;
    }

    @Override
    public CassandraTables getCassandraTables() {
        return cassandraTables;
    }

    @Override
    public boolean performanceIsSensitiveToTombstones() {
        return true;
    }

    private static class TableCellAndValue {
        private static final Function<TableCellAndValue, byte[]> EXTRACT_ROW_NAME_FUNCTION =
                input -> input.cell.getRowName();

        private static final Function<TableCellAndValue, Long> SIZING_FUNCTION =
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
