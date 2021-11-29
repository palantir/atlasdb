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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Functions;
import com.google.common.base.Predicates;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.UncheckedExecutionException;
import com.google.protobuf.ByteString;
import com.palantir.async.initializer.AsyncInitializer;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfig;
import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceRuntimeConfig;
import com.palantir.atlasdb.cassandra.CassandraMutationTimestampProvider;
import com.palantir.atlasdb.cassandra.CassandraMutationTimestampProviders;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.AsyncKeyValueService;
import com.palantir.atlasdb.keyvalue.api.BatchColumnRangeSelection;
import com.palantir.atlasdb.keyvalue.api.CandidateCellForSweeping;
import com.palantir.atlasdb.keyvalue.api.CandidateCellForSweepingRequest;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.CheckAndSetCompatibility;
import com.palantir.atlasdb.keyvalue.api.CheckAndSetException;
import com.palantir.atlasdb.keyvalue.api.CheckAndSetRequest;
import com.palantir.atlasdb.keyvalue.api.ClusterAvailabilityStatus;
import com.palantir.atlasdb.keyvalue.api.ColumnSelection;
import com.palantir.atlasdb.keyvalue.api.ImmutableCandidateCellForSweepingRequest;
import com.palantir.atlasdb.keyvalue.api.InsufficientConsistencyException;
import com.palantir.atlasdb.keyvalue.api.KeyAlreadyExistsException;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.RangeRequests;
import com.palantir.atlasdb.keyvalue.api.RetryLimitReachedException;
import com.palantir.atlasdb.keyvalue.api.RowColumnRangeIterator;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.TimestampRangeDelete;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.keyvalue.cassandra.CassandraKeyValueServices.StartTsResultsCollector;
import com.palantir.atlasdb.keyvalue.cassandra.cas.CheckAndSetRunner;
import com.palantir.atlasdb.keyvalue.cassandra.paging.RowGetter;
import com.palantir.atlasdb.keyvalue.cassandra.sweep.CandidateRowForSweeping;
import com.palantir.atlasdb.keyvalue.cassandra.sweep.CandidateRowsForSweepingIterator;
import com.palantir.atlasdb.keyvalue.cassandra.thrift.MutationMap;
import com.palantir.atlasdb.keyvalue.cassandra.thrift.SlicePredicates;
import com.palantir.atlasdb.keyvalue.cassandra.thrift.SlicePredicates.Limit;
import com.palantir.atlasdb.keyvalue.cassandra.thrift.SlicePredicates.Range;
import com.palantir.atlasdb.keyvalue.impl.AbstractKeyValueService;
import com.palantir.atlasdb.keyvalue.impl.Cells;
import com.palantir.atlasdb.keyvalue.impl.CheckAndSetResult;
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
import com.palantir.common.streams.KeyedStream;
import com.palantir.logsafe.Preconditions;
import com.palantir.logsafe.SafeArg;
import com.palantir.tracing.CloseableTracer;
import com.palantir.tracing.Tracers;
import com.palantir.tritium.metrics.MetricRegistries;
import com.palantir.tritium.metrics.registry.TaggedMetricRegistry;
import com.palantir.util.Pair;
import com.palantir.util.paging.AbstractPagingIterable;
import com.palantir.util.paging.SimpleTokenBackedResultsPage;
import com.palantir.util.paging.TokenBackedBasicResultsPage;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import javax.annotation.Nullable;
import org.apache.cassandra.thrift.CASResult;
import org.apache.cassandra.thrift.CfDef;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.Deletion;
import org.apache.cassandra.thrift.KeyPredicate;
import org.apache.cassandra.thrift.KsDef;
import org.apache.cassandra.thrift.Mutation;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
        public Collection<? extends KeyValueService> getDelegates() {
            return ImmutableList.of(delegate());
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
        public CheckAndSetCompatibility getCheckAndSetCompatibility() {
            return CassandraKeyValueServiceImpl.this.getCheckAndSetCompatibility();
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

    static final ConsistencyLevel WRITE_CONSISTENCY = ConsistencyLevel.EACH_QUORUM;
    static final ConsistencyLevel DELETE_CONSISTENCY = ConsistencyLevel.ALL;

    private final Logger log;

    private final MetricsManager metricsManager;
    private final CassandraKeyValueServiceConfig config;
    private final CassandraClientPool clientPool;

    private ConsistencyLevel readConsistency = ConsistencyLevel.LOCAL_QUORUM;

    private final TracingQueryRunner queryRunner;
    private final WrappingQueryRunner wrappingQueryRunner;
    private final CellLoader cellLoader;
    private final Optional<AsyncKeyValueService> asyncKeyValueService;
    private final RangeLoader rangeLoader;
    private final TaskRunner taskRunner;
    private final CellValuePutter cellValuePutter;
    private final CassandraTableMetadata tableMetadata;
    private final CassandraTableCreator cassandraTableCreator;
    private final CassandraTableDropper cassandraTableDropper;
    private final CassandraTableTruncator cassandraTableTruncator;
    private final CheckAndSetRunner checkAndSetRunner;

    private final CassandraTables cassandraTables;

    private final InitializingWrapper wrapper = new InitializingWrapper();

    private final CassandraMutationTimestampProvider mutationTimestampProvider;
    private final Supplier<CassandraKeyValueServiceRuntimeConfig> runtimeConfigSupplier;

    public static CassandraKeyValueService createForTesting(CassandraKeyValueServiceConfig config) {
        MetricsManager metricsManager = MetricsManagers.createForTests();
        CassandraClientPool clientPool = CassandraClientPoolImpl.createImplForTest(
                metricsManager, config, CassandraClientPoolImpl.StartupChecks.RUN, new Blacklist(config));

        return createOrShutdownClientPool(
                metricsManager,
                config,
                CassandraKeyValueServiceRuntimeConfig::getDefault,
                clientPool,
                CassandraMutationTimestampProviders.legacyModeForTestsOnly(),
                LoggerFactory.getLogger(CassandraKeyValueService.class),
                AtlasDbConstants.DEFAULT_INITIALIZE_ASYNC);
    }

    public static CassandraKeyValueService create(
            MetricsManager metricsManager,
            CassandraKeyValueServiceConfig config,
            CassandraMutationTimestampProvider mutationTimestampProvider) {
        return create(
                metricsManager,
                config,
                CassandraKeyValueServiceRuntimeConfig::getDefault,
                mutationTimestampProvider,
                AtlasDbConstants.DEFAULT_INITIALIZE_ASYNC);
    }

    public static CassandraKeyValueService create(
            MetricsManager metricsManager,
            CassandraKeyValueServiceConfig config,
            CassandraMutationTimestampProvider mutationTimestampProvider,
            CassandraClientPool clientPool) {
        return createOrShutdownClientPool(
                metricsManager,
                config,
                CassandraKeyValueServiceRuntimeConfig::getDefault,
                clientPool,
                mutationTimestampProvider,
                LoggerFactory.getLogger(CassandraKeyValueService.class),
                AtlasDbConstants.DEFAULT_INITIALIZE_ASYNC);
    }

    public static CassandraKeyValueService create(
            MetricsManager metricsManager,
            CassandraKeyValueServiceConfig config,
            Supplier<CassandraKeyValueServiceRuntimeConfig> runtimeConfig,
            CassandraMutationTimestampProvider mutationTimestampProvider,
            boolean initializeAsync) {
        return create(
                metricsManager,
                config,
                runtimeConfig,
                mutationTimestampProvider,
                LoggerFactory.getLogger(CassandraKeyValueService.class),
                initializeAsync);
    }

    @VisibleForTesting
    static CassandraKeyValueService create(
            MetricsManager metricsManager,
            CassandraKeyValueServiceConfig config,
            CassandraMutationTimestampProvider mutationTimestampProvider,
            Logger log) {
        return create(
                metricsManager,
                config,
                CassandraKeyValueServiceRuntimeConfig::getDefault,
                mutationTimestampProvider,
                log,
                AtlasDbConstants.DEFAULT_INITIALIZE_ASYNC);
    }

    @VisibleForTesting
    static CassandraKeyValueService create(
            MetricsManager metricsManager,
            CassandraKeyValueServiceConfig config,
            Supplier<CassandraKeyValueServiceRuntimeConfig> runtimeConfigSupplier,
            CassandraMutationTimestampProvider mutationTimestampProvider,
            Logger log,
            boolean initializeAsync) {
        CassandraClientPool clientPool =
                CassandraClientPoolImpl.create(metricsManager, config, runtimeConfigSupplier, initializeAsync);

        return createOrShutdownClientPool(
                metricsManager,
                config,
                runtimeConfigSupplier,
                clientPool,
                mutationTimestampProvider,
                log,
                initializeAsync);
    }

    private static CassandraKeyValueService createOrShutdownClientPool(
            MetricsManager metricsManager,
            CassandraKeyValueServiceConfig config,
            Supplier<CassandraKeyValueServiceRuntimeConfig> runtimeConfigSupplier,
            CassandraClientPool clientPool,
            CassandraMutationTimestampProvider mutationTimestampProvider,
            Logger log,
            boolean initializeAsync) {
        try {
            return createWithCqlClient(
                    metricsManager,
                    config,
                    runtimeConfigSupplier,
                    clientPool,
                    mutationTimestampProvider,
                    log,
                    initializeAsync);
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

    private static CassandraKeyValueService createWithCqlClient(
            MetricsManager metricsManager,
            CassandraKeyValueServiceConfig config,
            Supplier<CassandraKeyValueServiceRuntimeConfig> runtimeConfigSupplier,
            CassandraClientPool clientPool,
            CassandraMutationTimestampProvider mutationTimestampProvider,
            Logger log,
            boolean initializeAsync) {
        try {
            Optional<AsyncKeyValueService> asyncKeyValueService = config.asyncKeyValueServiceFactory()
                    .constructAsyncKeyValueService(metricsManager, config, initializeAsync);

            return createAndInitialize(
                    metricsManager,
                    config,
                    runtimeConfigSupplier,
                    clientPool,
                    asyncKeyValueService,
                    mutationTimestampProvider,
                    log,
                    initializeAsync);
        } catch (Exception e) {
            log.warn("Exception during async KVS creation.", e);
            throw Throwables.unwrapAndThrowAtlasDbDependencyException(e);
        }
    }

    private static CassandraKeyValueService createAndInitialize(
            MetricsManager metricsManager,
            CassandraKeyValueServiceConfig config,
            Supplier<CassandraKeyValueServiceRuntimeConfig> runtimeConfigSupplier,
            CassandraClientPool clientPool,
            Optional<AsyncKeyValueService> asyncKeyValueService,
            CassandraMutationTimestampProvider mutationTimestampProvider,
            Logger log,
            boolean initializeAsync) {
        CassandraKeyValueServiceImpl keyValueService = new CassandraKeyValueServiceImpl(
                log,
                metricsManager,
                config,
                asyncKeyValueService,
                runtimeConfigSupplier,
                clientPool,
                mutationTimestampProvider);
        keyValueService.wrapper.initialize(initializeAsync);
        return keyValueService.wrapper.isInitialized() ? keyValueService : keyValueService.wrapper;
    }

    private CassandraKeyValueServiceImpl(
            Logger log,
            MetricsManager metricsManager,
            CassandraKeyValueServiceConfig config,
            Optional<AsyncKeyValueService> asyncKeyValueService,
            Supplier<CassandraKeyValueServiceRuntimeConfig> runtimeConfigSupplier,
            CassandraClientPool clientPool,
            CassandraMutationTimestampProvider mutationTimestampProvider) {
        super(createBlockingThreadpool(config, metricsManager));
        this.log = log;
        this.metricsManager = metricsManager;
        this.config = config;
        this.clientPool = clientPool;
        this.asyncKeyValueService = asyncKeyValueService;
        this.mutationTimestampProvider = mutationTimestampProvider;
        this.queryRunner =
                new TracingQueryRunner(log, () -> runtimeConfigSupplier.get().tracing());
        this.wrappingQueryRunner = new WrappingQueryRunner(queryRunner);
        this.cassandraTables = new CassandraTables(clientPool, config);
        this.taskRunner = new TaskRunner(executor);
        this.cellLoader = CellLoader.create(clientPool, wrappingQueryRunner, taskRunner, runtimeConfigSupplier);
        this.rangeLoader = new RangeLoader(clientPool, queryRunner, metricsManager, readConsistency);
        this.cellValuePutter = new CellValuePutter(
                config,
                clientPool,
                taskRunner,
                wrappingQueryRunner,
                mutationTimestampProvider::getSweepSentinelWriteTimestamp);
        this.checkAndSetRunner = new CheckAndSetRunner(queryRunner);
        this.tableMetadata = new CassandraTableMetadata(rangeLoader, cassandraTables, clientPool, wrappingQueryRunner);
        this.cassandraTableCreator = new CassandraTableCreator(clientPool, config);
        this.cassandraTableTruncator = new CassandraTableTruncator(queryRunner, clientPool);
        this.cassandraTableDropper =
                new CassandraTableDropper(config, clientPool, tableMetadata, cassandraTableTruncator);
        this.runtimeConfigSupplier = runtimeConfigSupplier;
    }

    private static ExecutorService createBlockingThreadpool(
            CassandraKeyValueServiceConfig config, MetricsManager metricsManager) {
        return config.thriftExecutorServiceFactory()
                .orElseGet(() -> instrumentedFixedThreadPoolSupplier(config, metricsManager.getTaggedRegistry()))
                .get();
    }

    private static Supplier<ExecutorService> instrumentedFixedThreadPoolSupplier(
            CassandraKeyValueServiceConfig config, TaggedMetricRegistry registry) {
        return () -> {
            int numberOfThriftHosts = config.servers().numberOfThriftHosts();
            int corePoolSize = config.poolSize() * numberOfThriftHosts;
            int maxPoolSize = config.maxConnectionBurstSize() * numberOfThriftHosts;
            return Tracers.wrap(
                    "Atlas Cassandra KVS",
                    MetricRegistries.instrument(
                            registry,
                            createThreadPool("Atlas Cassandra KVS", corePoolSize, maxPoolSize),
                            "Atlas Cassandra KVS"));
        };
    }

    @Override
    public boolean isInitialized() {
        return wrapper.isInitialized();
    }

    protected void initialize(boolean asyncInitialize) {
        wrapper.initialize(asyncInitialize);
    }

    private void tryInitialize() {
        createTable(AtlasDbConstants.DEFAULT_METADATA_TABLE, AtlasDbConstants.EMPTY_TABLE_METADATA);
        lowerConsistencyWhenSafe();
        upgradeFromOlderInternalSchema();
        CassandraKeyValueServices.warnUserInInitializationIfClusterAlreadyInInconsistentState(clientPool, config);
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
                    CfDef clientSideCf = getCfForTable(tableRef, clusterSideMetadata, config.gcGraceSeconds());
                    if (!ColumnFamilyDefinitions.isMatchingCf(clientSideCf, clusterSideCf)) {
                        // mismatch; we have changed how we generate schema since we last persisted
                        log.warn("Upgrading table {} to new internal Cassandra schema", LoggingArgs.tableRef(tableRef));
                        updatedCfs.add(clientSideCf);
                    }
                } else if (!HiddenTables.isHidden(tableRef)) {
                    // Possible to get here from a race condition with another service starting up
                    // and performing schema upgrades concurrent with us doing this check
                    log.error(
                            "Found a table {} that did not have persisted"
                                    + " AtlasDB metadata. If you recently did a Palantir update, try waiting until"
                                    + " schema upgrades are completed on all backend CLIs/services etc and restarting"
                                    + " this service. If this error re-occurs on subsequent attempted startups, please"
                                    + " contact Palantir support.",
                            LoggingArgs.tableRef(tableRef));
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
            log.error(
                    "Couldn't upgrade from an older internal Cassandra schema. New table-related settings may not have"
                            + " taken effect.",
                    e);
        }
    }

    private static Optional<byte[]> lookupClusterSideMetadata(
            Map<TableReference, byte[]> metadataForTables, TableReference tableRef) {
        return Optional.ofNullable(metadataForTables.get(tableRef))
                .or(() -> Maps.filterEntries(metadataForTables, entry -> matchingIgnoreCase(entry.getKey(), tableRef))
                        .values()
                        .stream()
                        .findAny());
    }

    private void lowerConsistencyWhenSafe() {
        Set<String> dcs;
        Map<String, String> strategyOptions;

        try {
            dcs = clientPool.runWithRetry(client -> CassandraVerifier.sanityCheckDatacenters(client, config));
            KsDef ksDef = clientPool.runWithRetry(client -> client.describe_keyspace(config.getKeyspaceOrThrow()));
            strategyOptions = new HashMap<>(ksDef.getStrategy_options());

            if (dcs.size() == 1) {
                String dc = dcs.iterator().next();
                if (strategyOptions.get(dc) != null) {
                    int currentRf = Integer.parseInt(strategyOptions.get(dc));
                    if (currentRf == config.replicationFactor()) {
                        if (currentRf == 2 && config.clusterMeetsNormalConsistencyGuarantees()) {
                            log.info("Setting Read Consistency to ONE, as cluster has only one datacenter at RF2.");
                            readConsistency = ConsistencyLevel.ONE;
                            rangeLoader.setConsistencyLevel(readConsistency);
                        }
                    }
                }
            }
        } catch (TException e) {
            return;
        }
    }

    /**
     * Gets values from the key-value store. Requires a quorum of Cassandra nodes to be reachable.
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
     * @throws AtlasDbDependencyException if fewer than a quorum of Cassandra nodes are reachable.
     * @throws IllegalArgumentException if any of the requests were invalid
     *         (e.g., attempting to retrieve values from a non-existent table).
     */
    @Override
    public Map<Cell, Value> getRows(
            TableReference tableRef, Iterable<byte[]> rows, ColumnSelection selection, long startTs) {
        if (!selection.allColumnsSelected()) {
            return getRowsForSpecificColumns(tableRef, rows, selection, startTs);
        }

        Set<Map.Entry<InetSocketAddress, List<byte[]>>> rowsByHost = HostPartitioner.partitionByHost(
                        clientPool, rows, Functions.identity())
                .entrySet();
        List<Callable<Map<Cell, Value>>> tasks = new ArrayList<>(rowsByHost.size());
        for (final Map.Entry<InetSocketAddress, List<byte[]>> hostAndRows : rowsByHost) {
            tasks.add(AnnotatedCallable.wrapWithThreadName(
                    AnnotationType.PREPEND,
                    "Atlas getRows " + hostAndRows.getValue().size() + " rows from " + tableRef + " on "
                            + hostAndRows.getKey(),
                    () -> getRowsForSingleHost(hostAndRows.getKey(), tableRef, hostAndRows.getValue(), startTs)));
        }
        List<Map<Cell, Value>> perHostResults = taskRunner.runAllTasksCancelOnFailure(tasks);
        Map<Cell, Value> result = Maps.newHashMapWithExpectedSize(Iterables.size(rows));
        for (Map<Cell, Value> perHostResult : perHostResults) {
            result.putAll(perHostResult);
        }
        return result;
    }

    private Map<Cell, Value> getRowsForSingleHost(
            final InetSocketAddress host, final TableReference tableRef, final List<byte[]> rows, final long startTs) {
        try {
            int rowCount = 0;
            final Map<Cell, Value> result = new HashMap<>();
            int fetchBatchCount = config.fetchBatchCount();
            for (final List<byte[]> batch : Lists.partition(rows, fetchBatchCount)) {
                rowCount += batch.size();
                result.putAll(getAllCellsForRows(host, tableRef, batch, startTs));
            }
            if (rowCount > fetchBatchCount) {
                log.warn(
                        "Rebatched in getRows a call to {} that attempted to multiget {} rows; "
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

    private Map<Cell, Value> getAllCellsForRows(
            final InetSocketAddress host, final TableReference tableRef, final List<byte[]> rows, final long startTs)
            throws Exception {

        final ListMultimap<ByteBuffer, ColumnOrSuperColumn> result = LinkedListMultimap.create();

        List<KeyPredicate> query = rows.stream()
                .map(row -> keyPredicate(
                        ByteBuffer.wrap(row),
                        allPredicateWithLimit(runtimeConfigSupplier.get().fetchReadLimitPerRow())))
                .collect(Collectors.toList());

        while (!query.isEmpty()) {
            ListMultimap<ByteBuffer, ColumnOrSuperColumn> partialResult = KeyedStream.stream(
                            getForKeyPredicates(host, tableRef, query, startTs))
                    .filter(cells -> !cells.isEmpty())
                    .flatMap(Collection::stream)
                    .collectToMultimap(LinkedListMultimap::create);

            result.putAll(partialResult);

            query = KeyedStream.stream(Multimaps.asMap(partialResult))
                    .map((row, cells) -> keyPredicate(row, getNextLexicographicalSlicePredicate(cells)))
                    .values()
                    .collect(Collectors.toList());
        }

        ValueExtractor extractor = new ValueExtractor(metricsManager, Maps.newHashMapWithExpectedSize(result.size()));
        extractor.extractResults(Multimaps.asMap(result), startTs, ColumnSelection.all());
        return extractor.asMap();
    }

    private static KeyPredicate keyPredicate(ByteBuffer row, SlicePredicate predicate) {
        return new KeyPredicate().setKey(row).setPredicate(predicate);
    }

    private static SlicePredicate allPredicateWithLimit(int limit) {
        return SlicePredicates.create(Range.ALL, Limit.of(limit));
    }

    private Map<ByteBuffer, List<ColumnOrSuperColumn>> getForKeyPredicates(
            final InetSocketAddress host, final TableReference tableRef, List<KeyPredicate> query, final long startTs)
            throws Exception {
        return clientPool.runWithRetryOnHost(
                host,
                new FunctionCheckedException<CassandraClient, Map<ByteBuffer, List<ColumnOrSuperColumn>>, Exception>() {
                    @Override
                    public Map<ByteBuffer, List<ColumnOrSuperColumn>> apply(CassandraClient client) throws Exception {

                        if (log.isTraceEnabled()) {
                            log.trace(
                                    "Requesting {} cells from {} starting at timestamp {} on {} "
                                            + "as part of fetching cells for key predicates.",
                                    SafeArg.of("cells", query.size()),
                                    LoggingArgs.tableRef(tableRef),
                                    SafeArg.of("startTs", startTs),
                                    SafeArg.of("host", CassandraLogHelper.host(host)));
                        }

                        Map<ByteBuffer, List<List<ColumnOrSuperColumn>>> results =
                                wrappingQueryRunner.multiget_multislice(
                                        "getRows", client, tableRef, query, readConsistency);

                        return Maps.transformValues(results, lists -> Lists.newArrayList(Iterables.concat(lists)));
                    }

                    @Override
                    public String toString() {
                        return "multiget_multislice(" + host + ", " + tableRef + ", " + query.size() + " cells)";
                    }
                });
    }

    private SlicePredicate getNextLexicographicalSlicePredicate(List<ColumnOrSuperColumn> columns) {
        Preconditions.checkState(!columns.isEmpty(), "Columns was empty. This is probably an AtlasDb bug");

        Column lastColumn = columns.get(columns.size() - 1).getColumn();
        Pair<byte[], Long> columnNameAndTimestamp = CassandraKeyValueServices.decompose(lastColumn.name);
        ByteBuffer nextLexicographicColumn = CassandraKeyValueServices.makeCompositeBuffer(
                RangeRequests.nextLexicographicName(columnNameAndTimestamp.lhSide), Long.MAX_VALUE);

        return SlicePredicates.create(
                Range.of(nextLexicographicColumn, Range.UNBOUND_END),
                Limit.of(runtimeConfigSupplier.get().fetchReadLimitPerRow()));
    }

    private static List<ByteBuffer> wrap(List<byte[]> arrays) {
        List<ByteBuffer> byteBuffers = new ArrayList<>(arrays.size());
        for (byte[] r : arrays) {
            byteBuffers.add(ByteBuffer.wrap(r));
        }
        return byteBuffers;
    }

    private Map<Cell, Value> getRowsForSpecificColumns(
            final TableReference tableRef, final Iterable<byte[]> rows, ColumnSelection selection, final long startTs) {
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
     * Gets values from the key-value store. Requires a quorum of Cassandra nodes to be reachable.
     *
     * @param tableRef the name of the table to retrieve values from.
     * @param timestampByCell specifies, for each row, the maximum timestamp (exclusive) at which to
     *        retrieve that rows's value.
     *
     * @return map of retrieved values. Values which do not exist (either
     *         because they were deleted or never created in the first place)
     *         are simply not returned.
     *
     * @throws AtlasDbDependencyException if fewer than a quorum of Cassandra nodes are reachable.
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

            SetMultimap<Long, Cell> cellsByTs =
                    Multimaps.invertFrom(Multimaps.forMap(timestampByCell), HashMultimap.create());
            ImmutableMap.Builder<Cell, Value> builder = ImmutableMap.builder();
            for (long ts : cellsByTs.keySet()) {
                StartTsResultsCollector collector = new StartTsResultsCollector(metricsManager, ts);
                try (CloseableTracer tracer = CloseableTracer.startSpan("loadWithTs")) {
                    cellLoader.loadWithTs("get", tableRef, cellsByTs.get(ts), ts, false, collector, readConsistency);
                }
                builder.putAll(collector.getCollectedResults());
            }
            return builder.build();
        } catch (Exception e) {
            throw Throwables.unwrapAndThrowAtlasDbDependencyException(e);
        }
    }

    private Map<Cell, Value> get(
            String kvsMethodName, TableReference tableRef, Set<Cell> cells, long maxTimestampExclusive) {
        StartTsResultsCollector collector = new StartTsResultsCollector(metricsManager, maxTimestampExclusive);
        try (CloseableTracer tracer = CloseableTracer.startSpan("loadWithTs")) {
            cellLoader.loadWithTs(
                    kvsMethodName, tableRef, cells, maxTimestampExclusive, false, collector, readConsistency);
        }
        return collector.getCollectedResults();
    }

    /**
     * Gets values from the key-value store for the specified rows and column range as separate iterators for each row.
     * Requires a quorum of Cassandra nodes to be reachable, otherwise, the returned iterators will throw an
     * {@link AtlasDbDependencyException} when their methods are called.
     *
     * @param tableRef the name of the table to retrieve values from.
     * @param rows set containing the rows to retrieve values for. Behavior is undefined if {@code rows}
     *        contains duplicates (as defined by {@link Arrays#equals(byte[], byte[])}).
     * @param batchColumnRangeSelection specifies the column range and the per-row batchSize to fetch.
     * @param timestamp specifies the maximum timestamp (exclusive) at which to retrieve each rows's value.
     *
     * @return map of row names to {@link RowColumnRangeIterator}. Each {@link RowColumnRangeIterator} can iterate over
     *         the values that are spanned by the {@code batchColumnRangeSelection} in increasing order by column name.
     *
     * @throws IllegalArgumentException if {@code rows} contains duplicates.
     */
    @Override
    public Map<byte[], RowColumnRangeIterator> getRowsColumnRange(
            TableReference tableRef,
            Iterable<byte[]> rows,
            BatchColumnRangeSelection batchColumnRangeSelection,
            long timestamp) {
        Set<Map.Entry<InetSocketAddress, List<byte[]>>> rowsByHost = HostPartitioner.partitionByHost(
                        clientPool, rows, Functions.identity())
                .entrySet();
        List<Callable<Map<byte[], RowColumnRangeIterator>>> tasks = new ArrayList<>(rowsByHost.size());
        for (final Map.Entry<InetSocketAddress, List<byte[]>> hostAndRows : rowsByHost) {
            tasks.add(AnnotatedCallable.wrapWithThreadName(
                    AnnotationType.PREPEND,
                    "Atlas getRowsColumnRange " + hostAndRows.getValue().size() + " rows from " + tableRef + " on "
                            + hostAndRows.getKey(),
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

            Map<byte[], Map<Cell, Value>> results = firstPage.getResults();
            Map<byte[], Column> rowsToLastCompositeColumns = firstPage.getRowsToLastCompositeColumns();
            IdentityHashMap<byte[], byte[]> incompleteRowsToNextColumns = new IdentityHashMap<>();
            for (Map.Entry<byte[], Column> e : rowsToLastCompositeColumns.entrySet()) {
                byte[] row = e.getKey();
                byte[] col =
                        CassandraKeyValueServices.decomposeName(e.getValue()).getLhSide();
                // If we read a version of the cell before our start timestamp, it will be the most recent version
                // readable to us and we can continue to the next column. Otherwise we have to continue reading
                // this column.
                Map<Cell, Value> rowResult = results.get(row);
                boolean completedCell = (rowResult != null) && rowResult.containsKey(Cell.create(row, col));
                boolean endOfRange = isEndOfColumnRange(
                        completedCell, col, firstPage.getRowsToRawColumnCount().get(row), batchColumnRangeSelection);
                if (!endOfRange) {
                    byte[] nextCol = getNextColumnRangeColumn(completedCell, col);
                    incompleteRowsToNextColumns.put(row, nextCol);
                }
            }

            Map<byte[], RowColumnRangeIterator> ret = Maps.newHashMapWithExpectedSize(rows.size());
            for (byte[] row : rowsToLastCompositeColumns.keySet()) {
                Iterator<Map.Entry<Cell, Value>> resultIterator;
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
                    BatchColumnRangeSelection newColumnRange = BatchColumnRangeSelection.create(
                            nextCol, batchColumnRangeSelection.getEndCol(), batchColumnRangeSelection.getBatchHint());
                    ret.put(
                            row,
                            new LocalRowColumnRangeIterator(Iterators.concat(
                                    resultIterator, getRowColumnRange(host, tableRef, row, newColumnRange, startTs))));
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

    private RowColumnRangeExtractor.RowColumnRangeResult getRowsColumnRangeForSingleHost(
            InetSocketAddress host,
            TableReference tableRef,
            List<byte[]> rows,
            BatchColumnRangeSelection batchColumnRangeSelection,
            long startTs) {
        try {
            return clientPool.runWithRetryOnHost(
                    host,
                    new FunctionCheckedException<
                            CassandraClient, RowColumnRangeExtractor.RowColumnRangeResult, Exception>() {
                        @Override
                        public RowColumnRangeExtractor.RowColumnRangeResult apply(CassandraClient client)
                                throws Exception {
                            Range range = createColumnRange(
                                    batchColumnRangeSelection.getStartCol(),
                                    batchColumnRangeSelection.getEndCol(),
                                    startTs);
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

    private Iterator<Map.Entry<Cell, Value>> getRowColumnRange(
            InetSocketAddress host,
            TableReference tableRef,
            byte[] row,
            BatchColumnRangeSelection batchColumnRangeSelection,
            long startTs) {
        return ClosableIterators.wrap(
                new AbstractPagingIterable<
                        Map.Entry<Cell, Value>, TokenBackedBasicResultsPage<Map.Entry<Cell, Value>, byte[]>>() {
                    @Override
                    protected TokenBackedBasicResultsPage<Map.Entry<Cell, Value>, byte[]> getFirstPage()
                            throws Exception {
                        return page(batchColumnRangeSelection.getStartCol());
                    }

                    @Override
                    protected TokenBackedBasicResultsPage<Map.Entry<Cell, Value>, byte[]> getNextPage(
                            TokenBackedBasicResultsPage<Map.Entry<Cell, Value>, byte[]> previous) throws Exception {
                        return page(previous.getTokenForNextPage());
                    }

                    TokenBackedBasicResultsPage<Map.Entry<Cell, Value>, byte[]> page(final byte[] startCol)
                            throws Exception {
                        return clientPool.runWithRetryOnHost(
                                host,
                                new FunctionCheckedException<
                                        CassandraClient,
                                        TokenBackedBasicResultsPage<Map.Entry<Cell, Value>, byte[]>,
                                        Exception>() {
                                    @Override
                                    public TokenBackedBasicResultsPage<Map.Entry<Cell, Value>, byte[]> apply(
                                            CassandraClient client) throws Exception {
                                        Range range = createColumnRange(
                                                startCol, batchColumnRangeSelection.getEndCol(), startTs);
                                        Limit limit = Limit.of(batchColumnRangeSelection.getBatchHint());
                                        SlicePredicate pred = SlicePredicates.create(range, limit);

                                        ByteBuffer rowByteBuffer = ByteBuffer.wrap(row);

                                        Map<ByteBuffer, List<ColumnOrSuperColumn>> results =
                                                wrappingQueryRunner.multiget(
                                                        "getRowsColumnRange",
                                                        client,
                                                        tableRef,
                                                        ImmutableList.of(rowByteBuffer),
                                                        pred,
                                                        readConsistency);

                                        if (results.isEmpty()) {
                                            return SimpleTokenBackedResultsPage.create(
                                                    startCol, ImmutableList.of(), false);
                                        }
                                        List<ColumnOrSuperColumn> values = Iterables.getOnlyElement(results.values());
                                        if (values.isEmpty()) {
                                            return SimpleTokenBackedResultsPage.create(
                                                    startCol, ImmutableList.of(), false);
                                        }
                                        RowColumnRangeExtractor extractor = new RowColumnRangeExtractor(metricsManager);
                                        extractor.extractResults(ImmutableList.of(row), results, startTs);
                                        RowColumnRangeExtractor.RowColumnRangeResult decoded =
                                                extractor.getRowColumnRangeResult();

                                        // May be empty if all results are at ts > startTs
                                        Map<Cell, Value> ret =
                                                decoded.getResults().getOrDefault(row, Collections.emptyMap());
                                        ColumnOrSuperColumn lastColumn = values.get(values.size() - 1);
                                        byte[] lastCol = CassandraKeyValueServices.decomposeName(lastColumn.getColumn())
                                                .getLhSide();
                                        // Same idea as the getRows case to handle seeing only newer entries of a column
                                        boolean completedCell = ret.get(Cell.create(row, lastCol)) != null;
                                        if (isEndOfColumnRange(
                                                completedCell, lastCol, values.size(), batchColumnRangeSelection)) {
                                            return SimpleTokenBackedResultsPage.create(lastCol, ret.entrySet(), false);
                                        }
                                        byte[] nextCol = getNextColumnRangeColumn(completedCell, lastCol);
                                        return SimpleTokenBackedResultsPage.create(nextCol, ret.entrySet(), true);
                                    }

                                    @Override
                                    public String toString() {
                                        return "multiget_slice(" + tableRef.getQualifiedName() + ", single row, "
                                                + batchColumnRangeSelection.getBatchHint() + " batch hint)";
                                    }
                                });
                    }
                }.iterator());
    }

    private static boolean isEndOfColumnRange(
            boolean completedCell, byte[] lastCol, int numRawResults, BatchColumnRangeSelection columnRangeSelection) {
        return (numRawResults < columnRangeSelection.getBatchHint())
                || (completedCell
                        && (RangeRequests.isLastRowName(lastCol)
                                || Arrays.equals(
                                        RangeRequests.nextLexicographicName(lastCol),
                                        columnRangeSelection.getEndCol())));
    }

    private static byte[] getNextColumnRangeColumn(boolean completedCell, byte[] lastCol) {
        if (!completedCell) {
            return lastCol;
        } else {
            return RangeRequests.nextLexicographicName(lastCol);
        }
    }

    private static Range createColumnRange(byte[] startColOrEmpty, byte[] endColExlusiveOrEmpty, long startTs) {
        ByteBuffer start =
                startColOrEmpty.length == 0 ? Range.UNBOUND_START : Range.startOfColumn(startColOrEmpty, startTs);
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
     * Requires a quorum of Cassandra nodes to be reachable.
     *
     * @param tableRef the name of the table to put values into.
     * @param values map containing the key-value entries to put.
     * @param timestamp must be non-negative and not equal to {@link Long#MAX_VALUE}
     *
     * @throws AtlasDbDependencyException if fewer than a quorum of Cassandra nodes are reachable.
     */
    @Override
    public void put(final TableReference tableRef, final Map<Cell, byte[]> values, final long timestamp) {
        try {
            cellValuePutter.put(
                    "put", tableRef, KeyValueServices.toConstantTimestampValues(values.entrySet(), timestamp));
        } catch (Exception e) {
            throw Throwables.unwrapAndThrowAtlasDbDependencyException(e);
        }
    }

    /**
     * Puts values into the key-value store with individually specified timestamps. This call <i>does not</i>
     * guarantee atomicity across cells. On failure, it is possible that some of the requests have succeeded
     * (without having been rolled back). Similarly, concurrent batched requests may interleave.
     * <p>
     * Requires a quorum of Cassandra nodes to be reachable.
     *
     * @param tableRef the name of the table to put values into.
     * @param values map containing the key-value entries to put with
     *               non-negative timestamps less than {@link Long#MAX_VALUE}.
     *
     * @throws AtlasDbDependencyException if fewer than a quorum of Cassandra nodes are reachable.
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
     * Requires a quorum of Cassandra nodes to be reachable.
     *
     * @param valuesByTable map containing the key-value entries to put by table.
     * @param timestamp must be non-negative and not equal to {@link Long#MAX_VALUE}
     *
     * @throws AtlasDbDependencyException if fewer than a quorum of Cassandra nodes are reachable.
     */
    @Override
    public void multiPut(Map<TableReference, ? extends Map<Cell, byte[]>> valuesByTable, long timestamp)
            throws KeyAlreadyExistsException {
        List<TableCellAndValue> flattened = new ArrayList<>();
        for (Map.Entry<TableReference, ? extends Map<Cell, byte[]>> tableAndValues : valuesByTable.entrySet()) {
            for (Map.Entry<Cell, byte[]> entry : tableAndValues.getValue().entrySet()) {
                flattened.add(new TableCellAndValue(tableAndValues.getKey(), entry.getKey(), entry.getValue()));
            }
        }
        Map<InetSocketAddress, List<TableCellAndValue>> partitionedByHost =
                HostPartitioner.partitionByHost(clientPool, flattened, TableCellAndValue::extractRowName);

        List<Callable<Void>> callables = new ArrayList<>();
        for (Map.Entry<InetSocketAddress, List<TableCellAndValue>> entry : partitionedByHost.entrySet()) {
            callables.addAll(getMultiPutTasksForSingleHost(entry.getKey(), entry.getValue(), timestamp));
        }
        taskRunner.runAllTasksCancelOnFailure(callables);
    }

    private List<Callable<Void>> getMultiPutTasksForSingleHost(
            final InetSocketAddress host, Collection<TableCellAndValue> values, final long timestamp) {
        Iterable<List<TableCellAndValue>> partitioned = IterablePartitioner.partitionByCountAndBytes(
                values,
                getMultiPutBatchCount(),
                getMultiPutBatchSizeBytes(),
                extractTableNames(values).toString(),
                TableCellAndValue::getSize);
        List<Callable<Void>> tasks = new ArrayList<>();
        for (final List<TableCellAndValue> batch : partitioned) {
            final Set<TableReference> tableRefs = extractTableNames(batch);
            tasks.add(AnnotatedCallable.wrapWithThreadName(
                    AnnotationType.PREPEND,
                    "Atlas multiPut of " + batch.size() + " cells into " + tableRefs + " on " + host,
                    () -> multiPutForSingleHostInternal(host, tableRefs, batch, timestamp)));
        }
        return tasks;
    }

    private static Set<TableReference> extractTableNames(Iterable<TableCellAndValue> tableCellAndValues) {
        Set<TableReference> tableRefs = new HashSet<>();
        for (TableCellAndValue tableCellAndValue : tableCellAndValues) {
            tableRefs.add(tableCellAndValue.tableRef);
        }
        return tableRefs;
    }

    private Void multiPutForSingleHostInternal(
            final InetSocketAddress host,
            final Set<TableReference> tableRefs,
            final List<TableCellAndValue> batch,
            long timestamp)
            throws Exception {
        final MutationMap mutationMap = convertToMutations(batch, timestamp);
        return clientPool.runWithRetryOnHost(host, new FunctionCheckedException<CassandraClient, Void, Exception>() {
            @Override
            public Void apply(CassandraClient client) throws Exception {
                return wrappingQueryRunner.batchMutate("multiPut", client, tableRefs, mutationMap, WRITE_CONSISTENCY);
            }

            @Override
            public String toString() {
                return "batch_mutate(" + host + ", " + tableRefs + ", " + batch.size() + " values)";
            }
        });
    }

    private static MutationMap convertToMutations(List<TableCellAndValue> batch, long timestamp) {
        MutationMap mutationMap = new MutationMap();
        for (TableCellAndValue tableCellAndValue : batch) {
            Cell cell = tableCellAndValue.cell;
            Column col = CassandraKeyValueServices.createColumn(cell, Value.create(tableCellAndValue.value, timestamp));
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
     * Requires all Cassandra nodes to be reachable.
     *
     * @param tableRef the name of the table to truncate.
     *
     * @throws AtlasDbDependencyException if not all Cassandra nodes are reachable.
     * @throws RuntimeException if the table does not exist.
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
     * Requires all Cassandra nodes to be reachable.
     *
     * @param tablesToTruncate set od tables to truncate.
     *
     * @throws AtlasDbDependencyException if not all Cassandra nodes are reachable.
     * @throws RuntimeException if the table does not exist.
     */
    @Override
    public void truncateTables(final Set<TableReference> tablesToTruncate) {
        cassandraTableTruncator.truncateTables(tablesToTruncate);
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
        new CellDeleter(
                        clientPool,
                        wrappingQueryRunner,
                        DELETE_CONSISTENCY,
                        mutationTimestampProvider.getDeletionTimestampOperatorForBatchDelete())
                .delete(tableRef, keys);
    }

    @VisibleForTesting
    CfDef getCfForTable(TableReference tableRef, byte[] rawMetadata, int gcGraceSeconds) {
        return ColumnFamilyDefinitions.getCfDef(config.getKeyspaceOrThrow(), tableRef, gcGraceSeconds, rawMetadata);
    }

    // TODO(unknown): after cassandra change: handle multiRanges
    @Override
    @Idempotent
    public Map<RangeRequest, TokenBackedBasicResultsPage<RowResult<Value>, byte[]>> getFirstBatchForRanges(
            TableReference tableRef, Iterable<RangeRequest> rangeRequests, long timestamp) {
        int concurrency = config.rangesConcurrency();
        return KeyValueServices.getFirstBatchForRangesUsingGetRangeConcurrent(
                executor, this, tableRef, rangeRequests, timestamp, concurrency);
    }

    // TODO(unknown): after cassandra change: handle reverse ranges
    // TODO(unknown): after cassandra change: handle column filtering
    /**
     * For each row in the specified range, returns the most recent version strictly before timestamp. Requires a
     * quorum of Cassandra nodes to be reachable.
     *
     * Remember to close any {@link ClosableIterator}s you get in a finally block.
     *
     * @param rangeRequest the range to load.
     * @param timestamp specifies the maximum timestamp (exclusive) at which to retrieve each row's value.
     *
     * @throws AtlasDbDependencyException if fewer than a quorum of Cassandra nodes are reachable.
     */
    @Override
    @Idempotent
    public ClosableIterator<RowResult<Value>> getRange(
            TableReference tableRef, RangeRequest rangeRequest, long timestamp) {
        return rangeLoader.getRange(tableRef, rangeRequest, timestamp);
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
            TableReference tableRef, RangeRequest rangeRequest, long timestamp) {
        CandidateCellForSweepingRequest request = ImmutableCandidateCellForSweepingRequest.builder()
                .startRowInclusive(rangeRequest.getStartInclusive())
                .maxTimestampExclusive(timestamp)
                .shouldCheckIfLatestValueIsEmpty(false)
                .shouldDeleteGarbageCollectionSentinels(true)
                .build();
        return getCandidateRowsForSweeping("getRangeOfTimestamps", tableRef, request)
                .flatMap(rows -> rows)
                .map(CandidateRowForSweeping::toRowResult)
                .stopWhen(rowResult -> !rangeRequest.inRange(rowResult.getRowName()));
    }

    @Override
    public ClosableIterator<List<CandidateCellForSweeping>> getCandidateCellsForSweeping(
            TableReference tableRef, CandidateCellForSweepingRequest request) {
        return getCandidateRowsForSweeping("getCandidateCellsForSweeping", tableRef, request)
                .map(rows -> rows.stream()
                        .map(CandidateRowForSweeping::cells)
                        .flatMap(List::stream)
                        .collect(Collectors.toList()));
    }

    private ClosableIterator<List<CandidateRowForSweeping>> getCandidateRowsForSweeping(
            String kvsMethodName, TableReference tableRef, CandidateCellForSweepingRequest request) {
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

    /**
     * Returns a sorted list of row keys in the specified range; see
     * {@link CassandraKeyValueService#getRowKeysInRange(TableReference, byte[], byte[], int)}.
     *
     * Implementation specific: this method specifically does not read any of the columns and can therefore be used
     * in the presence of wide rows. However, as a side-effect, it may return row where the row only contains Cassandra
     * tombstones.
     */
    @Override
    public List<byte[]> getRowKeysInRange(TableReference tableRef, byte[] startRow, byte[] endRow, int maxResults) {
        RowGetter rowGetter = new RowGetter(clientPool, queryRunner, ConsistencyLevel.QUORUM, tableRef);
        return rowGetter.getRowKeysInRange(startRow, endRow, maxResults);
    }

    private CqlExecutor newInstrumentedCqlExecutor() {
        return AtlasDbMetrics.instrument(
                metricsManager.getRegistry(), CqlExecutor.class, new CqlExecutorImpl(clientPool, ConsistencyLevel.ALL));
    }

    /**
     * Drop the table, and also delete its table metadata. Requires a quorum of Cassandra nodes to be reachable.
     *
     * @param tableRef the name of the table to drop.
     *
     * @throws AtlasDbDependencyException if fewer than a quorum of Cassandra nodes are reachable, or the cluster
     * cannot come to an agreement on schema versions. Note that this method is not atomic: if quorum is lost during
     * its execution or Cassandra nodes fail to settle on a schema version after the Cassandra schema is mutated, we
     * may drop the tables, but fail to to persist the changes to the _metadata table.
     *
     * @throws UncheckedExecutionException if there are multiple schema mutation lock tables.
     */
    @Override
    public void dropTable(final TableReference tableRef) {
        dropTables(ImmutableSet.of(tableRef));
    }

    /**
     * Drop the tables, and also delete their table metadata. Requires a quorum of Cassandra nodes to be reachable.
     * <p>
     * Main gains here vs. dropTable:
     *    - problems excepting, we will basically be serializing a rapid series of schema changes
     *      through a single host checked out from the client pool, so reduced chance of schema disagreement issues
     *    - client-side in-memory lock to prevent misbehaving callers from shooting themselves in the foot
     *    - one less round trip
     *
     * @param tablesToDrop the set of tables to drop.
     *
     * @throws AtlasDbDependencyException if fewer than a quorum of Cassandra nodes are reachable, or the cluster
     * cannot come to an agreement on schema versions. Note that this method is not atomic: if quorum is lost during
     * its execution or Cassandra nodes fail to settle on a schema version after the Cassandra schema is mutated, we
     * may drop the tables, but fail to to persist the changes to the _metadata table.
     * @throws UncheckedExecutionException if there are multiple schema mutation lock tables.
     */
    @Override
    public void dropTables(final Set<TableReference> tablesToDrop) {
        cassandraTableDropper.dropTables(tablesToDrop);
    }

    /**
     * Creates a table with the specified name. If the table already exists, no action is performed
     * (the table is left in its current state). Requires a quorum of Cassandra nodes to be reachable.
     *
     * @param tableRef the name of the table to create.
     * @param metadata the metadata of the table to create.
     *
     * @throws AtlasDbDependencyException if fewer than a quorum of Cassandra nodes are reachable, or the cluster
     * cannot come to an agreement on schema versions. Note that this method is not atomic: if quorum is lost during
     * its execution or Cassandra nodes fail to settle on a schema version after the Cassandra schema is mutated, we
     * may fail to persist the changes to the _metadata table.
     * @throws UncheckedExecutionException if there are multiple schema mutation lock tables.
     */
    @Override
    public void createTable(final TableReference tableRef, final byte[] metadata) {
        createTables(ImmutableMap.of(tableRef, metadata));
    }

    /**
     * Creates a table with the specified name. If the table already exists, no action is performed
     * (the table is left in its current state).
     * <p>
     * Requires a quorum of Cassandra nodes to be up and available.
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
     * @param tablesToMetadata a mapping of names of tables to create to their respective metadata.
     *
     * @throws AtlasDbDependencyException if fewer than a quorum of Cassandra nodes are reachable, or the cluster
     * cannot come to an agreement on schema versions. Note that this method is not atomic: if quorum is lost during
     * its execution or Cassandra nodes fail to settle on a schema version after the Cassandra schema is mutated, we
     * may fail to persist the changes to the _metadata table.
     * @throws UncheckedExecutionException if there are multiple schema mutation lock tables.
     */
    @Override
    public void createTables(final Map<TableReference, byte[]> tablesToMetadata) {
        Map<TableReference, byte[]> tablesToCreate = tableMetadata.filterOutExistingTables(tablesToMetadata);
        Map<TableReference, byte[]> tablesToAlter = tableMetadata.filterOutNoOpMetadataChanges(tablesToMetadata);

        boolean onlyMetadataChangesAreForNewTables = tablesToAlter.keySet().equals(tablesToCreate.keySet());
        boolean putMetadataWillNeedASchemaChange = !onlyMetadataChangesAreForNewTables;

        if (!tablesToCreate.isEmpty()) {
            LoggingArgs.SafeAndUnsafeTableReferences safeAndUnsafe = LoggingArgs.tableRefs(tablesToCreate.keySet());
            log.info("Creating tables {} and {}", safeAndUnsafe.safeTableRefs(), safeAndUnsafe.unsafeTableRefs());
            cassandraTableCreator.createTables(tablesToCreate);
        }
        internalPutMetadataForTables(tablesToAlter, putMetadataWillNeedASchemaChange);
    }

    /**
     * Return the list of tables stored in this key value service. Requires a quorum of Cassandra nodes to be reachable
     * and agree on schema versions.
     * <p>
     * This will not contain the names of any hidden tables (e. g., the _metadata table).
     *
     * @return a set of TableReferences (table names) for all the visible tables
     *
     * @throws AtlasDbDependencyException if fewer than a quorum of Cassandra nodes are reachable, or the cluster
     * cannot come to an agreement on schema versions.
     */
    @Override
    public Set<TableReference> getAllTableNames() {
        return cassandraTables
                .getTableReferencesWithoutFiltering()
                .filter(tr -> !HiddenTables.isHidden(tr))
                .collect(Collectors.toSet());
    }

    /**
     * Gets the metadata for a given table. Also useful for checking to see if a table exists. Requires a quorum of
     * Cassandra nodes to be reachable.
     *
     * @param tableRef the name of the table to get metadata for.
     *
     * @return a byte array representing the metadata for the table. Array is empty if no table
     * with the given name exists. Consider {@link TableMetadata#BYTES_HYDRATOR} for hydrating.
     *
     * @throws AtlasDbDependencyException if fewer than a quorum of Cassandra nodes are reachable.
     */
    @Override
    public byte[] getMetadataForTable(TableReference tableRef) {
        // try and get with a single-key lookup
        String lowerCaseTableName = tableRef.getQualifiedName().toLowerCase();
        Map<Cell, Value> rows = getRows(
                AtlasDbConstants.DEFAULT_METADATA_TABLE,
                ImmutableSet.of(lowerCaseTableName.getBytes(StandardCharsets.UTF_8)),
                ColumnSelection.all(),
                Long.MAX_VALUE);

        if (!rows.isEmpty()) {
            return Iterables.getOnlyElement(rows.values()).getContents();
        }

        // if unsuccessful with fast code-path, we need to check if this table exists but was written at a key
        // before we started enforcing only writing lower-case canonicalised versions of keys
        return Optional.ofNullable(getMetadataForTables().get(tableRef)).orElse(AtlasDbConstants.EMPTY_TABLE_METADATA);
    }

    private static boolean matchingIgnoreCase(@Nullable TableReference t1, TableReference t2) {
        if (t1 != null) {
            return t1.getQualifiedName()
                    .toLowerCase()
                    .equals(t2.getQualifiedName().toLowerCase());
        } else {
            return t2 == null;
        }
    }

    /**
     * Gets the metadata for all non-hidden tables. Requires a quorum of Cassandra nodes to be reachable.
     *
     * @return a mapping of table names to their respective metadata in form of a byte array.  Consider
     * {@link TableMetadata#BYTES_HYDRATOR} for hydrating.
     *
     * @throws AtlasDbDependencyException if fewer than a quorum of Cassandra nodes are available.
     */
    @Override
    public Map<TableReference, byte[]> getMetadataForTables() {
        return tableMetadata.getMetadataForTables();
    }

    /**
     * Records the specified metadata for a given table. Requires a quorum of Cassandra nodes to be reachable.
     *
     * @param tableRef the name of the table to record metadata for.
     * @param meta a byte array representing the metadata to record.
     *
     * @throws AtlasDbDependencyException if fewer than a quorum of Cassandra nodes are reachable, or the cluster
     * cannot come to an agreement on schema versions. Note that this method is not atomic: if quorum is lost during
     * its execution or Cassandra nodes fail to settle on a schema version after the Cassandra schema is mutated, we
     * may fail to persist the changes to the _metadata table.
     */
    @Override
    public void putMetadataForTable(final TableReference tableRef, final byte[] meta) {
        putMetadataForTables(ImmutableMap.of(tableRef, meta));
    }

    /**
     * For each specified table records the respective metadata. Requires a quorum of Cassandra nodes to be reachable.
     *
     * @param tableRefToMetadata a mapping from each table's name to the respective byte array representing
     * the metadata to record.
     *
     * @throws AtlasDbDependencyException if fewer than a quorum of Cassandra nodes are reachable, or the cluster
     * cannot come to an agreement on schema versions. Note that this method is not atomic: if quorum is lost during
     * its execution or Cassandra nodes fail to settle on a schema version after the Cassandra schema is mutated, we
     * may fail to persist the changes to the _metadata table.
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
        Map<Cell, Value> existingMetadataAtNewName = get(
                AtlasDbConstants.DEFAULT_METADATA_TABLE,
                tableRefToNewCell.values().stream()
                        .collect(Collectors.toMap(Functions.identity(), Functions.constant(Long.MAX_VALUE))));

        Map<Cell, Value> existingMetadataAtOldName = get(
                AtlasDbConstants.DEFAULT_METADATA_TABLE,
                tableRefToOldCell.values().stream()
                        .collect(Collectors.toMap(Functions.identity(), Functions.constant(Long.MAX_VALUE))));

        final Map<Cell, byte[]> updatedMetadata = new HashMap<>();
        final Set<CfDef> updatedCfs = new HashSet<>();

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
                        existingMetadataAtOldName
                                .get(tableRefToOldCell.get(tableRef))
                                .getContents(),
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

    private static boolean metadataIsDifferent(byte[] existingMetadata, byte[] requestMetadata) {
        return !Arrays.equals(existingMetadata, requestMetadata);
    }

    private void putMetadataAndMaybeAlterTables(
            boolean possiblyNeedToPerformSettingsChanges, Map<Cell, byte[]> newMetadata, Collection<CfDef> updatedCfs) {
        try {
            clientPool.runWithRetry(client -> {
                if (possiblyNeedToPerformSettingsChanges) {
                    for (CfDef cf : updatedCfs) {
                        client.system_update_column_family(cf);
                    }

                    CassandraKeyValueServices.waitForSchemaVersions(
                            config.schemaMutationTimeoutMillis(),
                            client,
                            schemaChangeDescriptionForPutMetadataForTables(updatedCfs));
                }
                // Done with actual schema mutation, push the metadata
                put(AtlasDbConstants.DEFAULT_METADATA_TABLE, newMetadata, System.currentTimeMillis());
                return null;
            });
        } catch (Exception e) {
            throw Throwables.unwrapAndThrowAtlasDbDependencyException(e);
        }
    }

    private static String schemaChangeDescriptionForPutMetadataForTables(Collection<CfDef> updatedCfs) {
        String tables = updatedCfs.stream()
                .map(CassandraKeyValueServices::tableReferenceFromCfDef)
                .map(Object::toString)
                .collect(Collectors.toList())
                .toString();
        return String.format(
                "after updating the column family for tables %s in a call to put metadata for tables", tables);
    }

    @Override
    public void deleteRange(final TableReference tableRef, final RangeRequest range) {
        if (range.equals(RangeRequest.all())) {
            try {
                cassandraTableTruncator.truncateTables(ImmutableSet.of(tableRef));
            } catch (AtlasDbDependencyException e) {
                log.info(
                        "Tried to make a deleteRange({}, RangeRequest.all())"
                                + " into a more garbage-cleanup friendly truncate(), but this failed.",
                        LoggingArgs.tableRef(tableRef),
                        e);

                super.deleteRange(tableRef, range);
            }
        } else if (isForSingleRow(range.getStartInclusive(), range.getEndExclusive())) {
            try {
                long timestamp = mutationTimestampProvider.getRemoveTimestamp();
                byte[] row = range.getStartInclusive();
                clientPool.runWithRetry(client -> {
                    client.remove("deleteRange", tableRef, row, timestamp, DELETE_CONSISTENCY);
                    return null;
                });
            } catch (RetryLimitReachedException e) {
                throw CassandraUtils.wrapInIceForDeleteOrRethrow(e);
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
    public void deleteRows(TableReference tableRef, Iterable<byte[]> rows) {
        Set<ByteBuffer> actualKeys = StreamSupport.stream(rows.spliterator(), false)
                .map(ByteBuffer::wrap)
                .collect(Collectors.toSet());
        if (actualKeys.isEmpty()) {
            return;
        }
        long timestamp = mutationTimestampProvider.getRemoveTimestamp();

        Map<ByteBuffer, Map<String, List<Mutation>>> mutationMap = KeyedStream.of(actualKeys)
                .map(row -> new Deletion().setTimestamp(timestamp))
                .map(deletion -> new Mutation().setDeletion(deletion))
                .map(mutation -> keyMutationMapByColumnFamily(tableRef, mutation))
                .collectToMap();

        try {
            clientPool.runWithRetry(client -> {
                client.batch_mutate("deleteRows", mutationMap, DELETE_CONSISTENCY);
                return null;
            });
        } catch (RetryLimitReachedException e) {
            throw CassandraUtils.wrapInIceForDeleteOrRethrow(e);
        } catch (TException e) {
            throw Throwables.unwrapAndThrowAtlasDbDependencyException(e);
        }
    }

    private static Map<String, List<Mutation>> keyMutationMapByColumnFamily(
            TableReference tableRef, Mutation mutation) {
        return ImmutableMap.of(AbstractKeyValueService.internalTableName(tableRef), ImmutableList.of(mutation));
    }

    @Override
    public void deleteAllTimestamps(TableReference tableRef, Map<Cell, TimestampRangeDelete> deletes) {
        new CellRangeDeleter(
                        clientPool,
                        wrappingQueryRunner,
                        DELETE_CONSISTENCY,
                        mutationTimestampProvider::getRangeTombstoneTimestamp)
                .deleteAllTimestamps(tableRef, deletes);
    }

    /**
     * Performs non-destructive cleanup when the KVS is no longer needed.
     */
    @Override
    public void close() {
        clientPool.shutdown();
        asyncKeyValueService.ifPresent(AsyncKeyValueService::close);
        super.close();
    }

    /**
     * Adds a value with timestamp = Value.INVALID_VALUE_TIMESTAMP to each of the given cells. If
     * a value already exists at that time stamp, nothing is written for that cell.
     * <p>
     * Requires a quorum of Cassandra nodes to be reachable.
     *
     * @param tableRef the name of the table to add the value to.
     * @param cells a set of cells to store the values in.
     *
     * @throws AtlasDbDependencyException if fewer than a quorum of Cassandra nodes are reachable.
     */
    @Override
    public void addGarbageCollectionSentinelValues(TableReference tableRef, Iterable<Cell> cells) {
        try {
            final Value value = Value.create(PtBytes.EMPTY_BYTE_ARRAY, Value.INVALID_VALUE_TIMESTAMP);
            cellValuePutter.putWithOverriddenTimestamps(
                    "addGarbageCollectionSentinelValues",
                    tableRef,
                    Iterables.transform(cells, cell -> Maps.immutableEntry(cell, value)));
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
     * @throws AtlasDbDependencyException if not all Cassandra nodes are reachable.
     */
    @Override
    public Multimap<Cell, Long> getAllTimestamps(TableReference tableRef, Set<Cell> cells, long ts) {
        return cellLoader.getAllTimestamps(tableRef, cells, ts, DELETE_CONSISTENCY);
    }

    /**
     * Puts values into the key-value store. This call <i>does not</i> guarantee
     * atomicity across cells. On failure, it is possible that some of the requests will
     * have succeeded (without having been rolled back). Similarly, concurrent batched requests may
     * interleave.  However, concurrent writes to the same Cell will not both report success.
     * One of them will throw {@link KeyAlreadyExistsException}.
     * <p>
     * Requires a quorum of Cassandra nodes to be reachable.
     *
     * @param tableRef the name of the table to put values into.
     * @param values map containing the key-value entries to put.
     *
     * @throws AtlasDbDependencyException if fewer than a quorum of Cassandra nodes are reachable.
     * @throws KeyAlreadyExistsException if you are putting a Cell with the same timestamp as one that already exists.
     */
    @Override
    public void putUnlessExists(final TableReference tableRef, final Map<Cell, byte[]> values)
            throws KeyAlreadyExistsException {
        try {
            Optional<KeyAlreadyExistsException> failure = clientPool.runWithRetry(client -> {
                Map<ByteString, Map<Cell, byte[]>> partitionedEntries = partitionPerRow(values);

                for (Map.Entry<ByteString, Map<Cell, byte[]>> partition : partitionedEntries.entrySet()) {
                    CASResult casResult =
                            putUnlessExistsSinglePartition(tableRef, client, partition.getKey(), partition.getValue());
                    if (!casResult.isSuccess()) {
                        return Optional.of(new KeyAlreadyExistsException(
                                String.format("The cells in table %s already exist.", tableRef.getQualifiedName()),
                                casResult.getCurrent_values().stream()
                                        .map(column -> Cell.create(
                                                partition.getKey().toByteArray(),
                                                CassandraKeyValueServices.decompose(column.bufferForName()).lhSide))
                                        .collect(Collectors.toList())));
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

    public static Map<ByteString, Map<Cell, byte[]>> partitionPerRow(Map<Cell, byte[]> values) {
        return values.entrySet().stream()
                .collect(Collectors.groupingBy(
                        entry -> ByteString.copyFrom(entry.getKey().getRowName()),
                        Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)));
    }

    private static CASResult putUnlessExistsSinglePartition(
            TableReference tableRef, CassandraClient client, ByteString row, Map<Cell, byte[]> partition)
            throws TException {
        return client.put_unless_exists(
                tableRef,
                ByteBuffer.wrap(row.toByteArray()),
                partition.entrySet().stream()
                        .map(CassandraKeyValueServiceImpl::prepareColumnForPutUnlessExists)
                        .collect(Collectors.toList()),
                ConsistencyLevel.SERIAL,
                WRITE_CONSISTENCY);
    }

    private static Column prepareColumnForPutUnlessExists(Map.Entry<Cell, byte[]> insertion) {
        return new Column(CassandraKeyValueServices.makeCompositeBuffer(
                        insertion.getKey().getColumnName(),
                        // Atlas timestamp
                        CassandraConstants.CAS_TABLE_TIMESTAMP))
                // Cassandra timestamp
                .setTimestamp(CassandraConstants.CAS_TABLE_TIMESTAMP)
                .setValue(insertion.getValue());
    }

    @Override
    public CheckAndSetCompatibility getCheckAndSetCompatibility() {
        return CheckAndSetCompatibility.supportedBuilder()
                .supportsDetailOnFailure(true)
                .consistentOnFailure(false)
                .build();
    }

    /**
     * Performs a check-and-set into the key-value store.
     * Please see {@link CheckAndSetRequest} for information about how to create this request,
     * and {@link KeyValueService} for more detailed documentation.
     * <p>
     * Does not require all Cassandra nodes to be up and available, works as long as quorum is achieved.
     *
     * @param request the request, including table, cell, old value and new value.
     * @throws CheckAndSetException if the stored value for the cell was not as expected.
     */
    @Override
    public void checkAndSet(final CheckAndSetRequest request) throws CheckAndSetException {
        try {
            CheckAndSetResult<ByteString> casResult =
                    clientPool.runWithRetry(client -> checkAndSetRunner.executeCheckAndSet(client, request));
            if (!casResult.successful()) {
                List<byte[]> currentValues = casResult.existingValues().stream()
                        .map(ByteString::toByteArray)
                        .collect(Collectors.toList());

                throw new CheckAndSetException(
                        request.cell(), request.table(), request.oldValue().orElse(null), currentValues);
            }
        } catch (CheckAndSetException e) {
            throw e;
        } catch (Exception e) {
            throw Throwables.unwrapAndThrowAtlasDbDependencyException(e);
        }
    }

    @Override
    public void compactInternally(TableReference tableRef) {
        log.info(
                "Called compactInternally on {}, but this is a no-op for Cassandra KVS."
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

    private static boolean isClusterQuorumAvaialble(ClusterAvailabilityStatus clusterStatus) {
        return clusterStatus.equals(ClusterAvailabilityStatus.ALL_AVAILABLE)
                || clusterStatus.equals(ClusterAvailabilityStatus.QUORUM_AVAILABLE);
    }

    private boolean doesConfigReplicationFactorMatchWithCluster() {
        return clientPool.runWithRetry(client -> {
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

    /**
     * Asynchronously gets values from the cassandra key-value store.
     *
     * @param tableRef the name of the table to retrieve values from.
     * @param timestampByCell specifies, for each row, the maximum timestamp (exclusive) at which to
     *        retrieve that rows's value.
     * @return listenable future map of retrieved values. Values which do not exist (either
     *         because they were deleted or never created in the first place)
     *         are simply not returned.
     */
    @Override
    public ListenableFuture<Map<Cell, Value>> getAsync(TableReference tableRef, Map<Cell, Long> timestampByCell) {
        if (timestampByCell.isEmpty()) {
            log.info("Attempted get with no specified cells", LoggingArgs.tableRef(tableRef));
            return Futures.immediateFuture(ImmutableMap.of());
        }

        return asyncKeyValueService
                .map(asyncKvs -> asyncKvs.getAsync(tableRef, timestampByCell))
                .orElseGet(() -> Futures.immediateFuture(this.get(tableRef, timestampByCell)));
    }

    private static class TableCellAndValue {

        private static byte[] extractRowName(TableCellAndValue input) {
            return input.cell.getRowName();
        }

        private static Long getSize(TableCellAndValue input) {
            return input.value.length + Cells.getApproxSizeOfCell(input.cell);
        }

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
