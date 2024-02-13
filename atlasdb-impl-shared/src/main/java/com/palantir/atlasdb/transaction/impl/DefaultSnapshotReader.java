/*
 * (c) Copyright 2024 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.transaction.impl;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Timer;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Streams;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.AtlasDbMetricNames;
import com.palantir.atlasdb.futures.AtlasFutures;
import com.palantir.atlasdb.keyvalue.api.BatchColumnRangeSelection;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.ColumnSelection;
import com.palantir.atlasdb.keyvalue.api.KeyAlreadyExistsException;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.keyvalue.impl.Cells;
import com.palantir.atlasdb.logging.LoggingArgs;
import com.palantir.atlasdb.table.description.SweeperStrategy;
import com.palantir.atlasdb.transaction.api.Transaction.TransactionType;
import com.palantir.atlasdb.transaction.api.TransactionFailedRetriableException;
import com.palantir.atlasdb.transaction.api.TransactionReadSentinelBehavior;
import com.palantir.atlasdb.transaction.impl.metrics.TableLevelMetricsController;
import com.palantir.atlasdb.transaction.impl.metrics.TransactionMetrics;
import com.palantir.atlasdb.transaction.service.TransactionService;
import com.palantir.atlasdb.util.MetricsManager;
import com.palantir.common.annotation.Output;
import com.palantir.common.base.BatchingVisitable;
import com.palantir.common.streams.KeyedStream;
import com.palantir.logsafe.Preconditions;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.UnsafeArg;
import com.palantir.logsafe.exceptions.SafeIllegalArgumentException;
import com.palantir.logsafe.exceptions.SafeRuntimeException;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.eclipse.collections.api.factory.primitive.LongSets;
import org.eclipse.collections.api.map.primitive.LongLongMap;
import org.eclipse.collections.api.set.primitive.ImmutableLongSet;
import org.eclipse.collections.api.set.primitive.LongSet;

public class DefaultSnapshotReader implements SnapshotReader {

    private static final SafeLogger log = SafeLoggerFactory.get(SnapshotTransaction.class);

    @VisibleForTesting
    static final int MAX_POST_FILTERING_ITERATIONS = 200;

    // Config options
    // This really shouldn't be here.
    private final boolean allowHiddenTableAccess;

    // Actual state

    private final Supplier<Long> timestampSupplier;
    private final TransactionService defaultTransactionService;

    private final TimestampsLoader timestampsLoader;

    private final TransactionReadSentinelBehavior readSentinelBehavior;

    private final CellsLoadedCallback cellsLoadedCallback;

    private final CellStore cellStore;

    private final DeleteExecutor deleteExecutor;

    private final MetricsManager metricsManager;
    private final TableLevelMetricsController tableLevelMetricsController;

    private final TransactionMetrics transactionMetrics;

    public DefaultSnapshotReader(
            boolean allowHiddenTableAccess,
            Supplier<Long> timestampSupplier,
            TransactionService defaultTransactionService,
            TimestampsLoader timestampsLoader,
            TransactionReadSentinelBehavior readSentinelBehavior,
            CellsLoadedCallback cellsLoadedCallback,
            CellStore cellStore,
            DeleteExecutor deleteExecutor,
            MetricsManager metricsManager,
            TableLevelMetricsController tableLevelMetricsController,
            TransactionMetrics transactionMetrics) {
        this.allowHiddenTableAccess = allowHiddenTableAccess;
        this.timestampSupplier = timestampSupplier;
        this.defaultTransactionService = defaultTransactionService;
        this.timestampsLoader = timestampsLoader;
        this.readSentinelBehavior = readSentinelBehavior;
        this.cellsLoadedCallback = cellsLoadedCallback;
        this.cellStore = cellStore;
        this.deleteExecutor = deleteExecutor;
        this.metricsManager = metricsManager;
        this.tableLevelMetricsController = tableLevelMetricsController;
        this.transactionMetrics = transactionMetrics;
    }

    @Override
    public Map<Cell, byte[]> get(TableReference tableRef, Set<Cell> cells) {
        return AtlasFutures.getUnchecked(getFromKeyValueService(tableRef, cells));
    }

    @Override
    public NavigableMap<byte[], RowResult<byte[]>> getRows(
            TableReference tableRef, Iterable<byte[]> rows, ColumnSelection columnSelection) {
        return null;
    }

    @Override
    public BatchingVisitable<RowResult<byte[]>> getRange(TableReference tableRef, RangeRequest rangeRequest) {
        return null;
    }

    @Override
    public Map<byte[], BatchingVisitable<Entry<Cell, byte[]>>> getRowsColumnRange(
            TableReference tableRef, Iterable<byte[]> rows, BatchColumnRangeSelection columnRangeSelection) {
        return null;
    }

    @Override
    public Iterator<Entry<Cell, byte[]>> getSortedColumns(
            TableReference tableRef, Iterable<byte[]> rows, BatchColumnRangeSelection batchColumnRangeSelection) {
        return null;
    }

    // Layers:
    // * SnapshotReader(MVCC reads): filtering and postfiltering based on timestamps. Needs to know when to block
    // waiting for a commit.
    // timestamp, if it's reading with a commit timestamp, needs to know to ignore its own writes.
    // Self-writes.
    // Caching.

    // No lock checks
    // Can read from cache.
    // Needs to lookup timestamps
    // Where should we handle sweep sentinel deletions, filtering, and aborting other transactions.

    /**
     * This will load the given keys from the underlying key value service and apply postFiltering so we have snapshot
     * isolation.  If the value in the key value service is the empty array this will be included here and needs to be
     * filtered out.
     */
    private ListenableFuture<Map<Cell, byte[]>> getFromKeyValueService(TableReference tableRef, Set<Cell> cells) {
        Map<Cell, Long> toRead = Cells.constantValueMap(cells, getStartTimestamp());
        ListenableFuture<Collection<Entry<Cell, byte[]>>> postFilteredResults = Futures.transformAsync(
                cellStore.getAsync(tableRef, toRead),
                rawResults -> getWithPostFilteringAsync(tableRef, rawResults, Value.GET_VALUE),
                MoreExecutors.directExecutor());

        return Futures.transform(postFilteredResults, ImmutableMap::copyOf, MoreExecutors.directExecutor());
    }

    private <T> ListenableFuture<Collection<Map.Entry<Cell, T>>> getWithPostFilteringAsync(
            TableReference tableRef, Map<Cell, Value> rawResults, Function<Value, T> transformer) {
        long bytes = 0;
        for (Map.Entry<Cell, Value> entry : rawResults.entrySet()) {
            bytes += entry.getValue().getContents().length + Cells.getApproxSizeOfCell(entry.getKey());
        }
        if (bytes > TransactionConstants.WARN_LEVEL_FOR_QUEUED_BYTES && log.isWarnEnabled()) {
            log.warn(
                    "A single get had quite a few bytes: {} for table {}. The number of results was {}. "
                            + "Enable debug logging for more information.",
                    SafeArg.of("numBytes", bytes),
                    LoggingArgs.tableRef(tableRef),
                    SafeArg.of("numResults", rawResults.size()));
            if (log.isDebugEnabled()) {
                log.debug(
                        "The first 10 results of your request were {}.",
                        UnsafeArg.of("results", Iterables.limit(rawResults.entrySet(), 10)),
                        new SafeRuntimeException("This exception and stack trace are provided for debugging purposes"));
            }
            getHistogram(AtlasDbMetricNames.SNAPSHOT_TRANSACTION_TOO_MANY_BYTES_READ, tableRef)
                    .update(bytes);
        }

        getCounter(AtlasDbMetricNames.SNAPSHOT_TRANSACTION_CELLS_READ, tableRef).inc(rawResults.size());

        Collection<Map.Entry<Cell, T>> resultsAccumulator = new ArrayList<>();

        if (AtlasDbConstants.HIDDEN_TABLES.contains(tableRef)) {
            Preconditions.checkState(allowHiddenTableAccess, "hidden tables cannot be read in this transaction");
            // hidden tables are used outside of the transaction protocol, and in general have invalid timestamps,
            // so do not apply post-filtering as post-filtering would rollback (actually delete) the data incorrectly
            // this case is hit when reading a hidden table from console
            for (Map.Entry<Cell, Value> e : rawResults.entrySet()) {
                resultsAccumulator.add(Maps.immutableEntry(e.getKey(), transformer.apply(e.getValue())));
            }
            return Futures.immediateFuture(resultsAccumulator);
        }

        return Futures.transformAsync(
                Futures.immediateFuture(rawResults),
                resultsToPostFilter ->
                        getWithPostFilteringIterate(tableRef, resultsToPostFilter, resultsAccumulator, transformer),
                MoreExecutors.directExecutor());
    }

    private <T> ListenableFuture<Collection<Map.Entry<Cell, T>>> getWithPostFilteringIterate(
            TableReference tableReference,
            Map<Cell, Value> resultsToPostFilter,
            Collection<Map.Entry<Cell, T>> resultsAccumulator,
            Function<Value, T> transformer) {
        return Futures.transformAsync(
                Futures.immediateFuture(resultsToPostFilter),
                results -> {
                    int iterations = 0;
                    Map<Cell, Value> remainingResultsToPostFilter = results;
                    while (!remainingResultsToPostFilter.isEmpty()) {
                        remainingResultsToPostFilter = AtlasFutures.getUnchecked(getWithPostFilteringInternal(
                                tableReference, remainingResultsToPostFilter, resultsAccumulator, transformer));
                        Preconditions.checkState(
                                ++iterations < MAX_POST_FILTERING_ITERATIONS,
                                "Unable to filter cells to find correct result after "
                                        + "reaching max iterations. This is likely due to aborted cells lying around,"
                                        + " or in the very rare case, could be due to transactions which constantly "
                                        + "conflict but never commit. These values will be cleaned up eventually, but"
                                        + " if the issue persists, ensure that sweep is caught up.",
                                SafeArg.of("table", tableReference),
                                SafeArg.of("maxIterations", MAX_POST_FILTERING_ITERATIONS));
                    }
                    getCounter(AtlasDbMetricNames.SNAPSHOT_TRANSACTION_CELLS_RETURNED, tableReference)
                            .inc(resultsAccumulator.size());

                    return Futures.immediateFuture(resultsAccumulator);
                },
                MoreExecutors.directExecutor());
    }

    /**
     * This will return all the key-value pairs that still need to be postFiltered.  It will output properly post
     * filtered keys to the {@code resultsCollector} output param.
     */
    private <T> ListenableFuture<Map<Cell, Value>> getWithPostFilteringInternal(
            TableReference tableRef,
            Map<Cell, Value> rawResults,
            @Output Collection<Map.Entry<Cell, T>> resultsCollector,
            Function<Value, T> transformer) {
        Set<Cell> orphanedSentinels = findOrphanedSweepSentinels(tableRef, rawResults);
        LongSet valuesStartTimestamps = getStartTimestampsForValues(rawResults.values());
        return Futures.transformAsync(
                timestampsLoader.getCommitTimestamps(tableRef, valuesStartTimestamps, true),
                commitTimestamps -> collectCellsToPostFilter(
                        tableRef, rawResults, resultsCollector, transformer, orphanedSentinels, commitTimestamps),
                MoreExecutors.directExecutor());
    }

    /**
     * A sentinel becomes orphaned if the table has been truncated between the time where the write occurred and where
     * it was truncated. In this case, there is a chance that we end up with a sentinel with no valid AtlasDB cell
     * covering it. In this case, we ignore it.
     */
    private Set<Cell> findOrphanedSweepSentinels(TableReference table, Map<Cell, Value> rawResults) {
        Set<Cell> sweepSentinels = Maps.filterValues(rawResults, DefaultSnapshotReader::isSweepSentinel)
                .keySet();
        if (sweepSentinels.isEmpty()) {
            return Collections.emptySet();
        }

        // for each sentinel, start at long max. Then iterate down with each found uncommitted value.
        // if committed value seen, stop: the sentinel is not orphaned
        // if we get back -1, the sentinel is orphaned
        Map<Cell, Long> timestampCandidates =
                new HashMap<>(cellStore.getLatestTimestamps(table, Maps.asMap(sweepSentinels, x -> Long.MAX_VALUE)));
        Set<Cell> actualOrphanedSentinels = new HashSet<>();

        while (!timestampCandidates.isEmpty()) {
            Map<SentinelType, Map<Cell, Long>> sentinelTypeToTimestamps = timestampCandidates.entrySet().stream()
                    .collect(Collectors.groupingBy(
                            entry -> entry.getValue() == Value.INVALID_VALUE_TIMESTAMP
                                    ? SentinelType.DEFINITE_ORPHANED
                                    : SentinelType.INDETERMINATE,
                            Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)));

            Map<Cell, Long> definiteOrphans = sentinelTypeToTimestamps.get(SentinelType.DEFINITE_ORPHANED);
            if (definiteOrphans != null) {
                actualOrphanedSentinels.addAll(definiteOrphans.keySet());
            }

            Map<Cell, Long> cellsToQuery = sentinelTypeToTimestamps.get(SentinelType.INDETERMINATE);
            if (cellsToQuery == null) {
                break;
            }
            Set<Long> committedStartTimestamps = KeyedStream.stream(
                            defaultTransactionService.get(cellsToQuery.values()))
                    .filter(Objects::nonNull)
                    .keys()
                    .collect(Collectors.toSet());

            Map<Cell, Long> nextTimestampCandidates = KeyedStream.stream(cellsToQuery)
                    .filter(cellStartTimestamp -> !committedStartTimestamps.contains(cellStartTimestamp))
                    .collectToMap();
            timestampCandidates = cellStore.getLatestTimestamps(table, nextTimestampCandidates);
        }

        deleteOrphanedSentinelsAsync(table, actualOrphanedSentinels);

        return actualOrphanedSentinels;
    }

    private long getStartTimestamp() {
        return timestampSupplier.get();
    }

    private static boolean isSweepSentinel(Value value) {
        return value.getTimestamp() == Value.INVALID_VALUE_TIMESTAMP;
    }

    private enum SentinelType {
        DEFINITE_ORPHANED,
        INDETERMINATE;
    }

    private <T> ListenableFuture<Map<Cell, Value>> collectCellsToPostFilter(
            TableReference tableRef,
            Map<Cell, Value> rawResults,
            @Output Collection<Map.Entry<Cell, T>> resultsCollector,
            Function<Value, T> transformer,
            Set<Cell> orphanedSentinels,
            LongLongMap commitTimestamps) {
        Map<Cell, Long> keysToReload = Maps.newHashMapWithExpectedSize(0);
        Map<Cell, Long> keysToDelete = Maps.newHashMapWithExpectedSize(0);
        ImmutableSet.Builder<Cell> keysAddedBuilder = ImmutableSet.builder();

        for (Map.Entry<Cell, Value> e : rawResults.entrySet()) {
            Cell key = e.getKey();
            Value value = e.getValue();

            if (isSweepSentinel(value)) {
                getCounter(AtlasDbMetricNames.CellFilterMetrics.INVALID_START_TS, tableRef)
                        .inc();

                // This means that this transaction started too long ago. When we do garbage collection,
                // we clean up old values, and this transaction started at a timestamp before the garbage collection.
                switch (readSentinelBehavior) {
                    case IGNORE:
                        break;
                    case THROW_EXCEPTION:
                        if (!orphanedSentinels.contains(key)) {
                            throw new TransactionFailedRetriableException("Tried to read a value that has been "
                                    + "deleted. This can be caused by hard delete transactions using the type "
                                    + TransactionType.AGGRESSIVE_HARD_DELETE
                                    + ". It can also be caused by transactions taking too long, or"
                                    + " its locks expired. Retrying it should work.");
                        }
                        break;
                    default:
                        throw new IllegalStateException("Invalid read sentinel behavior " + readSentinelBehavior);
                }
            } else {
                long theirCommitTimestamp =
                        commitTimestamps.getIfAbsent(value.getTimestamp(), TransactionConstants.FAILED_COMMIT_TS);
                if (theirCommitTimestamp == TransactionConstants.FAILED_COMMIT_TS) {
                    keysToReload.put(key, value.getTimestamp());
                    if (shouldDeleteAndRollback()) {
                        // This is from a failed transaction so we can roll it back and then reload it.
                        keysToDelete.put(key, value.getTimestamp());
                        getCounter(AtlasDbMetricNames.CellFilterMetrics.INVALID_COMMIT_TS, tableRef)
                                .inc();
                    }
                } else if (theirCommitTimestamp > getStartTimestamp()) {
                    // The value's commit timestamp is after our start timestamp.
                    // This means the value is from a transaction which committed
                    // after our transaction began. We need to try reading at an
                    // earlier timestamp.
                    keysToReload.put(key, value.getTimestamp());
                    getCounter(AtlasDbMetricNames.CellFilterMetrics.COMMIT_TS_GREATER_THAN_TRANSACTION_TS, tableRef)
                            .inc();
                } else {
                    // The value has a commit timestamp less than our start timestamp, and is visible and valid.
                    if (value.getContents().length != 0) {
                        resultsCollector.add(Maps.immutableEntry(key, transformer.apply(value)));
                        keysAddedBuilder.add(key);
                    }
                }
            }
        }
        Set<Cell> keysAddedToResults = keysAddedBuilder.build();

        if (!keysToDelete.isEmpty()) {
            // if we can't roll back the failed transactions, we should just try again
            if (!rollbackFailedTransactions(tableRef, keysToDelete, commitTimestamps, defaultTransactionService)) {
                return Futures.immediateFuture(getRemainingResults(rawResults, keysAddedToResults));
            }
        }

        if (!keysToReload.isEmpty()) {
            return Futures.transform(
                    cellStore.getAsync(tableRef, keysToReload),
                    nextRawResults -> {
                        boolean allPossibleCellsReadAndPresent = nextRawResults.size() == keysToReload.size();
                        cellsLoadedCallback.onLoaded(tableRef, getStartTimestamp(), allPossibleCellsReadAndPresent);
                        return getRemainingResults(nextRawResults, keysAddedToResults);
                    },
                    MoreExecutors.directExecutor());
        }
        return Futures.immediateFuture(ImmutableMap.of());
    }

    private boolean rollbackFailedTransactions(
            TableReference tableRef,
            Map<Cell, Long> keysToDelete,
            LongLongMap commitTimestamps,
            TransactionService transactionService) {
        ImmutableLongSet timestamps = LongSets.immutable.ofAll(keysToDelete.values());
        boolean allRolledBack = timestamps.allSatisfy(startTs -> {
            if (commitTimestamps.containsKey(startTs)) {
                long commitTs = commitTimestamps.get(startTs);
                if (commitTs != TransactionConstants.FAILED_COMMIT_TS) {
                    throw new SafeIllegalArgumentException(
                            "Cannot rollback already committed transaction",
                            SafeArg.of("startTs", startTs),
                            SafeArg.of("commitTs", commitTs));
                }
                return true;
            }
            log.warn("Rolling back transaction: {}", SafeArg.of("startTs", startTs));
            return rollbackOtherTransaction(startTs, transactionService);
        });

        if (allRolledBack) {
            deleteExecutor.delete(tableRef, keysToDelete);
        }
        return allRolledBack;
    }

    private boolean rollbackOtherTransaction(long startTs, TransactionService transactionService) {
        try {
            transactionService.putUnlessExists(startTs, TransactionConstants.FAILED_COMMIT_TS);
            transactionMetrics.rolledBackOtherTransaction().mark();
            return true;
        } catch (KeyAlreadyExistsException e) {
            log.debug(
                    "This isn't a bug but it should be very infrequent. Two transactions tried to roll back someone"
                            + " else's request with start: {}",
                    SafeArg.of("startTs", startTs),
                    new TransactionFailedRetriableException(
                            "Two transactions tried to roll back someone else's request with start: " + startTs, e));
            return false;
        }
    }

    private Map<Cell, Value> getRemainingResults(Map<Cell, Value> rawResults, Set<Cell> keysAddedToResults) {
        Map<Cell, Value> remainingResults = new HashMap<>(rawResults);
        remainingResults.keySet().removeAll(keysAddedToResults);
        return remainingResults;
    }

    private void deleteOrphanedSentinelsAsync(TableReference table, Set<Cell> actualOrphanedSentinels) {
        // TODO(jakubk): This could probably be refactored nicely.
        if (sweepQueue.getSweepStrategy(table) == SweeperStrategy.THOROUGH) {
            Map<Cell, Long> sentinels = KeyedStream.of(actualOrphanedSentinels)
                    .map(_ignore -> Value.INVALID_VALUE_TIMESTAMP)
                    .collectToMap();
            deleteExecutor.delete(table, sentinels);
        }
    }

    protected boolean shouldDeleteAndRollback() {
        Preconditions.checkNotNull(
                timelockService, "if we don't have a valid lock server we can't roll back transactions");
        return true;
    }

    private LongSet getStartTimestampsForValues(Iterable<Value> values) {
        return LongSets.immutable.withAll(Streams.stream(values).mapToLong(Value::getTimestamp));
    }

    private Timer getTimer(String name) {
        return metricsManager.registerOrGetTimer(SnapshotTransaction.class, name);
    }

    private Histogram getHistogram(String name, TableReference tableRef) {
        return metricsManager.registerOrGetTaggedHistogram(
                SnapshotTransaction.class, name, metricsManager.getTableNameTagFor(tableRef));
    }

    private Counter getCounter(String name, TableReference tableRef) {
        return tableLevelMetricsController.createAndRegisterCounter(SnapshotTransaction.class, name, tableRef);
    }
}
