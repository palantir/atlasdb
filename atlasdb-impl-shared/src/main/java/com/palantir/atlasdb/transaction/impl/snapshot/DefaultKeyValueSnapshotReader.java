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

package com.palantir.atlasdb.transaction.impl.snapshot;

import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Streams;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.cell.api.TransactionKeyValueService;
import com.palantir.atlasdb.futures.AtlasFutures;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.ColumnSelection;
import com.palantir.atlasdb.keyvalue.api.KeyAlreadyExistsException;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.keyvalue.impl.Cells;
import com.palantir.atlasdb.keyvalue.impl.RowResults;
import com.palantir.atlasdb.logging.LoggingArgs;
import com.palantir.atlasdb.tracing.TraceStatistics;
import com.palantir.atlasdb.transaction.api.CommitTimestampLoader;
import com.palantir.atlasdb.transaction.api.TransactionFailedRetriableException;
import com.palantir.atlasdb.transaction.api.metrics.KeyValueSnapshotMetricRecorder;
import com.palantir.atlasdb.transaction.api.snapshot.KeyValueSnapshotReader;
import com.palantir.atlasdb.transaction.impl.DeleteExecutor;
import com.palantir.atlasdb.transaction.impl.ReadSentinelHandler;
import com.palantir.atlasdb.transaction.impl.SnapshotTransaction;
import com.palantir.atlasdb.transaction.impl.TransactionConstants;
import com.palantir.atlasdb.transaction.impl.precommit.ReadSnapshotValidator;
import com.palantir.atlasdb.transaction.service.TransactionService;
import com.palantir.common.annotation.Output;
import com.palantir.logsafe.Preconditions;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.UnsafeArg;
import com.palantir.logsafe.exceptions.SafeIllegalArgumentException;
import com.palantir.logsafe.exceptions.SafeRuntimeException;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.function.Function;
import java.util.function.LongSupplier;
import org.eclipse.collections.api.LongIterable;
import org.eclipse.collections.api.map.primitive.LongLongMap;
import org.eclipse.collections.api.set.primitive.ImmutableLongSet;
import org.eclipse.collections.api.set.primitive.LongSet;
import org.eclipse.collections.impl.factory.primitive.LongSets;

public final class DefaultKeyValueSnapshotReader implements KeyValueSnapshotReader {
    private static final SafeLogger log = SafeLoggerFactory.get(DefaultKeyValueSnapshotReader.class);

    // TODO (jkong): Move canonical value of constant here, once post-filtering is removed from SnapshotTransaction
    private static final int MAX_POST_FILTERING_ITERATIONS = SnapshotTransaction.MAX_POST_FILTERING_ITERATIONS;

    private final TransactionKeyValueService transactionKeyValueService;
    private final TransactionService transactionService;
    private final CommitTimestampLoader commitTimestampLoader;
    private final boolean allowHiddenTableAccess;
    private final ReadSentinelHandler readSentinelHandler;
    private final LongSupplier startTimestampSupplier;
    private final ReadSnapshotValidator readSnapshotValidator;
    private final DeleteExecutor deleteExecutor;
    private final KeyValueSnapshotMetricRecorder metricRecorder;

    public DefaultKeyValueSnapshotReader(
            TransactionKeyValueService transactionKeyValueService,
            TransactionService transactionService,
            CommitTimestampLoader commitTimestampLoader,
            boolean allowHiddenTableAccess,
            ReadSentinelHandler readSentinelHandler,
            LongSupplier startTimestampSupplier,
            ReadSnapshotValidator readSnapshotValidator,
            DeleteExecutor deleteExecutor,
            KeyValueSnapshotMetricRecorder metricRecorder) {
        this.transactionKeyValueService = transactionKeyValueService;
        this.transactionService = transactionService;
        this.commitTimestampLoader = commitTimestampLoader;
        this.allowHiddenTableAccess = allowHiddenTableAccess;
        this.readSentinelHandler = readSentinelHandler;
        this.startTimestampSupplier = startTimestampSupplier;
        this.readSnapshotValidator = readSnapshotValidator;
        this.deleteExecutor = deleteExecutor;
        this.metricRecorder = metricRecorder;
    }

    @Override
    public ListenableFuture<Map<Cell, byte[]>> getAsync(TableReference tableReference, Set<Cell> cells) {
        return getInternal(tableReference, cells);
    }

    @Override
    public NavigableMap<byte[], RowResult<byte[]>> getRows(
            TableReference tableReference,
            Iterable<byte[]> rows,
            ColumnSelection columnSelection,
            ImmutableMap.Builder<Cell, byte[]> resultCollector) {
        Map<Cell, Value> rawResults = new HashMap<>(transactionKeyValueService.getRows(
                tableReference, rows, columnSelection, startTimestampSupplier.getAsLong()));
        // We don't need to do work postFiltering if we have a write locally.
        rawResults.keySet().removeAll(resultCollector.buildOrThrow().keySet());
        return filterRowResults(tableReference, rawResults, resultCollector);
    }

    private ListenableFuture<Map<Cell, byte[]>> getInternal(TableReference tableReference, Set<Cell> cells) {
        Map<Cell, Long> timestampsByCell = Cells.constantValueMap(cells, startTimestampSupplier.getAsLong());
        ListenableFuture<Collection<Map.Entry<Cell, byte[]>>> postFilteredResults = Futures.transformAsync(
                transactionKeyValueService.getAsync(tableReference, timestampsByCell),
                rawResults -> getWithPostFilteringAsync(tableReference, rawResults, Value.GET_VALUE),
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
            metricRecorder.recordManyBytesReadForTable(tableRef, bytes);
        }

        metricRecorder.recordCellsRead(tableRef, rawResults.size());

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
                                LoggingArgs.tableRef(tableReference),
                                SafeArg.of("maxIterations", MAX_POST_FILTERING_ITERATIONS));
                    }
                    metricRecorder.recordCellsReturned(tableReference, resultsAccumulator.size());
                    return Futures.immediateFuture(resultsAccumulator);
                },
                MoreExecutors.directExecutor());
    }

    private <T> ListenableFuture<Map<Cell, Value>> getWithPostFilteringInternal(
            TableReference tableRef,
            Map<Cell, Value> rawResults,
            @Output Collection<Map.Entry<Cell, T>> resultsCollector,
            Function<Value, T> transformer) {
        Set<Cell> orphans = readSentinelHandler.findAndMarkOrphanedSweepSentinelsForDeletion(tableRef, rawResults);
        LongSet valuesStartTimestamps = getStartTimestampsForValues(rawResults.values());

        return Futures.transformAsync(
                getCommitTimestamps(tableRef, valuesStartTimestamps),
                commitTimestamps -> collectCellsToPostFilter(
                        tableRef, rawResults, resultsCollector, transformer, commitTimestamps, orphans),
                MoreExecutors.directExecutor());
    }

    private <T> ListenableFuture<Map<Cell, Value>> collectCellsToPostFilter(
            TableReference tableRef,
            Map<Cell, Value> rawResults,
            @Output Collection<Map.Entry<Cell, T>> resultsCollector,
            Function<Value, T> transformer,
            LongLongMap commitTimestamps,
            Set<Cell> knownOrphans) {
        Map<Cell, Long> keysToReload = Maps.newHashMapWithExpectedSize(0);
        Map<Cell, Long> keysToDelete = Maps.newHashMapWithExpectedSize(0);
        ImmutableSet.Builder<Cell> keysAddedBuilder = ImmutableSet.builder();

        for (Map.Entry<Cell, Value> e : rawResults.entrySet()) {
            Cell key = e.getKey();
            Value value = e.getValue();

            if (isSweepSentinel(value) && !knownOrphans.contains(key)) {
                metricRecorder.recordFilteredSweepSentinel(tableRef);

                // This means that this transaction started too long ago. When we do garbage collection,
                // we clean up old values, and this transaction started at a timestamp before the garbage collection.
                readSentinelHandler.handleReadSentinel();
            } else {
                long theirCommitTimestamp =
                        commitTimestamps.getIfAbsent(value.getTimestamp(), TransactionConstants.FAILED_COMMIT_TS);
                if (theirCommitTimestamp == TransactionConstants.FAILED_COMMIT_TS) {
                    keysToReload.put(key, value.getTimestamp());

                    // This is from a failed transaction so we can roll it back and then reload it.
                    keysToDelete.put(key, value.getTimestamp());
                    metricRecorder.recordFilteredUncommittedTransaction(tableRef);
                } else if (theirCommitTimestamp > startTimestampSupplier.getAsLong()) {
                    // The value's commit timestamp is after our start timestamp.
                    // This means the value is from a transaction which committed
                    // after our transaction began. We need to try reading at an
                    // earlier timestamp.
                    keysToReload.put(key, value.getTimestamp());
                    metricRecorder.recordFilteredTransactionCommittingAfterOurStart(tableRef);
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
            if (!rollbackFailedTransactions(tableRef, keysToDelete, commitTimestamps)) {
                return Futures.immediateFuture(getRemainingResults(rawResults, keysAddedToResults));
            }
        }

        if (!keysToReload.isEmpty()) {
            // TODO (jkong): Consider removing when a decision on validating pre-commit requirements mid-read is made
            return Futures.transform(
                    transactionKeyValueService.getAsync(tableRef, keysToReload),
                    nextRawResults -> {
                        boolean allPossibleCellsReadAndPresent = nextRawResults.size() == keysToReload.size();
                        readSnapshotValidator.throwIfPreCommitRequirementsNotMetOnRead(
                                tableRef, startTimestampSupplier.getAsLong(), allPossibleCellsReadAndPresent);
                        return getRemainingResults(nextRawResults, keysAddedToResults);
                    },
                    MoreExecutors.directExecutor());
        }
        return Futures.immediateFuture(ImmutableMap.of());
    }

    private Map<Cell, Value> getRemainingResults(Map<Cell, Value> rawResults, Set<Cell> keysAddedToResults) {
        Map<Cell, Value> remainingResults = new HashMap<>(rawResults);
        remainingResults.keySet().removeAll(keysAddedToResults);
        return remainingResults;
    }

    private LongSet getStartTimestampsForValues(Iterable<Value> values) {
        return LongSets.immutable.withAll(Streams.stream(values).mapToLong(Value::getTimestamp));
    }

    private ListenableFuture<LongLongMap> getCommitTimestamps(TableReference tableRef, LongIterable startTimestamps) {
        return commitTimestampLoader.getCommitTimestamps(tableRef, startTimestamps);
    }

    /**
     * This will attempt to rollback the passed transactions.  If all are rolled back correctly this method will also
     * delete the values for the transactions that have been rolled back.
     *
     * @return false if we cannot roll back the failed transactions because someone beat us to it
     */
    private boolean rollbackFailedTransactions(
            TableReference tableRef, Map<Cell, Long> keysToDelete, LongLongMap commitTimestamps) {
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
            return rollbackOtherTransaction(startTs);
        });

        if (allRolledBack) {
            deleteExecutor.scheduleForDeletion(tableRef, keysToDelete);
        }
        return allRolledBack;
    }

    private boolean rollbackOtherTransaction(long startTs) {
        try {
            transactionService.putUnlessExists(startTs, TransactionConstants.FAILED_COMMIT_TS);
            metricRecorder.recordRolledBackOtherTransaction();
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

    private NavigableMap<byte[], RowResult<byte[]>> filterRowResults(
            TableReference tableRef, Map<Cell, Value> rawResults, ImmutableMap.Builder<Cell, byte[]> resultCollector) {
        ImmutableMap<Cell, byte[]> collected = resultCollector
                .putAll(getWithPostFilteringSync(tableRef, rawResults, Value.GET_VALUE))
                .buildOrThrow();
        Map<Cell, byte[]> filterDeletedValues = removeEmptyColumns(collected, tableRef);
        return RowResults.viewOfSortedMap(Cells.breakCellsUpByRow(filterDeletedValues));
    }

    private <T> Collection<Map.Entry<Cell, T>> getWithPostFilteringSync(
            TableReference tableRef, Map<Cell, Value> rawResults, Function<Value, T> transformer) {
        return AtlasFutures.getUnchecked(getWithPostFilteringAsync(tableRef, rawResults, transformer));
    }

    private Map<Cell, byte[]> removeEmptyColumns(Map<Cell, byte[]> unfiltered, TableReference tableReference) {
        Map<Cell, byte[]> filtered = Maps.filterValues(unfiltered, Predicates.not(Value::isTombstone));

        int emptyValues = unfiltered.size() - filtered.size();
        metricRecorder.recordFilteredEmptyValues(tableReference, emptyValues);
        TraceStatistics.incEmptyValues(emptyValues);

        return filtered;
    }

    private static boolean isSweepSentinel(Value value) {
        return value.getTimestamp() == Value.INVALID_VALUE_TIMESTAMP;
    }
}
