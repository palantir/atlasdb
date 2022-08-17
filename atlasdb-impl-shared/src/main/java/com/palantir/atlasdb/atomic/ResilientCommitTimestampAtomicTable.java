/*
 * (c) Copyright 2022 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.atomic;

import com.github.benmanes.caffeine.cache.CacheLoader;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.CheckAndSetException;
import com.palantir.atlasdb.keyvalue.api.KeyAlreadyExistsException;
import com.palantir.atlasdb.pue.PutUnlessExistsTableMetrics;
import com.palantir.atlasdb.transaction.encoding.EncodingStrategyV3;
import com.palantir.atlasdb.transaction.encoding.TwoPhaseEncodingStrategy;
import com.palantir.atlasdb.transaction.impl.TransactionStatusUtils;
import com.palantir.atlasdb.transaction.service.TransactionStatus;
import com.palantir.atlasdb.transaction.service.TransactionStatuses;
import com.palantir.common.exception.AtlasDbDependencyException;
import com.palantir.common.streams.KeyedStream;
import com.palantir.common.time.Clock;
import com.palantir.common.time.SystemClock;
import com.palantir.logsafe.Preconditions;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import com.palantir.tritium.metrics.registry.TaggedMetricRegistry;
import com.palantir.util.RateLimitedLogger;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import javax.annotation.Nonnull;
import org.immutables.value.Value;

public class ResilientCommitTimestampAtomicTable implements AtomicTable<Long, TransactionStatus> {
    private static final RateLimitedLogger log =
            new RateLimitedLogger(SafeLoggerFactory.get(ResilientCommitTimestampAtomicTable.class), 1.0 / 3600);
    private static final int TOUCH_CACHE_SIZE = 1000;
    private static final Duration COMMIT_THRESHOLD = Duration.ofSeconds(1);

    private final ConsensusForgettingStore store;
    private final EncodingStrategyV3 encodingStrategy;
    private final Supplier<Boolean> acceptStagingReadsAsCommitted;
    private final Clock clock;
    private final PutUnlessExistsTableMetrics metrics;
    private final Map<ByteBuffer, Object> rowLocks = new ConcurrentHashMap<>();
    private final AtomicLong fallbacks = new AtomicLong(0);

    private final LoadingCache<CellInfo, TransactionStatus> touchCache = Caffeine.newBuilder()
            .maximumSize(TOUCH_CACHE_SIZE)
            .build(new CacheLoader<>() {
                @Override
                @Nonnull
                public TransactionStatus load(@Nonnull CellInfo cellInfo) {
                    metrics.touchCacheLoad().time(() -> {
                        FollowUpAction followUpAction = FollowUpAction.PUT;
                        if (shouldTouch()) {
                            synchronized (
                                    rowLocks.computeIfAbsent(
                                            ByteBuffer.wrap(cellInfo.cell().getRowName()), x -> x)) {
                                followUpAction = touchAndReturn(cellInfo);
                            }
                        }
                        if (followUpAction == FollowUpAction.PUT) {
                            store.put(ImmutableMap.of(
                                    cellInfo.cell(),
                                    TwoPhaseEncodingStrategy.INSTANCE.transformStagingToCommitted(cellInfo.value())));
                        }
                    });
                    return TransactionStatusUtils.fromTimestamp(cellInfo.commitTs());
                }
            });

    private volatile Instant acceptStagingUntil = Instant.EPOCH;

    public ResilientCommitTimestampAtomicTable(
            ConsensusForgettingStore store, EncodingStrategyV3 encodingStrategy, TaggedMetricRegistry metricRegistry) {
        this(store, encodingStrategy, () -> false, metricRegistry);
    }

    public ResilientCommitTimestampAtomicTable(
            ConsensusForgettingStore store,
            EncodingStrategyV3 encodingStrategy,
            Supplier<Boolean> acceptStagingReadsAsCommitted,
            TaggedMetricRegistry metricRegistry) {
        this(store, encodingStrategy, acceptStagingReadsAsCommitted, new SystemClock(), metricRegistry);
    }

    @VisibleForTesting
    ResilientCommitTimestampAtomicTable(
            ConsensusForgettingStore store,
            EncodingStrategyV3 encodingStrategy,
            Supplier<Boolean> acceptStagingReadsAsCommitted,
            Clock clock,
            TaggedMetricRegistry metricRegistry) {
        this.store = store;
        this.encodingStrategy = encodingStrategy;
        this.acceptStagingReadsAsCommitted = acceptStagingReadsAsCommitted;
        this.clock = clock;
        this.metrics = PutUnlessExistsTableMetrics.of(metricRegistry);
        metrics.acceptStagingTriggered(fallbacks::get);
    }

    @Override
    public void markInProgress(Iterable<Long> keys) {
        // no op
    }

    @Override
    public void updateMultiple(Map<Long, TransactionStatus> keyValues) throws KeyAlreadyExistsException {
        Map<Cell, Long> cellToStartTs = keyValues.keySet().stream()
                .collect(Collectors.toMap(encodingStrategy::encodeStartTimestampAsCell, x -> x));
        Map<Cell, byte[]> stagingValues = KeyedStream.stream(cellToStartTs)
                .map(startTs -> encodingStrategy.encodeCommitStatusAsValue(
                        startTs, AtomicValue.staging(keyValues.get(startTs))))
                .collectToMap();
        store.atomicUpdate(stagingValues);
        store.put(KeyedStream.stream(stagingValues)
                .map(TwoPhaseEncodingStrategy.INSTANCE::transformStagingToCommitted)
                .collectToMap());
    }

    @Override
    public ListenableFuture<Map<Long, TransactionStatus>> get(Iterable<Long> cells) {
        Map<Long, Cell> startTsToCell = StreamSupport.stream(cells.spliterator(), false)
                .collect(Collectors.toMap(x -> x, encodingStrategy::encodeStartTimestampAsCell));
        ListenableFuture<Map<Cell, byte[]>> asyncReads = store.getMultiple(startTsToCell.values());
        return Futures.transform(
                asyncReads,
                presentValues -> processReads(presentValues, startTsToCell),
                MoreExecutors.directExecutor());
    }

    private FollowUpAction touchAndReturn(CellInfo cellAndValue) {
        if (!shouldTouch()) {
            return FollowUpAction.PUT;
        }
        Cell cell = cellAndValue.cell();
        byte[] actual = cellAndValue.value();
        try {
            store.checkAndTouch(cell, actual);
            return FollowUpAction.PUT;
        } catch (CheckAndSetException e) {
            long startTs = cellAndValue.startTs();
            AtomicValue<TransactionStatus> currentValue = encodingStrategy.decodeValueAsCommitStatus(startTs, actual);
            TransactionStatus commitStatus = currentValue.value();
            AtomicValue<TransactionStatus> kvsValue =
                    encodingStrategy.decodeValueAsCommitStatus(startTs, Iterables.getOnlyElement(e.getActualValues()));
            Preconditions.checkState(
                    kvsValue.isCommitted()
                            && TransactionStatuses.getCommitTimestamp(kvsValue.value())
                                    .equals(TransactionStatuses.getCommitTimestamp(commitStatus)),
                    "Failed to persist a staging value for commit timestamp because an unexpected value "
                            + "was found in the KVS",
                    SafeArg.of("kvsValue", kvsValue),
                    SafeArg.of("stagingValue", currentValue));
            return FollowUpAction.NONE;
        }
    }

    private Map<Long, TransactionStatus> processReads(Map<Cell, byte[]> reads, Map<Long, Cell> startTsToCell) {
        ImmutableMap.Builder<Long, TransactionStatus> resultBuilder = ImmutableMap.builder();
        for (Map.Entry<Long, Cell> startTsAndCell : startTsToCell.entrySet()) {
            Cell cell = startTsAndCell.getValue();
            Optional<byte[]> maybeActual = Optional.ofNullable(reads.get(cell));
            Long startTs = startTsAndCell.getKey();

            if (maybeActual.isEmpty()) {
                // put in_progress if there is no value in the kvs
                resultBuilder.put(
                        startTs,
                        encodingStrategy
                                .decodeValueAsCommitStatus(startTs, null)
                                .value());
                continue;
            }

            byte[] actual = maybeActual.get();
            AtomicValue<TransactionStatus> currentValue = encodingStrategy.decodeValueAsCommitStatus(startTs, actual);

            TransactionStatus commitStatus = currentValue.value();
            if (currentValue.isCommitted()) {
                // if there is a committed value, it will be committed or aborted
                // put and move on
                resultBuilder.put(startTs, commitStatus);
                continue;
            }
            try {
                Instant startTime = clock.instant();
                long commitTs = TransactionStatusUtils.getCommitTimestampOrThrow(commitStatus);
                resultBuilder.put(startTs, touchCache.get(ImmutableCellInfo.of(cell, startTs, commitTs, actual)));
                Duration timeTaken = Duration.between(startTime, clock.instant());
                if (timeTaken.compareTo(COMMIT_THRESHOLD) >= 0) {
                    acceptStagingUntil = clock.instant().plusSeconds(60);
                    log.log(logger -> logger.warn(
                            "Committing a staging value for the transactions table took too long. "
                                    + "Treating staging values as committed for 60 seconds to ensure liveness.",
                            SafeArg.of("startTs", startTs),
                            SafeArg.of("commitStatus", commitStatus),
                            SafeArg.of("timeTaken", timeTaken)));
                    fallbacks.incrementAndGet();
                }
                /**
                 * This in particular catches {@link com.palantir.atlasdb.keyvalue.api.RetryLimitReachedException}
                 */
            } catch (AtlasDbDependencyException e) {
                acceptStagingUntil = clock.instant().plusSeconds(60);
                log.log(logger -> logger.warn(
                        "Encountered exception attempting to commit a staging value for the transactions table. "
                                + "Treating staging values as committed for 60 seconds to ensure liveness.",
                        SafeArg.of("startTs", startTs),
                        SafeArg.of("commitStatus", commitStatus),
                        e));
                fallbacks.incrementAndGet();
                throw e;
            }
        }
        return resultBuilder.build();
    }

    private boolean shouldTouch() {
        return clock.instant().isAfter(acceptStagingUntil) && !acceptStagingReadsAsCommitted.get();
    }

    @Value.Immutable
    interface CellInfo {
        @Value.Parameter
        Cell cell();

        @Value.Parameter
        long startTs();

        @Value.Parameter
        long commitTs();

        @Value.Parameter
        byte[] value();
    }

    private enum FollowUpAction {
        PUT,
        NONE;
    }
}
