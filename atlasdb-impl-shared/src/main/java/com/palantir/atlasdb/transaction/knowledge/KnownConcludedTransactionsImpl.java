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

package com.palantir.atlasdb.transaction.knowledge;

import com.google.common.collect.*;
import com.palantir.common.concurrent.CoalescingSupplier;
import com.palantir.logsafe.Preconditions;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import com.palantir.tritium.metrics.registry.TaggedMetricRegistry;
import org.immutables.value.Value;

import java.util.Comparator;
import java.util.concurrent.atomic.AtomicReference;

@SuppressWarnings("UnstableApiUsage") // RangeSet usage
public final class KnownConcludedTransactionsImpl implements KnownConcludedTransactions {
    private static final SafeLogger log = SafeLoggerFactory.get(KnownConcludedTransactionsImpl.class);
    private static final int MAX_ATTEMPTS = 20;

    private final KnownConcludedTransactionsStore knownConcludedTransactionsStore;

    /**
     * Concurrency: All updates go through {@link #ensureRangesCached(RangeSet)}} and perform CASes to atomically
     * evolve the value here. Copy on write should be acceptable given these range-sets are not expected to be large.
     */
    private final AtomicReference<Cache> cachedConcludedTimestampsRef;

    private final KnownConcludedTransactionsMetrics knownConcludedTransactionsMetrics;

    /**
     * This ensures that only one outstanding read to the database per object is running at any given time.
     */
    private final CoalescingSupplier<Void> cacheUpdater = new CoalescingSupplier<>(() -> {
        updateCacheFromRemote();
        return null;
    });

    private KnownConcludedTransactionsImpl(
            KnownConcludedTransactionsStore knownConcludedTransactionsStore,
            KnownConcludedTransactionsMetrics metrics) {
        this.knownConcludedTransactionsStore = knownConcludedTransactionsStore;
        this.cachedConcludedTimestampsRef = new AtomicReference<>(ImmutableCache.of(ImmutableRangeSet.of()));
        this.knownConcludedTransactionsMetrics = metrics;
        metrics.disjointCacheIntervals(
                () -> cachedConcludedTimestampsRef.get().ranges().size());
    }

    public static KnownConcludedTransactions create(
            KnownConcludedTransactionsStore knownConcludedTransactionsStore,
            TaggedMetricRegistry taggedMetricRegistry) {
        KnownConcludedTransactionsImpl store = new KnownConcludedTransactionsImpl(
                knownConcludedTransactionsStore, KnownConcludedTransactionsMetrics.of(taggedMetricRegistry));
        return store;
    }

    @Override
    public boolean isKnownConcluded(long startTimestamp, Consistency consistency) {
        if (cachedConcludedTimestampsRef.get().rangeSet().contains(startTimestamp)) {
            knownConcludedTransactionsMetrics.localReads().mark();
            return true;
        }
        if (consistency == Consistency.REMOTE_READ) {
            knownConcludedTransactionsMetrics.remoteReads().mark();
            return performRemoteReadAndCheckConcluded(startTimestamp);
        }
        return false;
    }

    @Override
    public void addConcludedTimestamps(Range<Long> knownConcludedInterval) {
        Preconditions.checkState(
                sanityCheckConcludedRange(knownConcludedInterval),
                "KnownConcludedInterval is expected to have closed lower and upper bounds.",
                SafeArg.of("knownConcludedInterval", knownConcludedInterval));
        knownConcludedTransactionsStore.supplement(knownConcludedInterval);
        ensureRangesCached(ImmutableRangeSet.of(knownConcludedInterval));
    }

    private boolean sanityCheckConcludedRange(Range<Long> range) {
        return range.hasLowerBound()
                && range.hasUpperBound()
                && range.lowerBoundType().equals(BoundType.CLOSED)
                && range.upperBoundType().equals(BoundType.CLOSED);
    }

    @Override
    public long lastLocallyKnownConcludedTimestamp() {
        return cachedConcludedTimestampsRef.get().lastKnownConcludedTs();
    }

    private boolean performRemoteReadAndCheckConcluded(long startTimestamp) {
        cacheUpdater.get();
        return cachedConcludedTimestampsRef.get().rangeSet().contains(startTimestamp);
    }

    private void updateCacheFromRemote() {
        ensureRangesCached(knownConcludedTransactionsStore
                .get()
                .map(TimestampRangeSet::timestampRanges)
                .orElseGet(ImmutableRangeSet::of));
    }

    private void ensureRangesCached(RangeSet<Long> timestampRanges) {
        for (int attempt = 0; attempt < MAX_ATTEMPTS; attempt++) {

            Cache cachedConcludedTimestamps = cachedConcludedTimestampsRef.get();

            ImmutableRangeSet<Long> cachedRanges = cachedConcludedTimestamps.rangeSet();

            if (cachedRanges.enclosesAll(timestampRanges)) {
                return;
            }
            Cache targetCacheValue = ImmutableCache.of(ImmutableRangeSet.unionOf(
                    Sets.union(cachedConcludedTimestamps.ranges(), timestampRanges.asRanges())));
            if (cachedConcludedTimestampsRef.compareAndSet(cachedConcludedTimestamps, targetCacheValue)) {
                return;
            }
            // Concurrent update; can try again.
        }
        log.warn(
                "Unable to ensure ranges of known concluded transactions were cached.",
                SafeArg.of("numAttempts", MAX_ATTEMPTS));
        throw new SafeIllegalStateException("Unable to ensure ranges of known concluded transactions were cached.");
    }

    @Value.Immutable
    interface Cache {
        @Value.Parameter
        ImmutableRangeSet<Long> rangeSet();

        @Value.Lazy
        default ImmutableSet<Range<Long>> ranges() {
            return rangeSet().asRanges();
        }

        @Value.Lazy
        default long lastKnownConcludedTs() {
            return ranges().stream()
                    .filter(Range::hasUpperBound)
                    .map(Range::upperEndpoint)
                    .max(Comparator.naturalOrder())
                    .orElse(0L);
        }
    }
}
