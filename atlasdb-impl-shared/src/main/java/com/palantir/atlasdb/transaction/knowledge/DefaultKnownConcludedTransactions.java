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

import com.google.common.collect.ImmutableRangeSet;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.Sets;
import com.palantir.common.concurrent.CoalescingSupplier;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import com.palantir.tritium.metrics.registry.TaggedMetricRegistry;
import java.util.concurrent.atomic.AtomicReference;

@SuppressWarnings("UnstableApiUsage") // RangeSet usage
public final class DefaultKnownConcludedTransactions implements KnownConcludedTransactions {
    private static final SafeLogger log = SafeLoggerFactory.get(DefaultKnownConcludedTransactions.class);
    private static final int MAX_ATTEMPTS = 20;

    private final KnownConcludedTransactionsStore knownConcludedTransactionsStore;

    /**
     * Concurrency: All updates go through {@link #ensureRangesCached(RangeSet)}} and perform CASes to atomically
     * evolve the value here. Copy on write should be acceptable given these range-sets are not expected to be large.
     */
    private final AtomicReference<ImmutableRangeSet<Long>> cachedConcludedTimestamps;

    private final KnownConcludedTransactionsMetrics knownConcludedTransactionsMetrics;

    /**
     * This ensures that only one outstanding read to the database per object is running at any given time.
     */
    private final CoalescingSupplier<Void> cacheUpdater = new CoalescingSupplier<>(() -> {
        updateCacheFromRemote();
        return null;
    });

    private DefaultKnownConcludedTransactions(
            KnownConcludedTransactionsStore knownConcludedTransactionsStore,
            KnownConcludedTransactionsMetrics metrics) {
        this.knownConcludedTransactionsStore = knownConcludedTransactionsStore;
        this.cachedConcludedTimestamps = new AtomicReference<>(ImmutableRangeSet.of());
        this.knownConcludedTransactionsMetrics = metrics;
        metrics.disjointCacheIntervals(
                () -> cachedConcludedTimestamps.get().asRanges().size());
    }

    public static KnownConcludedTransactions create(
            KnownConcludedTransactionsStore knownConcludedTransactionsStore,
            TaggedMetricRegistry taggedMetricRegistry) {
        DefaultKnownConcludedTransactions store = new DefaultKnownConcludedTransactions(
                knownConcludedTransactionsStore, KnownConcludedTransactionsMetrics.of(taggedMetricRegistry));
        return store;
    }

    @Override
    public boolean isKnownConcluded(long startTimestamp, Consistency consistency) {
        if (cachedConcludedTimestamps.get().contains(startTimestamp)) {
            return true;
        }
        if (consistency == Consistency.REMOTE_READ) {
            return performRemoteReadAndCheckConcluded(startTimestamp);
        }
        return false;
    }

    @Override
    public void addConcludedTimestamps(Range<Long> knownConcludedInterval) {
        knownConcludedTransactionsStore.supplement(knownConcludedInterval);
        ensureRangesCached(ImmutableRangeSet.of(knownConcludedInterval));
    }

    private boolean performRemoteReadAndCheckConcluded(long startTimestamp) {
        cacheUpdater.get();
        return cachedConcludedTimestamps.get().contains(startTimestamp);
    }

    private void updateCacheFromRemote() {
        ensureRangesCached(knownConcludedTransactionsStore
                .get()
                .map(TimestampRangeSet::timestampRanges)
                .orElseGet(ImmutableRangeSet::of));
    }

    private void ensureRangesCached(RangeSet<Long> timestampRanges) {
        for (int attempt = 0; attempt < MAX_ATTEMPTS; attempt++) {
            ImmutableRangeSet<Long> cache = cachedConcludedTimestamps.get();
            if (cache.enclosesAll(timestampRanges)) {
                return;
            }
            ImmutableRangeSet<Long> targetCacheValue =
                    ImmutableRangeSet.unionOf(Sets.union(cache.asRanges(), timestampRanges.asRanges()));
            if (cachedConcludedTimestamps.compareAndSet(cache, targetCacheValue)) {
                return;
            }
            // Concurrent update; can try again.
        }
        log.warn(
                "Unable to ensure ranges of known concluded transactions were cached.",
                SafeArg.of("numAttempts", MAX_ATTEMPTS));
        throw new SafeIllegalStateException("Unable to ensure ranges of known concluded transactions were cached.");
    }
}
