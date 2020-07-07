/*
 * (c) Copyright 2020 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.util;

import java.time.Duration;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.codahale.metrics.Gauge;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Suppliers;
import com.google.common.collect.Sets;
import com.palantir.atlasdb.metrics.MetricPublicationFilter;
import com.palantir.logsafe.Preconditions;
import com.palantir.logsafe.SafeArg;

/**
 * Given a provided number of metrics, ensures that only the top {@code maxPermittedRank} are published in the steady
 * state.
 */
public final class TopNMetricPublicationController<T> {
    private static final Duration REFRESH_INTERVAL = Duration.ofSeconds(30);

    private final Set<Gauge<T>> gauges;
    private final Comparator<T> comparator;
    private final int maxPermittedRank;
    private final Supplier<Optional<T>> lowerBoundSupplier;

    private TopNMetricPublicationController(Comparator<T> comparator, int maxPermittedRank) {
        this.comparator = comparator;
        this.maxPermittedRank = maxPermittedRank;
        this.gauges = Sets.newConcurrentHashSet();
        this.lowerBoundSupplier = Suppliers.memoizeWithExpiration(
                this::calculateLowerBound,
                REFRESH_INTERVAL.toNanos(),
                TimeUnit.NANOSECONDS);
    }

    @SuppressWarnings("unchecked") // Guaranteed correct
    public static <T extends Comparable> TopNMetricPublicationController<T> create(int maxPermittedRank) {
        Preconditions.checkState(maxPermittedRank > 0, "maxPermittedRank must be positive",
                SafeArg.of("maxPermittedRank", maxPermittedRank));
        return new TopNMetricPublicationController<T>(Comparator.naturalOrder(), maxPermittedRank);
    }

    public MetricPublicationFilter registerAndCreateFilter(Gauge<T> gauge) {
        gauges.add(gauge);
        return () -> shouldPublishIndividualGaugeMetric(gauge);
    }

    private boolean shouldPublishIndividualGaugeMetric(Gauge<T> constituentGauge) {
        T value = constituentGauge.getValue();
        return lowerBoundSupplier.get()
                .map(lowerBound -> isAtLeastLowerBound(value, lowerBound))
                .orElse(true); // This means there aren't maxPermittedRank series, so we should publish the metric.
    }

    private boolean isAtLeastLowerBound(T value, T lowerBound) {
        return comparator.compare(value, lowerBound) >= 0;
    }

    private Optional<T> calculateLowerBound() {
        return calculateOrderStatistic(gauges.stream().map(Gauge::getValue), comparator, maxPermittedRank);
    }

    @VisibleForTesting
    static <T> Optional<T> calculateOrderStatistic(
            Stream<T> values, Comparator<T> comparator, int orderStatistic) {
        Preconditions.checkState(orderStatistic > 0,
                "The order statistic to be queried for must be positive",
                SafeArg.of("queriedOrderStatistic", orderStatistic));

        // O(N log N) sorting algorithm. The number of gauges is not expected to be large.
        // If performance is bad: consider switching to a heap O(n log orderStatistic) or quick-select algorithms.
        return values.filter(Objects::nonNull)
                .sorted(comparator.reversed())
                .skip(orderStatistic - 1)
                .findFirst();
    }
}
