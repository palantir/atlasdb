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
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import com.codahale.metrics.Gauge;
import com.google.common.base.Suppliers;
import com.google.common.collect.Sets;
import com.palantir.atlasdb.metrics.MetricPublicationFilter;

/**
 * Given a provided number of metrics, ensures that only the top {@code maxPermittedRank} are published in the steady
 * state.
 */
public final class TopNMetricPublicationController<T extends Comparable> {
    static final Duration REFRESH_INTERVAL = Duration.ofSeconds(30);

    private final Set<Gauge<T>> gauges;
    private final Comparator<T> comparator;
    private final int maxPermittedRank;
    private final Supplier<Optional<T>> orderStatisticSupplier;

    private TopNMetricPublicationController(Comparator<T> comparator, int maxPermittedRank) {
        this.comparator = comparator;
        this.maxPermittedRank = maxPermittedRank;
        this.gauges = Sets.newConcurrentHashSet();
        this.orderStatisticSupplier = Suppliers.memoizeWithExpiration(
                this::calculateOrderStatistic,
                REFRESH_INTERVAL.toNanos(),
                TimeUnit.NANOSECONDS);
    }

    private Optional<T> calculateOrderStatistic() {
        return gauges.stream()
                .map(Gauge::getValue)
                .filter(Objects::nonNull)
                .sorted(comparator.reversed())
                .skip(maxPermittedRank - 1)
                .findFirst();
    }

    MetricPublicationFilter registerAndCreateFilter(Gauge<T> gauge) {
        gauges.add(gauge);
        return () -> shouldPublishIndividualGaugeMetric(gauge);
    }

    private boolean shouldPublishIndividualGaugeMetric(Gauge<T> constituentGauge) {
        T value = constituentGauge.getValue();
        return orderStatisticSupplier.get()
                .map(orderStatistic -> isAtLeastAsLargeAsOrderStatistic(value, orderStatistic))
                .orElse(false);
    }

    private boolean isAtLeastAsLargeAsOrderStatistic(T value, T orderStatistic) {
        return comparator.compare(value, orderStatistic) >= 0;
    }
}
