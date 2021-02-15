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

import com.codahale.metrics.Gauge;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Suppliers;
import com.palantir.atlasdb.metrics.MetricPublicationFilter;
import com.palantir.common.streams.KeyedStream;
import com.palantir.logsafe.Preconditions;
import com.palantir.logsafe.SafeArg;
import java.time.Duration;
import java.util.Comparator;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * Given a provided number of metrics, ensures that only the top {@code maxPermittedRank} are published in the steady
 * state. If there are ties (e.g. the 9th, 10th, 11th and 12th highest value are equal in a controller configured to
 * permit the top 10 metrics), which of the tying metrics are selected is undefined, though it is guaranteed that
 * the correct number will be selected so that the total number of metrics published equals the max permitted rank.
 */
public final class TopNMetricPublicationController<T> {
    private static final Duration REFRESH_INTERVAL = Duration.ofSeconds(30);

    private final Set<Gauge<T>> gauges;
    private final Comparator<T> comparator;
    private final int maxPermittedRank;
    private final Supplier<Set<Gauge<T>>> publishableGauges;

    @VisibleForTesting
    TopNMetricPublicationController(Comparator<T> comparator, int maxPermittedRank, Duration refreshInterval) {
        this.comparator = comparator;
        this.maxPermittedRank = maxPermittedRank;
        this.gauges = ConcurrentHashMap.newKeySet();
        this.publishableGauges = Suppliers.memoizeWithExpiration(
                this::getEligibleGauges, refreshInterval.toNanos(), TimeUnit.NANOSECONDS);
    }

    @SuppressWarnings("unchecked") // Guaranteed correct
    public static <T extends Comparable> TopNMetricPublicationController<T> create(int maxPermittedRank) {
        Preconditions.checkState(
                maxPermittedRank > 0,
                "maxPermittedRank must be positive",
                SafeArg.of("maxPermittedRank", maxPermittedRank));
        return new TopNMetricPublicationController<T>(Comparator.naturalOrder(), maxPermittedRank, REFRESH_INTERVAL);
    }

    public MetricPublicationFilter registerAndCreateFilter(Gauge<T> gauge) {
        gauges.add(gauge);
        return () -> shouldPublishIndividualGaugeMetric(gauge);
    }

    private boolean shouldPublishIndividualGaugeMetric(Gauge<T> constituentGauge) {
        return publishableGauges.get().contains(constituentGauge);
    }

    private Set<Gauge<T>> getEligibleGauges() {
        return KeyedStream.of(gauges.stream())
                .map(Gauge::getValue)
                .filter(Objects::nonNull)
                .entries()
                .sorted(Comparator.comparing(Map.Entry::getValue, comparator.reversed()))
                .limit(maxPermittedRank)
                .map(Map.Entry::getKey)
                .collect(Collectors.toSet());
    }
}
