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
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.google.common.collect.ImmutableList;
import com.palantir.logsafe.Preconditions;
import com.palantir.logsafe.SafeArg;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import org.immutables.value.Value;

/**
 * A gauge that calculates the weighted mean of updates during a sliding window of time. Correctness is only guaranteed
 * as long as the sum of recorded weights and sum of weighted values do not exceed {@link Long#MAX_VALUE} and
 * {@link Double#MAX_VALUE}, respectively.
 */
public class SlidingWindowWeightedMeanGauge implements Gauge<Double> {
    private final Cache<Long, WeightedEntry> updates;
    private final AtomicLong fakeCounter = new AtomicLong();

    public SlidingWindowWeightedMeanGauge(Duration expirationDuration) {
        this.updates =
                Caffeine.newBuilder().expireAfterWrite(expirationDuration).build();
    }

    public static SlidingWindowWeightedMeanGauge create() {
        return new SlidingWindowWeightedMeanGauge(Duration.ofMinutes(5L));
    }

    @Override
    public Double getValue() {
        List<WeightedEntry> snapshot = ImmutableList.copyOf(updates.asMap().values());
        return summarize(snapshot);
    }

    public void update(double value, long weight) {
        Preconditions.checkArgument(weight >= 0, "Weight cannot be negative.", SafeArg.of("weight", weight));
        if (weight == 0) {
            return;
        }
        updates.put(fakeCounter.getAndIncrement(), ImmutableWeightedEntry.of(value, weight));
    }

    private double summarize(List<WeightedEntry> snapshot) {
        long totalWeight = snapshot.stream().mapToLong(WeightedEntry::weight).sum();
        if (totalWeight == 0) {
            return 0.0;
        }
        double valueSum = snapshot.stream()
                .mapToDouble(entry -> entry.value() * entry.weight())
                .sum();
        return valueSum / totalWeight;
    }

    @Value.Immutable
    interface WeightedEntry {
        @Value.Parameter
        double value();

        @Value.Parameter
        long weight();
    }
}
