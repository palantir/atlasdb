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

package com.palantir.atlasdb.sweep.metrics;

import com.google.common.annotations.VisibleForTesting;
import com.palantir.atlasdb.metrics.MetricPublicationFilter;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.LongSupplier;
import org.immutables.value.Value;

/**
 * Indicates whether targeted sweep metrics should be published.
 *
 * The criteria for publishing metrics for a given strategy, is if the number of enqueued writes or reads is in
 * excess of {@link #MINIMUM_READS_WRITES_TO_BE_CONSIDERED_ACTIVE}, or it is believed that targeted sweep is behind by
 * more than {@link #MINIMUM_STALE_DURATION}. If any of these conditions are true, then all targeted sweep metrics
 * will be published.
 */
public class TargetedSweepMetricPublicationFilter implements MetricPublicationFilter {
    @VisibleForTesting
    static final long MINIMUM_READS_WRITES_TO_BE_CONSIDERED_ACTIVE = 1_000;

    @VisibleForTesting
    static final Duration MINIMUM_STALE_DURATION = Duration.ofHours(4);

    private final AtomicBoolean publicationLatch;

    private final LongSupplier enqueuedWrites;
    private final LongSupplier entriesRead;
    private final LongSupplier millisSinceLastSweptTs;

    public TargetedSweepMetricPublicationFilter(DecisionMetrics decisionMetrics) {
        this.publicationLatch = new AtomicBoolean(false);
        this.enqueuedWrites = decisionMetrics.enqueuedWrites();
        this.entriesRead = decisionMetrics.entriesRead();
        this.millisSinceLastSweptTs = decisionMetrics.millisSinceLastSweptTs();
    }

    @Override
    public boolean shouldPublish() {
        if (publicationLatch.get()) {
            return true;
        }
        boolean conditionsAchieved = testConditions();
        if (conditionsAchieved) {
            publicationLatch.set(true);
        }
        return conditionsAchieved;
    }

    private boolean testConditions() {
        return enqueuedWrites.getAsLong() >= MINIMUM_READS_WRITES_TO_BE_CONSIDERED_ACTIVE
                || entriesRead.getAsLong() >= MINIMUM_READS_WRITES_TO_BE_CONSIDERED_ACTIVE
                || millisSinceLastSweptTs.getAsLong() >= MINIMUM_STALE_DURATION.toMillis();
    }

    @Value.Immutable
    public interface DecisionMetrics {
        LongSupplier enqueuedWrites();

        LongSupplier entriesRead();

        LongSupplier millisSinceLastSweptTs();
    }
}
