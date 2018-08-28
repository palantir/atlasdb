/*
 * Copyright 2018 Palantir Technologies, Inc. All rights reserved.
 *
 * Licensed under the BSD-3 License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://opensource.org/licenses/BSD-3-Clause
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.palantir.atlasdb.sweep.metrics;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.codahale.metrics.SlidingTimeWindowReservoir;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.palantir.atlasdb.AtlasDbMetricNames;
import com.palantir.atlasdb.sweep.BackgroundSweeperImpl;
import com.palantir.atlasdb.util.MetricsManager;

public final class SweepOutcomeMetrics {
    public static final List<SweepOutcome> LEGACY_OUTCOMES = Arrays.asList(SweepOutcome.values());
    public static final List<SweepOutcome> TARGETED_OUTCOMES = ImmutableList.of(SweepOutcome.NOT_ENOUGH_DB_NODES_ONLINE,
            SweepOutcome.DISABLED, SweepOutcome.SUCCESS, SweepOutcome.ERROR, SweepOutcome.NOTHING_TO_SWEEP);

    private final SlidingTimeWindowReservoir reservoir;
    private volatile boolean shutdown = false;
    private volatile boolean fatal = false;

    private SweepOutcomeMetrics() {
        reservoir = new SlidingTimeWindowReservoir(60L, TimeUnit.SECONDS);
    }

    public static SweepOutcomeMetrics registerLegacy(MetricsManager metricsManager) {
        SweepOutcomeMetrics metrics = new SweepOutcomeMetrics();
        metrics.registerMetric(metricsManager, LEGACY_OUTCOMES, BackgroundSweeperImpl.class);
        return metrics;
    }

    public static SweepOutcomeMetrics registerTargeted(MetricsManager metricsManager) {
        SweepOutcomeMetrics metrics = new SweepOutcomeMetrics();
        metrics.registerMetric(metricsManager, TARGETED_OUTCOMES, TargetedSweepMetrics.class);
        return metrics;
    }

    private void registerMetric(MetricsManager manager, List<SweepOutcome> outcomes, Class<?> forClass) {
        outcomes.forEach(outcome -> manager.registerOrGet(forClass, AtlasDbMetricNames.SWEEP_OUTCOME,
                () -> getOutcomeCount(outcome), ImmutableMap.of(AtlasDbMetricNames.TAG_OUTCOME, outcome.name())));
    }

    private Long getOutcomeCount(SweepOutcome outcome) {
        if (outcome == SweepOutcome.SHUTDOWN) {
            return shutdown ? 1L : 0L;
        }
        if (outcome == SweepOutcome.FATAL) {
            return fatal ? 1L : 0L;
        }

        return Arrays.stream(reservoir.getSnapshot().getValues())
                .filter(l -> l == outcome.ordinal())
                .count();
    }

    public void registerOccurrenceOf(SweepOutcome outcome) {
        if (outcome == SweepOutcome.SHUTDOWN) {
            shutdown = true;
            return;
        }
        if (outcome == SweepOutcome.FATAL) {
            fatal = true;
            return;
        }

        reservoir.update(outcome.ordinal());
    }
}
