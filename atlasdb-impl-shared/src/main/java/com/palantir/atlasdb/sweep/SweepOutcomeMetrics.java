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

package com.palantir.atlasdb.sweep;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import com.codahale.metrics.SlidingTimeWindowReservoir;
import com.google.common.collect.ImmutableMap;
import com.palantir.atlasdb.util.MetricsManager;

class SweepOutcomeMetrics {
    private final MetricsManager metricsManager = new MetricsManager();
    private final SlidingTimeWindowReservoir reservoir;

    private boolean shutdown;
    private boolean fatal;

    SweepOutcomeMetrics() {
        Arrays.stream(SweepOutcome.values()).forEach(outcome ->
                metricsManager.registerOrAddToMetric(BackgroundSweeperImpl.class, "outcome",
                        () -> getOutcomeCount(outcome), ImmutableMap.of("status", outcome.name())));
        reservoir = new SlidingTimeWindowReservoir(60L, TimeUnit.SECONDS);
        shutdown = false;
        fatal = false;
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

    void registerOccurrenceOf(SweepOutcome outcome) {
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
