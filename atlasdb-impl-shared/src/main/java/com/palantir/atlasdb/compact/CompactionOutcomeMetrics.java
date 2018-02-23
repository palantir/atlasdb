/*
 * Copyright 2018 Palantir Technologies
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

package com.palantir.atlasdb.compact;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import com.codahale.metrics.SlidingTimeWindowReservoir;
import com.google.common.annotations.VisibleForTesting;
import com.palantir.atlasdb.util.MetricsManager;

class CompactionOutcomeMetrics {
    private final MetricsManager metricsManager = new MetricsManager();
    private final SlidingTimeWindowReservoir reservoir;

    private boolean shutdown;

    CompactionOutcomeMetrics() {
        Arrays.stream(BackgroundCompactor.CompactionOutcome.values()).forEach(outcome ->
                metricsManager.registerMetric(BackgroundCompactor.class, outcome.name(),
                        () -> getOutcomeCount(outcome)));
        reservoir = new SlidingTimeWindowReservoir(60L, TimeUnit.SECONDS);
    }

    @VisibleForTesting
    Long getOutcomeCount(BackgroundCompactor.CompactionOutcome outcome) {
        if (outcome == BackgroundCompactor.CompactionOutcome.SHUTDOWN) {
            return shutdown ? 1L : 0L;
        }

        return Arrays.stream(reservoir.getSnapshot().getValues())
                .filter(l -> l == outcome.ordinal())
                .count();
    }

    void registerOccurrenceOf(BackgroundCompactor.CompactionOutcome outcome) {
        if (outcome == BackgroundCompactor.CompactionOutcome.SHUTDOWN) {
            shutdown = true;
            return;
        }

        reservoir.update(outcome.ordinal());
    }
}
