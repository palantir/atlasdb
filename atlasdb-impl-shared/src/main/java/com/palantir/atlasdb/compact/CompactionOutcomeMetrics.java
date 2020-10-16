/*
 * (c) Copyright 2018 Palantir Technologies Inc. All rights reserved.
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
package com.palantir.atlasdb.compact;

import com.codahale.metrics.SlidingTimeWindowReservoir;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.palantir.atlasdb.util.MetricsManager;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

class CompactionOutcomeMetrics {
    private final SlidingTimeWindowReservoir reservoir;

    private boolean shutdown;

    CompactionOutcomeMetrics(MetricsManager metricsManager) {
        Arrays.stream(BackgroundCompactor.CompactionOutcome.values())
                .forEach(outcome -> metricsManager.registerOrGet(
                        BackgroundCompactor.class,
                        "outcome",
                        () -> getOutcomeCount(outcome),
                        ImmutableMap.of("status", outcome.name())));
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
