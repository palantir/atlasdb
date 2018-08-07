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

import static com.palantir.atlasdb.sweep.metrics.SweepMetricsAssert.assertThat;

import java.util.Arrays;
import java.util.stream.IntStream;

import org.junit.Before;
import org.junit.Test;

import com.palantir.atlasdb.keyvalue.impl.InMemoryKeyValueService;
import com.palantir.atlasdb.util.MetricsManager;
import com.palantir.atlasdb.util.MetricsManagers;

public class SweepOutcomeMetricsTest {
    private MetricsManager metricsManager;

    private SweepOutcomeMetrics legacyMetrics;
    private TargetedSweepMetrics targetedSweepMetrics;

    @Before
    public void setup() {
        metricsManager = MetricsManagers.createForTests();
        legacyMetrics = SweepOutcomeMetrics.registerLegacy(metricsManager);
        targetedSweepMetrics = TargetedSweepMetrics
                .create(metricsManager, new InMemoryKeyValueService(true), Long.MAX_VALUE);
    }

    @Test
    public void testAllOutcomes() {
        SweepOutcomeMetrics.LEGACY_OUTCOMES.forEach(outcome -> {
            assertThat(metricsManager).hasLegacyOutcomeEqualTo(outcome, 0L);
            legacyMetrics.registerOccurrenceOf(outcome);
            assertThat(metricsManager).hasLegacyOutcomeEqualTo(outcome, 1L);
        });
        SweepOutcomeMetrics.TARGETED_OUTCOMES.forEach(outcome -> {
            assertThat(metricsManager).hasTargetedOutcomeEqualTo(outcome, 0L);
            targetedSweepMetrics.registerOccurrenceOf(outcome);
            assertThat(metricsManager).hasTargetedOutcomeEqualTo(outcome, 1L);
        });
    }

    @Test
    public void testShutdownAndFatalAreBinary() {
        SweepOutcomeMetrics.LEGACY_OUTCOMES.forEach(outcome ->
                IntStream.range(0, 10).forEach(ignore -> legacyMetrics.registerOccurrenceOf(outcome)));
        assertThat(metricsManager).hasLegacyOutcomeEqualTo(SweepOutcome.SHUTDOWN, 1L);
        assertThat(metricsManager).hasLegacyOutcomeEqualTo(SweepOutcome.FATAL, 1L);
    }

    @Test
    public void testOtherMetricsAreCumulative() {
        SweepOutcomeMetrics.LEGACY_OUTCOMES.forEach(outcome ->
                IntStream.range(0, 10).forEach(ignore -> legacyMetrics.registerOccurrenceOf(outcome)));
        SweepOutcomeMetrics.LEGACY_OUTCOMES.stream()
                .filter(outcome -> outcome != SweepOutcome.SHUTDOWN && outcome != SweepOutcome.FATAL)
                .forEach(outcome -> assertThat(metricsManager).hasLegacyOutcomeEqualTo(outcome, 10L));
    }

    @Test
    public void testTargetedSweepMetricsAreCumulative() {
        SweepOutcomeMetrics.TARGETED_OUTCOMES.forEach(outcome ->
                IntStream.range(0, 10).forEach(ignore -> legacyMetrics.registerOccurrenceOf(outcome)));
        SweepOutcomeMetrics.TARGETED_OUTCOMES
                .forEach(outcome -> assertThat(metricsManager).hasLegacyOutcomeEqualTo(outcome, 10L));
    }

    @Test
    public void targetedSweepDoesNotRegisterExcludedOutcomes() {
        Arrays.stream(SweepOutcome.values())
                .filter(outcome -> !SweepOutcomeMetrics.TARGETED_OUTCOMES.contains(outcome))
                .forEach(outcome -> assertThat(metricsManager).hasNotRegisteredTargetedOutcome(outcome));
    }
}
