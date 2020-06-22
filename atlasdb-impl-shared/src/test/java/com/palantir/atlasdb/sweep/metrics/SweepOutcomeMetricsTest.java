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
package com.palantir.atlasdb.sweep.metrics;

import static com.palantir.atlasdb.sweep.metrics.SweepMetricsAssert.assertThat;
import static com.palantir.atlasdb.table.description.SweepStrategy.SweeperStrategy.CONSERVATIVE;
import static com.palantir.atlasdb.table.description.SweepStrategy.SweeperStrategy.THOROUGH;
import static org.mockito.Mockito.mock;

import com.palantir.atlasdb.keyvalue.impl.InMemoryKeyValueService;
import com.palantir.atlasdb.sweep.queue.ShardAndStrategy;
import com.palantir.atlasdb.util.MetricsManager;
import com.palantir.atlasdb.util.MetricsManagers;
import com.palantir.lock.v2.TimelockService;
import java.util.Arrays;
import java.util.stream.IntStream;
import org.junit.Before;
import org.junit.Test;

public class SweepOutcomeMetricsTest {
    private MetricsManager metricsManager;

    private SweepOutcomeMetrics legacyMetrics;
    private TargetedSweepMetrics targetedSweepMetrics;

    @Before
    public void setup() {
        metricsManager = MetricsManagers.createForTests();
        legacyMetrics = SweepOutcomeMetrics.registerLegacy(metricsManager);
        targetedSweepMetrics = TargetedSweepMetrics
                .create(metricsManager, mock(TimelockService.class), new InMemoryKeyValueService(true),
                        TargetedSweepMetrics.MetricsConfiguration.builder()
                                .millisBetweenRecomputingMetrics(Long.MAX_VALUE)
                                .build());
    }

    @Test
    public void testMetricsAreNotRegisteredEagerly() {
        SweepOutcomeMetrics.LEGACY_OUTCOMES.forEach(outcome -> {
            assertThat(metricsManager).hasNotRegisteredLegacyOutcome(outcome);
        });
        SweepOutcomeMetrics.TARGETED_OUTCOMES.forEach(outcome -> {
            assertThat(metricsManager).hasNotRegisteredTargetedOutcome(CONSERVATIVE, outcome);
            assertThat(metricsManager).hasNotRegisteredTargetedOutcome(THOROUGH, outcome);
        });
    }

    @Test
    public void testAllOutcomes() {
        SweepOutcomeMetrics.LEGACY_OUTCOMES.forEach(outcome -> {
            legacyMetrics.registerOccurrenceOf(outcome);
            assertThat(metricsManager).hasLegacyOutcomeEqualTo(outcome, 1L);
        });
        SweepOutcomeMetrics.TARGETED_OUTCOMES.forEach(outcome -> {
            targetedSweepMetrics.registerOccurrenceOf(ShardAndStrategy.conservative(3), outcome);
            assertThat(metricsManager).hasTargetedOutcomeEqualTo(CONSERVATIVE, outcome, 1L);

            targetedSweepMetrics.registerOccurrenceOf(ShardAndStrategy.thorough(1), outcome);
            assertThat(metricsManager).hasTargetedOutcomeEqualTo(CONSERVATIVE, outcome, 1L);
            assertThat(metricsManager).hasTargetedOutcomeEqualTo(THOROUGH, outcome, 1L);
        });
    }

    @Test
    public void testFatalIsBinary() {
        SweepOutcomeMetrics.LEGACY_OUTCOMES.forEach(outcome ->
                IntStream.range(0, 10).forEach(ignore -> legacyMetrics.registerOccurrenceOf(outcome)));
        assertThat(metricsManager).hasLegacyOutcomeEqualTo(SweepOutcome.FATAL, 1L);
    }

    @Test
    public void testOtherMetricsAreCumulative() {
        SweepOutcomeMetrics.LEGACY_OUTCOMES.forEach(outcome ->
                IntStream.range(0, 10).forEach(ignore -> legacyMetrics.registerOccurrenceOf(outcome)));
        SweepOutcomeMetrics.LEGACY_OUTCOMES.stream()
                .filter(outcome -> outcome != SweepOutcome.FATAL)
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
        targetedSweepMetrics.registerOccurrenceOf(CONSERVATIVE, SweepOutcome.SUCCESS);
        targetedSweepMetrics.registerOccurrenceOf(THOROUGH, SweepOutcome.SUCCESS);
        Arrays.stream(SweepOutcome.values())
                .filter(outcome -> !SweepOutcomeMetrics.TARGETED_OUTCOMES.contains(outcome))
                .forEach(outcome -> assertThat(metricsManager).hasNotRegisteredTargetedOutcome(CONSERVATIVE, outcome));
        Arrays.stream(SweepOutcome.values())
                .filter(outcome -> !SweepOutcomeMetrics.TARGETED_OUTCOMES.contains(outcome))
                .forEach(outcome -> assertThat(metricsManager).hasNotRegisteredTargetedOutcome(THOROUGH, outcome));
    }
}
