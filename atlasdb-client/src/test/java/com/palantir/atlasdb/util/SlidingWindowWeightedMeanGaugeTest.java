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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.withinPercentage;

import java.time.Duration;
import org.junit.Test;

public class SlidingWindowWeightedMeanGaugeTest {
    private SlidingWindowWeightedMeanGauge gauge = SlidingWindowWeightedMeanGauge.create();

    @Test
    public void initialValueIsZero() {
        assertValueIsCloseTo(0.0);
    }

    @Test
    public void withEqualWeightsCalculateMeanOfValues() {
        gauge.update(1.2, 5L);
        gauge.update(1.5, 5L);
        gauge.update(1.8, 5L);
        assertValueIsCloseTo(1.5);
    }

    @Test
    public void calculateWeightedMean() {
        gauge.update(101.0, 1);
        gauge.update(1.0, 98);
        gauge.update(301.0, 1);
        assertValueIsCloseTo(5.0);
    }

    @Test
    public void testMultipleGets() {
        gauge.update(1.0, 1);
        assertValueIsCloseTo(1.0);
        assertValueIsCloseTo(1.0);
        gauge.update(0.5, 4);
        assertValueIsCloseTo(0.6);
    }

    @Test
    public void entriesWithWeightZeroAreIgnored() {
        gauge.update(999.0, 0);
        gauge.update(1.0, 1);
        gauge.update(123.0, 0);
        assertValueIsCloseTo(1.0);
    }

    @Test
    public void entriesWithNegativeWeightThrow() {
        gauge.update(5.0, 4);
        assertThatThrownBy(() -> gauge.update(2345.0, -1)).isInstanceOf(IllegalArgumentException.class);
        assertValueIsCloseTo(5.0);
    }

    @Test
    public void entriesExpire() {
        SlidingWindowWeightedMeanGauge expiringGauge = new SlidingWindowWeightedMeanGauge(Duration.ZERO);
        expiringGauge.update(100.0, 50);
        expiringGauge.update(50.0, 4);
        assertThat(expiringGauge.getValue()).isCloseTo(0.0, withinPercentage(0.1));
    }

    private void assertValueIsCloseTo(double expected) {
        assertThat(gauge.getValue()).isCloseTo(expected, withinPercentage(0.1));
    }
}
