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

package com.palantir.atlasdb.transaction.impl.metrics;

import static org.assertj.core.api.Assertions.assertThat;

import com.codahale.metrics.Gauge;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.Test;

public class ZeroBasedDeltaGaugeTest {
    private static final long FIVE = 5L;

    private final AtomicLong value = new AtomicLong(0);
    private final Gauge<Long> longGauge = new ZeroBasedDeltaGauge(value::get);

    @Test
    public void initialValueIsReported() {
        value.set(FIVE);
        assertThat(longGauge.getValue()).isEqualTo(FIVE);
    }

    @Test
    public void reportsDifferencesBetweenValues() {
        value.set(FIVE);
        longGauge.getValue();
        value.addAndGet(FIVE);
        assertThat(value.get()).isEqualTo(FIVE + FIVE);
        assertThat(longGauge.getValue()).isEqualTo(FIVE);
    }

    @Test
    public void reportsNegativeDifferenceValues() {
        value.set(FIVE);
        assertThat(longGauge.getValue()).isEqualTo(FIVE);
        value.set(2L);
        assertThat(longGauge.getValue()).isEqualTo(2L - FIVE);
    }
}
