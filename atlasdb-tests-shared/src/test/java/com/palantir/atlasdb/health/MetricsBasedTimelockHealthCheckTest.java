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

package com.palantir.atlasdb.health;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.palantir.atlasdb.transaction.impl.InstrumentedTimelockService;
import com.palantir.atlasdb.util.AtlasDbMetrics;
import com.palantir.lock.v2.TimelockService;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class MetricsBasedTimelockHealthCheckTest {
    private static final TimelockService timelockService = mock(TimelockService.class);
    private static final TimelockHealthCheck timelockHealthCheck = new MetricsBasedTimelockHealthCheck();

    @Rule
    public final ExpectedException exception = ExpectedException.none();

    @Test
    public void timelockIsHealthyAfterSuccessfulRequests() {
        TimelockService instrumentedTimelockService = new InstrumentedTimelockService(
                timelockService,
                AtlasDbMetrics.getMetricRegistry()
        );
        instrumentedTimelockService.getFreshTimestamp();
        assertThat(timelockHealthCheck.getStatus().isHealthy()).isTrue();
    }

    @Test
    public void timelockIsUnhealthyAfterFailedRequests() {
        when(timelockService.getFreshTimestamp()).thenThrow(new RuntimeException());
        TimelockService instrumentedTimelockService = new InstrumentedTimelockService(
                timelockService,
                AtlasDbMetrics.getMetricRegistry()
        );

        exception.expect(RuntimeException.class);
        instrumentedTimelockService.getFreshTimestamp();

        //wait for metrics clock to tick
        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {}

        assertThat(timelockHealthCheck.getStatus().isHealthy()).isFalse();
    }
}
