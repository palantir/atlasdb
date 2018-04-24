/*
 * (c) Copyright 2018 Palantir Technologies Inc. All rights reserved.
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

import java.util.concurrent.TimeUnit;

import org.junit.Test;

import com.palantir.atlasdb.AtlasDbMetricNames;
import com.palantir.atlasdb.transaction.impl.InstrumentedTimelockService;
import com.palantir.atlasdb.util.AtlasDbMetrics;
import com.palantir.lock.v2.TimelockService;

public class MetricsBasedTimelockHealthCheckTest {
    private static final long METRICS_TICK_INTERVAL = TimeUnit.SECONDS.toMillis(5);

    private static final TimelockHealthCheck timelockHealthCheck = new MetricsBasedTimelockHealthCheck();
    private static TimelockService timelockService = mock(TimelockService.class);

    @Test
    public void timelockIsHealthyAfterSuccessfulRequests() {
        TimelockService instrumentedTimelockService = getFreshInstrumentedTimelockService();
        when(timelockService.getFreshTimestamp()).thenReturn(0L);

        tryGetFreshTimestamp(instrumentedTimelockService, 10);
        waitForClockTick();

        assertThat(timelockHealthCheck.getStatus().isHealthy()).isTrue();
    }

    @Test
    public void timelockIsUnhealthyAfterFailedRequests() {
        TimelockService instrumentedTimelockService = getFreshInstrumentedTimelockService();
        when(timelockService.getFreshTimestamp()).thenThrow(new RuntimeException());

        tryGetFreshTimestamp(instrumentedTimelockService, 10);
        waitForClockTick();

        assertThat(timelockHealthCheck.getStatus().isHealthy()).isFalse();
    }

    @Test
    public void timlockIsUnhealthyAfterOneSuccessMultipleFailures() {
        TimelockService instrumentedTimelockService = getFreshInstrumentedTimelockService();
        when(timelockService.getFreshTimestamp()).thenReturn(0L).thenThrow(new RuntimeException());

        tryGetFreshTimestamp(instrumentedTimelockService, 10);
        waitForClockTick();

        assertThat(timelockHealthCheck.getStatus().isHealthy()).isFalse();
    }

    @Test
    public void timlockIsHealthyAfterOneFailureMultipleSuccesses() {
        TimelockService instrumentedTimelockService = getFreshInstrumentedTimelockService();
        when(timelockService.getFreshTimestamp()).thenThrow(new RuntimeException()).thenReturn(0L);

        tryGetFreshTimestamp(instrumentedTimelockService, 10);
        waitForClockTick();

        assertThat(timelockHealthCheck.getStatus().isHealthy()).isTrue();
    }

    private static TimelockService getFreshInstrumentedTimelockService() {
        //Remove previously set metrics
        AtlasDbMetrics.getMetricRegistry().remove(AtlasDbMetricNames.TIMELOCK_FAILED_REQUEST);
        AtlasDbMetrics.getMetricRegistry().remove(AtlasDbMetricNames.TIMELOCK_SUCCESSFUL_REQUEST);

        timelockService = mock(TimelockService.class);
        TimelockService instrumentedTimelockService = new InstrumentedTimelockService(
                timelockService,
                AtlasDbMetrics.getMetricRegistry()
        );

        return instrumentedTimelockService;
    }

    private static void tryGetFreshTimestamp(TimelockService timelockService, int repetition) {
        for (int i=0; i<repetition; i++) {
            try {
                timelockService.getFreshTimestamp();
            } catch (RuntimeException e) {
            }
        }
    }

    private static void waitForClockTick() {
        try {
            Thread.sleep(METRICS_TICK_INTERVAL);
        } catch (InterruptedException e) {}
    }
}
