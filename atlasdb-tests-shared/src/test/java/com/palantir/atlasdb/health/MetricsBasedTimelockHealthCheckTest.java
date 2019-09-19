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
package com.palantir.atlasdb.health;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.codahale.metrics.MetricRegistry;
import com.palantir.atlasdb.transaction.impl.InstrumentedTimelockService;
import com.palantir.lock.v2.TimelockService;
import java.util.concurrent.TimeUnit;
import org.junit.Test;

public class MetricsBasedTimelockHealthCheckTest {
    private static final long METRICS_TICK_INTERVAL = TimeUnit.SECONDS.toMillis(5);

    private final MetricRegistry metricRegistry = new MetricRegistry();
    private final TimelockHealthCheck timelockHealthCheck = new MetricsBasedTimelockHealthCheck(metricRegistry);
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
    public void timelockIsUnhealthyAfterOneSuccessMultipleFailures() {
        TimelockService instrumentedTimelockService = getFreshInstrumentedTimelockService();
        when(timelockService.getFreshTimestamp()).thenReturn(0L).thenThrow(new RuntimeException());

        tryGetFreshTimestamp(instrumentedTimelockService, 10);
        waitForClockTick();

        assertThat(timelockHealthCheck.getStatus().isHealthy()).isFalse();
    }

    @Test
    public void timelockIsHealthyAfterOneFailureMultipleSuccesses() {
        TimelockService instrumentedTimelockService = getFreshInstrumentedTimelockService();
        when(timelockService.getFreshTimestamp()).thenThrow(new RuntimeException()).thenReturn(0L);

        tryGetFreshTimestamp(instrumentedTimelockService, 10);
        waitForClockTick();

        assertThat(timelockHealthCheck.getStatus().isHealthy()).isTrue();
    }

    private TimelockService getFreshInstrumentedTimelockService() {
        timelockService = mock(TimelockService.class);
        TimelockService instrumentedTimelockService = new InstrumentedTimelockService(
                timelockService,
                metricRegistry);

        return instrumentedTimelockService;
    }

    private static void tryGetFreshTimestamp(TimelockService service, int repetition) {
        for (int i = 0; i < repetition; i++) {
            try {
                service.getFreshTimestamp();
            } catch (RuntimeException e) {
                // ignored - TODO(gsheasby): should assert exception message
            }
        }
    }

    private static void waitForClockTick() {
        try {
            Thread.sleep(METRICS_TICK_INTERVAL);
        } catch (InterruptedException e) {
            // ignored - TODO(gsheasby): should assert exception message
        }
    }
}
