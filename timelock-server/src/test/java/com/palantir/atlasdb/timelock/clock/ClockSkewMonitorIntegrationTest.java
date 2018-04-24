/*
 * (c) Copyright 2017 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.timelock.clock;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.jmock.lib.concurrent.DeterministicScheduler;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class ClockSkewMonitorIntegrationTest {
    private final String server = "test";
    private final ClockService mockedLocalClockService = mock(ClockService.class);
    private final ClockService mockedRemoteClockService = mock(ClockService.class);

    private ClockSkewEvents mockedEvents;
    private final Map<String, ClockService> monitorByServer = new HashMap<>();

    @SuppressWarnings("unchecked")
    private DeterministicScheduler executorService = new DeterministicScheduler();
    private ClockSkewMonitor monitor;

    @Before
    public void setUp() {
        mockedEvents = mock(ClockSkewEvents.class);
        monitorByServer.put(server, mockedRemoteClockService);
        monitor = new ClockSkewMonitor(monitorByServer,
                mockedEvents, executorService, mockedLocalClockService);
        monitor.runInBackground();
    }

    @Test
    public void logsRequestsWithoutSkew() {
        RequestTime requestTime = RequestTime.builder()
                .localTimeAtStart(1)
                .localTimeAtEnd(1)
                .remoteSystemTime(1)
                .build();
        mockLocalAndRemoteClockSuppliers(requestTime);
        executorService.tick(1, TimeUnit.NANOSECONDS);

        RequestTime remoteTime = requestTime
                .progressLocalClock(100L)
                .progressRemoteClock(100L);
        mockLocalAndRemoteClockSuppliers(remoteTime);
        tickOneIteration();

        verify(mockedEvents, times(1))
                .clockSkew(server, 0L, 100L, 0L);
    }

    @Test
    public void logsRequestsWithSkew() {
        RequestTime requestTime = RequestTime.builder()
                .localTimeAtStart(1)
                .localTimeAtEnd(1)
                .remoteSystemTime(1)
                .build();
        mockLocalAndRemoteClockSuppliers(requestTime);
        executorService.tick(1, TimeUnit.NANOSECONDS);

        RequestTime remoteTime = requestTime
                .progressLocalClock(100L)
                .progressRemoteClock(200L);
        mockLocalAndRemoteClockSuppliers(remoteTime);
        tickOneIteration();

        verify(mockedEvents, times(1))
                .clockSkew(server, 100L, 100L, 0L);
    }

    @Test
    public void logsIfLocalTimeGoesBackwards() {
        when(mockedLocalClockService.getSystemTimeInNanos())
                .thenReturn(10L)
                .thenReturn(5L);
        tickOneIteration();

        verify(mockedEvents, times(1))
                .clockWentBackwards("local", 5);
    }

    @Test
    public void logsIfRemoteTimeGoesBackwards() {
        when(mockedRemoteClockService.getSystemTimeInNanos())
                .thenReturn(10L)
                .thenReturn(5L);
        tickOneIteration();

        verify(mockedEvents, times(1))
                .clockWentBackwards(server, 5);
    }

    private void tickOneIteration() {
        executorService.tick(ClockSkewMonitor.PAUSE_BETWEEN_REQUESTS.toNanos(), TimeUnit.NANOSECONDS);
    }

    private void mockLocalAndRemoteClockSuppliers(RequestTime request) {
        when(mockedLocalClockService.getSystemTimeInNanos()).thenReturn(request.localTimeAtStart(),
                request.localTimeAtEnd());
        when(mockedRemoteClockService.getSystemTimeInNanos()).thenReturn(request.remoteSystemTime());
    }

    @After
    public void tearDown() throws InterruptedException {
        verifyNoMoreInteractions(mockedEvents);
    }
}
