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

    private RequestTime originalRequest;

    @Before
    public void setUp() {
        mockedEvents = mock(ClockSkewEvents.class);
        monitorByServer.put(server, mockedRemoteClockService);
        monitor = new ClockSkewMonitor(monitorByServer,
                mockedEvents, executorService, mockedLocalClockService);
        monitor.runInBackground();

        originalRequest = RequestTime.builder()
                .localTimeAtStart(0)
                .localTimeAtEnd(1)
                .remoteSystemTime(0)
                .build();

        mockLocalAndRemoteClockSuppliers(originalRequest);
        executorService.tick(1, TimeUnit.NANOSECONDS);
    }

    @Test
    public void logsRequestsWithoutSkew() {
        RequestTime nextRequest = originalRequest
                .progressLocalClock(100L)
                .progressRemoteClock(100L);

        mockLocalAndRemoteClockSuppliers(nextRequest);
        tickOneIteration();

        ClockSkewEvent expectedClockSkew = ImmutableClockSkewEvent.builder()
                .maxElapsedTime(100L + 1L)
                .minElapsedTime(100L - 1L)
                .remoteElapsedTime(100L)
                .build();


        verify(mockedEvents, times(1))
                .clockSkew(server, expectedClockSkew, 1L);
    }

    @Test
    public void logsRequestsWithFastRemote() {
        RequestTime nextRequst = originalRequest
                .progressLocalClock(100L)
                .progressRemoteClock(200L);

        mockLocalAndRemoteClockSuppliers(nextRequst);
        tickOneIteration();

        ClockSkewEvent expectedClockSkew = ImmutableClockSkewEvent.builder()
                .maxElapsedTime(100L + 1L)
                .minElapsedTime(100L - 1L)
                .remoteElapsedTime(200L)
                .build();

        verify(mockedEvents, times(1))
                .clockSkew(server, expectedClockSkew, 1L);
    }

    @Test
    public void logsRequestsWithSlowRemote() {
        RequestTime nextRequst = originalRequest
                .progressLocalClock(100L)
                .progressRemoteClock(50L);

        mockLocalAndRemoteClockSuppliers(nextRequst);
        tickOneIteration();

        ClockSkewEvent expectedClockSkew = ImmutableClockSkewEvent.builder()
                .maxElapsedTime(100L + 1L)
                .minElapsedTime(100L - 1L)
                .remoteElapsedTime(50L)
                .build();

        verify(mockedEvents, times(1))
                .clockSkew(server, expectedClockSkew, 1L);
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
    public void tearDown() {
        verifyNoMoreInteractions(mockedEvents);
    }
}
