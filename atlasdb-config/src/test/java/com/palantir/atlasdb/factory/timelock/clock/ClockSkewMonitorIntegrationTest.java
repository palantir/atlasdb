/*
 * (c) Copyright 2019 Palantir Technologies Inc. All rights reserved.
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
package com.palantir.atlasdb.factory.timelock.clock;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.jmock.lib.concurrent.DeterministicScheduler;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class ClockSkewMonitorIntegrationTest {
    private static final UUID LOCAL_ID = new UUID(0, 0);
    private static final UUID REMOTE_ID = new UUID(1, 1);
    private static final UUID DIFFERENT_REMOTE_ID = new UUID(2, 2);

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
                .remoteSystemId(REMOTE_ID)
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
        when(mockedLocalClockService.getSystemTime()).thenReturn(local(-1L));
        tickOneIteration();

        verify(mockedEvents, times(1)).clockWentBackwards("local", 2);
    }

    @Test
    public void logsIfRemoteTimeGoesBackwards() {
        when(mockedRemoteClockService.getSystemTime())
                .thenReturn(remote(-1L));
        tickOneIteration();

        verify(mockedEvents, times(1))
                .clockWentBackwards(server, 1);
    }

    @Test
    public void doesNotLogIfRemoteChanges() {
        when(mockedRemoteClockService.getSystemTime())
                .thenReturn(differentRemote(-1L));
        tickOneIteration();
        verifyZeroInteractions(mockedEvents);
    }

    private void tickOneIteration() {
        executorService.tick(ClockSkewMonitor.PAUSE_BETWEEN_REQUESTS.toNanos(), TimeUnit.NANOSECONDS);
    }

    private void mockLocalAndRemoteClockSuppliers(RequestTime request) {
        when(mockedLocalClockService.getSystemTime()).thenReturn(local(request.localTimeAtStart()),
                local(request.localTimeAtEnd()));
        when(mockedRemoteClockService.getSystemTime()).thenReturn(remote(request.remoteSystemTime()));
    }

    @After
    public void tearDown() {
        verifyNoMoreInteractions(mockedEvents);
    }

    private static IdentifiedSystemTime local(long time) {
        return IdentifiedSystemTime.of(time, LOCAL_ID);
    }

    private static IdentifiedSystemTime remote(long time) {
        return IdentifiedSystemTime.of(time, REMOTE_ID);
    }

    private static IdentifiedSystemTime differentRemote(long time) {
        return IdentifiedSystemTime.of(time, DIFFERENT_REMOTE_ID);
    }
}
