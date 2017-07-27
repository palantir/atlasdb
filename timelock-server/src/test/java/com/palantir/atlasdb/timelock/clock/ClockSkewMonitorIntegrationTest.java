/*
 * Copyright 2017 Palantir Technologies, Inc. All rights reserved.
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

import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
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
    private final ClockService mockedLocalTimeSupplier = mock(ClockService.class);
    private final ClockService mockedRemoteClockService = mock(ClockService.class);

    private ClockSkewEvents mockedEvents;
    private final Map<String, ClockService> monitorByServer = new HashMap<>();
    private final Map<String, RequestTime> previousRequestsByServer = new HashMap<>();

    @SuppressWarnings("unchecked")
    private DeterministicScheduler executorService = new DeterministicScheduler();
    private ClockSkewMonitor monitor;

    @Before
    public void setUp() {
        mockedEvents = mock(ClockSkewEvents.class);
        monitorByServer.put(server, mockedRemoteClockService);
        previousRequestsByServer.put(server, RequestTime.EMPTY);
        monitor = new ClockSkewMonitor(monitorByServer, previousRequestsByServer,
                mockedEvents, executorService, mockedLocalTimeSupplier);
    }

    @Test
    public void logsRequestsWithoutSkew() {
        monitor.runInBackground();
        RequestTime requestTime = new RequestTime(1L, 1L, 1L);
        mockLocalAndRemoteClockSuppliers(requestTime);
        executorService.tick(1, TimeUnit.NANOSECONDS);

        RequestTime remoteTime = new RequestTime.Builder(requestTime)
                .progressLocalClock(100L)
                .progressRemoteClock(100L)
                .build();
        mockLocalAndRemoteClockSuppliers(remoteTime);
        executorService.tick(ClockSkewMonitor.PAUSE_BETWEEN_REQUESTS.toNanos(), TimeUnit.NANOSECONDS);

        verify(mockedEvents, times(1))
                .requestPace(anyString(), anyLong(), anyLong(), anyLong());
    }

    @Test
    public void logsRequestsWithSkew() {
        monitor.runInBackground();
        RequestTime requestTime = new RequestTime(1L, 1L, 1L);
        mockLocalAndRemoteClockSuppliers(requestTime);
        executorService.tick(1, TimeUnit.NANOSECONDS);

        RequestTime remoteTime = new RequestTime.Builder(requestTime)
                .progressLocalClock(100L)
                .progressRemoteClock(200L)
                .build();
        mockLocalAndRemoteClockSuppliers(remoteTime);
        executorService.tick(ClockSkewMonitor.PAUSE_BETWEEN_REQUESTS.toNanos(), TimeUnit.NANOSECONDS);

        verify(mockedEvents, times(1))
                .requestPace(anyString(), anyLong(), anyLong(), anyLong());
        verify(mockedEvents, times(1))
                .clockSkew(anyString(), eq(100L));
    }

    private void mockLocalAndRemoteClockSuppliers(RequestTime request) {
        when(mockedLocalTimeSupplier.getSystemTimeInNanos()).thenReturn(request.localTimeAtStart,
                request.localTimeAtEnd);
        when(mockedRemoteClockService.getSystemTimeInNanos()).thenReturn(request.remoteSystemTime);
    }

    @After
    public void tearDown() throws InterruptedException {
        verifyNoMoreInteractions(mockedEvents);
    }
}
