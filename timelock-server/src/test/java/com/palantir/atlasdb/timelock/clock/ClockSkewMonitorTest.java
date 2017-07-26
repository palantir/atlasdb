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

import static java.lang.Thread.sleep;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class ClockSkewMonitorTest {
    private final String server = "test";
    private final Supplier<Long> mockedLocalTimeSupplier = mock(Supplier.class);
    private final ClockService mockedRemoteClockService = mock(ClockService.class);
    private final ClockSkewEvents mockedEvents = mock(ClockSkewEvents.class);

    private final Map<String, ClockService> monitorByServer = new HashMap<>();
    private final Map<String, ClockSkewMonitor.RequestTime> previousRequestsByServer = new HashMap<>();
    private ClockSkewMonitor.RequestTime requestTime;

    @SuppressWarnings("unchecked")
    private ScheduledExecutorService executorService;
    private ClockSkewMonitor monitor;

    @Before
    public void setUp() {
        monitorByServer.put(server, mockedRemoteClockService);
        previousRequestsByServer.put(server, ClockSkewMonitor.RequestTime.EMPTY);
        executorService = Executors.newSingleThreadScheduledExecutor();
        monitor = new ClockSkewMonitor(monitorByServer, previousRequestsByServer, () -> Boolean.TRUE,
                mockedEvents, executorService, mockedLocalTimeSupplier);
        requestTime = new ClockSkewMonitor.RequestTime(0L, 1L, 0L);
    }

    @Test
    public void monitorCanBeDisabled() throws InterruptedException {
        new ClockSkewMonitor(monitorByServer, previousRequestsByServer, () -> Boolean.FALSE,
                mockedEvents, executorService, mockedLocalTimeSupplier).run();

        // nothing should happen, since we've disabled the monitor.
        sleep(100);

        executorService.shutdown();
        executorService.awaitTermination(100, TimeUnit.MILLISECONDS);
    }

    @Test
    public void throwsOnRemoteOverflow() {
        mockLocalAndRemoteClockSuppliers(requestTime);
        monitor.runInternal();

        // Make remote clock go back in time. Since time just goes forward, this would only happen when time
        // overflows.
        requestTime = progressLocalClock(requestTime, 1L);
        requestTime = progressRemoteClock(requestTime, -1L);
        mockLocalAndRemoteClockSuppliers(requestTime);

        assertThatThrownBy(monitor::runInternal).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void throwsOnLocalOverflow() {
        mockLocalAndRemoteClockSuppliers(requestTime);
        monitor.runInternal();

        // Make local clock go back in time. Since time just goes forward, this would only happen when time
        // overflows.
        requestTime = progressLocalClock(requestTime, -1L);
        requestTime = progressRemoteClock(requestTime, 1L);
        mockLocalAndRemoteClockSuppliers(requestTime);

        assertThatThrownBy(monitor::runInternal).isInstanceOf(IllegalArgumentException.class);
    }


    @Test
    public void logsWhenServerHasNotBeenQueriedForTooLong() {
        mockLocalAndRemoteClockSuppliers(requestTime);
        monitor.runInternal();

        requestTime = progressLocalClock(requestTime, ClockSkewMonitor.MAX_TIME_SINCE_PREVIOUS_REQUEST_NANOS + 2);
        requestTime = progressRemoteClock(requestTime, 1L);
        mockLocalAndRemoteClockSuppliers(requestTime);
        monitor.runInternal();

        verify(mockedEvents, times(1)).tooMuchTimeSinceLastRequest(anyLong());
    }

    @Test
    public void logsWhenRequestsTakeTooLong() {
        requestTime = new ClockSkewMonitor.RequestTime(0L, ClockSkewMonitor.MAX_REQUEST_TIME_NANOS + 1, 0L);
        mockLocalAndRemoteClockSuppliers(requestTime);
        monitor.runInternal();

        requestTime = progressLocalClock(requestTime, ClockSkewMonitor.MAX_REQUEST_TIME_NANOS + 2);
        requestTime = progressRemoteClock(requestTime, 1L);
        mockLocalAndRemoteClockSuppliers(requestTime);
        monitor.runInternal();

        verify(mockedEvents, times(1)).requestsTookTooLong(anyLong(), anyLong());
    }

    @Test
    public void logsRequestsWithoutSkew() {
        mockLocalAndRemoteClockSuppliers(requestTime);
        monitor.runInternal();

        requestTime = progressLocalClock(requestTime, 100L);
        requestTime = progressRemoteClock(requestTime, 100L);
        mockLocalAndRemoteClockSuppliers(requestTime);
        monitor.runInternal();

        verify(mockedEvents, times(1))
                .requestPace(anyString(), anyLong(), anyLong(), anyLong());
    }

    @Test
    public void logsRequestsWithSlowRemote() {
        requestTime = new ClockSkewMonitor.RequestTime(0L, 100L, 0L);
        mockLocalAndRemoteClockSuppliers(requestTime);
        monitor.runInternal();

        requestTime = progressLocalClock(requestTime, 102L);
        requestTime = progressRemoteClock(requestTime, 1L);
        mockLocalAndRemoteClockSuppliers(requestTime);
        monitor.runInternal();

        verify(mockedEvents, times(1))
                .requestPace(anyString(), anyLong(), anyLong(), anyLong());
        verify(mockedEvents, times(1))
                .clockSkew(server, 1L);
    }

    @Test
    public void logsRequestsWithFastRemote() {
        mockLocalAndRemoteClockSuppliers(requestTime);
        monitor.runInternal();

        requestTime = progressLocalClock(requestTime, 2L);
        requestTime = progressRemoteClock(requestTime, 4L);
        mockLocalAndRemoteClockSuppliers(requestTime);
        monitor.runInternal();

        verify(mockedEvents, times(1))
                .requestPace(anyString(), anyLong(), anyLong(), anyLong());
        verify(mockedEvents, times(1))
                .clockSkew(server, 1L);
    }

    private ClockSkewMonitor.RequestTime progressLocalClock(ClockSkewMonitor.RequestTime originalRequest, long delta) {
        return new ClockSkewMonitor.RequestTime(originalRequest.localTimeAtStart + delta,
                originalRequest.localTimeAtEnd + delta,
                originalRequest.remoteSystemTime);
    }

    private ClockSkewMonitor.RequestTime progressRemoteClock(ClockSkewMonitor.RequestTime originalRequest, long delta) {
        return new ClockSkewMonitor.RequestTime(originalRequest.localTimeAtStart, originalRequest.localTimeAtEnd,
                originalRequest.remoteSystemTime + delta);
    }

    private void mockLocalAndRemoteClockSuppliers(ClockSkewMonitor.RequestTime originalRequest) {
        when(mockedLocalTimeSupplier.get()).thenReturn(originalRequest.localTimeAtStart,
                originalRequest.localTimeAtEnd);
        when(mockedRemoteClockService.getSystemTimeInNanos()).thenReturn(originalRequest.remoteSystemTime);
    }

    @After
    public void tearDown() throws InterruptedException {
        executorService.shutdown();
        executorService.awaitTermination(100, TimeUnit.MILLISECONDS);
        monitorByServer.clear();
        previousRequestsByServer.clear();
    }
}
