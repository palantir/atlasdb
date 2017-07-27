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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import static com.palantir.atlasdb.timelock.clock.ClockSkewMonitor.PAUSE_BETWEEN_REQUESTS;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.jmock.lib.concurrent.DeterministicScheduler;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.internal.util.collections.Sets;

import com.google.common.collect.Maps;

public class ClockSkewMonitorIntegrationTest {
    private final String server = "test";
    private final ClockService mockedLocalTimeSupplier = mock(ClockService.class);
    private final ClockService mockedRemoteClockService = mock(ClockService.class);
    private final ClockSkewEvents mockedEvents = mock(ClockSkewEvents.class);

    private final Map<String, ClockService> monitorByServer = new HashMap<>();
    private final Map<String, RequestTime> previousRequestsByServer = new HashMap<>();

    @SuppressWarnings("unchecked")
    private DeterministicScheduler executorService = new DeterministicScheduler();
    private ClockSkewMonitor monitor;

    @Before
    public void setUp() {
        monitorByServer.put(server, mockedRemoteClockService);
        previousRequestsByServer.put(server, RequestTime.EMPTY);
        monitor = new ClockSkewMonitor(monitorByServer, previousRequestsByServer,
                mockedEvents, executorService, mockedLocalTimeSupplier);
    }

    @Test
    public void logsRequestsWithoutSkew() {
        monitor.runInBackground();

        RequestTime requestTime = new RequestTime(0L, 1L, 0L);
        mockLocalAndRemoteClockSuppliers(requestTime);
        executorService.tick(PAUSE_BETWEEN_REQUESTS.toNanos() + 1, TimeUnit.NANOSECONDS);

        RequestTime remoteTime = new RequestTimeBuilder(requestTime)
                .progressLocalClock(100L)
                .progressRemoteClock(100L)
                .build();
        mockLocalAndRemoteClockSuppliers(remoteTime);
        executorService.tick(PAUSE_BETWEEN_REQUESTS.toNanos() + 1, TimeUnit.NANOSECONDS);

        verify(mockedEvents, times(1))
                .requestPace(anyString(), anyLong(), anyLong(), anyLong());
    }

    private void mockLocalAndRemoteClockSuppliers(RequestTime originalRequest) {
        when(mockedLocalTimeSupplier.getSystemTimeInNanos()).thenReturn(originalRequest.localTimeAtStart,
                originalRequest.localTimeAtEnd);
        when(mockedRemoteClockService.getSystemTimeInNanos()).thenReturn(originalRequest.remoteSystemTime);
    }

    @After
    public void tearDown() throws InterruptedException {
        monitorByServer.clear();
        previousRequestsByServer.clear();
    }

    private class RequestTimeBuilder {
        private long localTimeAtStart;
        private long localTimeAtEnd;
        private long remoteSystemTime;

        private RequestTimeBuilder(RequestTime requestTime) {
            localTimeAtStart = requestTime.localTimeAtStart;
            localTimeAtEnd = requestTime.localTimeAtEnd;
            remoteSystemTime = requestTime.remoteSystemTime;
        }

        private RequestTimeBuilder progressLocalClock(long delta) {
            localTimeAtStart += delta;
            localTimeAtEnd += delta;
            return this;
        }

        private RequestTimeBuilder progressRemoteClock(long delta) {
            remoteSystemTime += delta;
            return this;
        }

        private RequestTime build() {
            return new RequestTime(localTimeAtStart, localTimeAtEnd, remoteSystemTime);
        }
    }
}
