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

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class ClockSkewComparerTest {
    private final String server = "server";
    private final ClockSkewEvents mockedEvents = mock(ClockSkewEvents.class);
    private RequestTime originalRequest;

    @Before
    public void setUp() {
        originalRequest = RequestTime.builder()
                .localTimeAtStart(0)
                .localTimeAtEnd(1)
                .remoteSystemTime(0)
                .build();
    }

    @Test
    public void handlesNegativeRemoteElapsedTimes() {
        // Make remote clock go back in time. Since time just goes forward, this would only happen when time
        // overflows.
        RequestTime nextRequest = originalRequest
                .progressLocalClock(1L)
                .progressRemoteClock(-1L);

        compare(originalRequest, nextRequest);
    }

    @Test
    public void handlesNegativeLocalElapsedTimes() {
        // Make local clock go back in time. Since time just goes forward, this would only happen when time
        // overflows.
        RequestTime nextRequest = originalRequest
                .progressLocalClock(-1L)
                .progressRemoteClock(1L);

        compare(originalRequest, nextRequest);
    }

    @Test
    public void logsRequestsWithoutSkew() {
        RequestTime nextRequest = originalRequest
                .progressLocalClock(100L)
                .progressRemoteClock(100L);
        compare(originalRequest, nextRequest);

        verify(mockedEvents, times(1))
                .clockSkew(server, 0L, 100L - 1L, 1L);
    }

    @Test
    public void logsRequestsWithSlowRemote() {
        RequestTime nextRequest = originalRequest
                .progressLocalClock(3L)
                .progressRemoteClock(1L);
        compare(originalRequest, nextRequest);

        verify(mockedEvents, times(1))
                .clockSkew(server, 1L, 3L - 1L, 1L);
    }

    @Test
    public void logsRequestsWithFastRemote() {
        RequestTime nextRequest = originalRequest
                .progressLocalClock(2L)
                .progressRemoteClock(4L);
        compare(originalRequest, nextRequest);

        verify(mockedEvents, times(1))
                .clockSkew(server, 1L, 2L - 1L, 1L);
    }

    @After
    public void tearDown() {
        verifyNoMoreInteractions(mockedEvents);
    }

    private void compare(RequestTime previousRequest, RequestTime nextRequest) {
        new ClockSkewComparer(server, mockedEvents, previousRequest, nextRequest).compare();
    }
}
