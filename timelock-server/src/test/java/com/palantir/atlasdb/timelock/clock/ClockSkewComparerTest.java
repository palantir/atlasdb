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

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import static com.palantir.atlasdb.timelock.clock.ClockSkewComparer.MAX_INTERVAL_SINCE_PREVIOUS_REQUEST;
import static com.palantir.atlasdb.timelock.clock.ClockSkewComparer.MAX_REQUEST_DURATION;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class ClockSkewComparerTest {
    private final String server = "server";
    private final ClockSkewEvents mockedEvents = mock(ClockSkewEvents.class);
    private RequestTime originalRequest;


    @Before
    public void setUp() {
        originalRequest = new RequestTime(0L, 1L, 0L);
    }

    @Test
    public void throwsIfElapsedTimeIsNegative() {
        // Make remote clock go back in time. Since time just goes forward, this would only happen when time
        // overflows.
        RequestTime nextRequest = new RequestTimeBuilder(originalRequest)
                .progressLocalClock(1L)
                .progressRemoteClock(-1L)
                .build();

        assertThatThrownBy(() -> compare(originalRequest, nextRequest))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void throwsOnLocalOverflow() {
        // Make local clock go back in time. Since time just goes forward, this would only happen when time
        // overflows.
        RequestTime nextRequest = new RequestTimeBuilder(originalRequest)
                .progressLocalClock(-1L)
                .progressRemoteClock(1L)
                .build();

        assertThatThrownBy(() -> compare(originalRequest, nextRequest))
                .isInstanceOf(IllegalArgumentException.class);
    }


    @Test
    public void logsWhenServerHasNotBeenQueriedForTooLong() {
        RequestTime nextRequest = new RequestTimeBuilder(originalRequest)
                .progressLocalClock(MAX_INTERVAL_SINCE_PREVIOUS_REQUEST.toNanos() + 2)
                .progressRemoteClock(1L)
                .build();
        compare(originalRequest, nextRequest);

        verify(mockedEvents, times(1)).tooMuchTimeSincePreviousRequest(anyLong());
    }

    @Test
    public void logsWhenRequestsTakeTooLong() {
        originalRequest = new RequestTime(0L, MAX_REQUEST_DURATION.toNanos() + 1, 0L);
        RequestTime nextRequest = new RequestTimeBuilder(originalRequest)
                .progressLocalClock(MAX_INTERVAL_SINCE_PREVIOUS_REQUEST.toNanos() + 2)
                .progressRemoteClock(1L)
                .build();
        compare(originalRequest, nextRequest);

        verify(mockedEvents, times(1)).requestsTookTooLong(anyLong(), anyLong());
    }

    @Test
    public void logsRequestsWithoutSkew() {
        RequestTime nextRequest = new RequestTimeBuilder(originalRequest)
                .progressLocalClock(100L)
                .progressRemoteClock(100L)
                .build();
        compare(originalRequest, nextRequest);

        verify(mockedEvents, times(1))
                .requestPace(anyString(), anyLong(), anyLong(), anyLong());
    }

    @Test
    public void logsRequestsWithSlowRemote() {
        RequestTime nextRequest = new RequestTimeBuilder(originalRequest)
                .progressLocalClock(3L)
                .progressRemoteClock(1L)
                .build();
        compare(originalRequest, nextRequest);

        verify(mockedEvents, times(1))
                .requestPace(anyString(), anyLong(), anyLong(), anyLong());
        verify(mockedEvents, times(1))
                .clockSkew(server, 1L);
    }

    @Test
    public void logsRequestsWithFastRemote() {
        RequestTime nextRequest = new RequestTimeBuilder(originalRequest)
                .progressLocalClock(2L)
                .progressRemoteClock(4L)
                .build();
        compare(originalRequest, nextRequest);

        verify(mockedEvents, times(1))
                .requestPace(anyString(), anyLong(), anyLong(), anyLong());
        verify(mockedEvents, times(1))
                .clockSkew(server, 1L);
    }

    @After
    public void tearDown() {
        verifyNoMoreInteractions(mockedEvents);
    }

    private void compare(RequestTime previousRequest, RequestTime nextRequest) {
        new ClockSkewComparer(server, mockedEvents, previousRequest, nextRequest).compare();
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
