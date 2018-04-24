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

import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import javax.ws.rs.BadRequestException;

import org.jmock.lib.concurrent.DeterministicScheduler;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InOrder;

import com.google.common.collect.ImmutableMap;
import com.palantir.common.remoting.ServiceNotAvailableException;

public class MultiNodeClockSkewMonitorIntegrationTest {
    private static final int NUM_REMOTE_HOSTS = 2;
    private static final String REMOTE_HOST_1 = "host1";
    private static final String REMOTE_HOST_2 = "host2";

    private final ClockService localClock = mock(ClockService.class);
    private final ClockService remoteClock1 = mock(ClockService.class);
    private final ClockService remoteClock2 = mock(ClockService.class);

    private final Map<String, ClockService> clockServices = ImmutableMap.<String, ClockService>builder()
            .put(REMOTE_HOST_1, remoteClock1)
            .put(REMOTE_HOST_2, remoteClock2)
            .build();
    private final ClockSkewEvents events = mock(ClockSkewEvents.class);
    private final DeterministicScheduler executor = new DeterministicScheduler();
    private final ClockSkewMonitor clockSkewMonitor =
            new ClockSkewMonitor(clockServices, events, executor, localClock);

    @Before
    public void setUp() {
        clockSkewMonitor.runInBackground();

        // Causes the clock skew monitor to get past its initial run.
        executor.tick(1, TimeUnit.NANOSECONDS);
    }

    @Test
    public void registersMultipleExceptionsIfThereAreMultipleFailures() {
        Exception serviceNotAvailable = new ServiceNotAvailableException("foo");
        Exception badRequest = new BadRequestException("bar");

        when(remoteClock1.getSystemTimeInNanos()).thenThrow(serviceNotAvailable);
        when(remoteClock2.getSystemTimeInNanos()).thenThrow(badRequest);
        tickOneIteration();

        verify(events).exception(eq(serviceNotAvailable));
        verify(events).exception(eq(badRequest));
    }

    @Test
    public void registersMultipleClockWentBackwardsEvents() {
        when(remoteClock1.getSystemTimeInNanos()).thenReturn(2L, 1L);
        when(remoteClock2.getSystemTimeInNanos()).thenReturn(4L, 3L);

        tickOneIteration();
        tickOneIteration();

        verify(events).clockSkew(REMOTE_HOST_1, 2L, 0L, 0L);
        verify(events).clockSkew(REMOTE_HOST_2, 4L, 0L, 0L);
        verify(events).clockWentBackwards(REMOTE_HOST_1, 1L);
        verify(events).clockWentBackwards(REMOTE_HOST_2, 1L);
    }

    @Test
    public void registersCombinationsOfFailuresCorrectly() {
        when(remoteClock1.getSystemTimeInNanos()).thenReturn(2L, 1L);

        Exception badRequest = new BadRequestException("bar");
        when(remoteClock2.getSystemTimeInNanos()).thenThrow(badRequest);

        tickOneIteration();
        tickOneIteration();

        verify(events).clockSkew(REMOTE_HOST_1, 2L, 0L, 0L);
        verify(events).clockWentBackwards(REMOTE_HOST_1, 1L);
        verify(events, times(2)).exception(eq(badRequest));
    }

    @Test
    public void registersClockSkewFromMultipleNodes() {
        when(localClock.getSystemTimeInNanos()).thenReturn(100L, 110L, 120L, 130L);
        when(remoteClock1.getSystemTimeInNanos()).thenReturn(105L);
        when(remoteClock2.getSystemTimeInNanos()).thenReturn(125L);
        tickOneIteration();

        when(localClock.getSystemTimeInNanos()).thenReturn(140L, 150L, 160L, 170L);
        when(remoteClock1.getSystemTimeInNanos()).thenReturn(1105L + 50L);
        when(remoteClock2.getSystemTimeInNanos()).thenReturn(5125L + 50L);
        tickOneIteration();

        // Don't really like this, but didn't find an easy way of verifying subsequences of events.
        InOrder inOrder = inOrder(events);
        inOrder.verify(events).clockSkew(REMOTE_HOST_1, 0L, 100L, 10L);
        inOrder.verify(events).clockSkew(REMOTE_HOST_2, 0L, 120L, 10L);
        inOrder.verify(events).clockSkew(REMOTE_HOST_1, 1000L, 30L, 10L);
        inOrder.verify(events).clockSkew(REMOTE_HOST_2, 5000L, 30L, 10L);
    }

    @Test
    public void registersBothFailuresAndClockSkew() {
        Exception serviceNotAvailable = new ServiceNotAvailableException("foo");
        when(remoteClock1.getSystemTimeInNanos()).thenThrow(serviceNotAvailable);

        // The first value is a peculiarity of the implementation. We get the local time and THEN query the remote
        // server.
        when(localClock.getSystemTimeInNanos()).thenReturn(100L, 110L, 120L);
        when(remoteClock2.getSystemTimeInNanos()).thenReturn(115L);
        tickOneIteration();

        when(localClock.getSystemTimeInNanos()).thenReturn(200L, 210L, 220L);
        when(remoteClock2.getSystemTimeInNanos()).thenReturn(515L + 110L);
        tickOneIteration();

        verify(events).clockSkew(REMOTE_HOST_2, 0L, 110L, 10L);
        verify(events).clockSkew(REMOTE_HOST_2, 400L, 90L, 10L);
        verify(events, times(2)).exception(serviceNotAvailable);
    }

    private void tickOneIteration() {
        executor.tick(ClockSkewMonitor.PAUSE_BETWEEN_REQUESTS.toNanos(), TimeUnit.NANOSECONDS);
    }

    @After
    public void tearDown() throws InterruptedException {
        verifyNoMoreInteractions(events);
    }
}
