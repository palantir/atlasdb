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

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
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

import com.google.common.collect.ImmutableMap;
import com.palantir.common.remoting.ServiceNotAvailableException;

public class MultiNodeClockSkewMonitorIntegrationTest {
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
        when(remoteClock1.getSystemTimeInNanos()).thenReturn(-1L);
        when(remoteClock2.getSystemTimeInNanos()).thenReturn(-2L);

        tickOneIteration();

        verify(events).clockWentBackwards(REMOTE_HOST_1, 1L);
        verify(events).clockWentBackwards(REMOTE_HOST_2, 2L);
    }

    @Test
    public void registersCombinationsOfFailuresCorrectly() {
        when(remoteClock1.getSystemTimeInNanos()).thenReturn(-1L);

        Exception badRequest = new BadRequestException("bar");
        when(remoteClock2.getSystemTimeInNanos()).thenThrow(badRequest);

        tickOneIteration();

        verify(events).clockWentBackwards(REMOTE_HOST_1, 1L);
        verify(events).exception(eq(badRequest));
    }

    @Test
    public void registersClockSkewFromMultipleNodes() {
        when(localClock.getSystemTimeInNanos()).thenReturn(100L, 110L, 120L, 130L);
        when(remoteClock1.getSystemTimeInNanos()).thenReturn(105L);
        when(remoteClock2.getSystemTimeInNanos()).thenReturn(125L);
        tickOneIteration();

        verify(events).clockSkew(REMOTE_HOST_1, clockSkewEventOf(100L, 110L, 105L), 10L);
        verify(events).clockSkew(REMOTE_HOST_2, clockSkewEventOf(120L, 130L, 125L), 10L);
    }

    @Test
    public void registersBothFailuresAndClockSkew() {
        Exception serviceNotAvailable = new ServiceNotAvailableException("foo");
        when(remoteClock2.getSystemTimeInNanos()).thenThrow(serviceNotAvailable);

        when(localClock.getSystemTimeInNanos()).thenReturn(100L, 110L);
        when(remoteClock1.getSystemTimeInNanos()).thenReturn(115L);
        tickOneIteration();

        verify(events).clockSkew(REMOTE_HOST_1, clockSkewEventOf(100L, 110L, 115L), 10L);
        verify(events).exception(serviceNotAvailable);
    }

    private void tickOneIteration() {
        executor.tick(ClockSkewMonitor.PAUSE_BETWEEN_REQUESTS.toNanos(), TimeUnit.NANOSECONDS);
    }

    private ClockSkewEvent clockSkewEventOf(long minElapsedTime, long maxElapsedTime, long remoteElapsedTime) {
        return ImmutableClockSkewEvent.builder()
                .maxElapsedTime(maxElapsedTime)
                .minElapsedTime(minElapsedTime)
                .remoteElapsedTime(remoteElapsedTime)
                .build();
    }

    @After
    public void tearDown() {
        verifyNoMoreInteractions(events);
    }
}
