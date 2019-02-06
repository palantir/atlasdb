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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.Random;
import java.util.UUID;

import org.junit.After;
import org.junit.Test;
import org.mpierce.metrics.reservoir.hdrhistogram.HdrHistogramReservoir;

import com.codahale.metrics.Histogram;

public class ReversalDetectingClockServiceTest {

    private static final String SERVER_NAME = "foo";

    private final ClockService delegate = mock(ClockService.class);
    private final ClockSkewEvents events = mock(ClockSkewEvents.class);
    private final ReversalDetectingClockService clock = new ReversalDetectingClockService(delegate, SERVER_NAME,
            events);

    @After
    public void after() {
        verifyNoMoreInteractions(events);
    }

    private static IdentifiedSystemTime st(long time) {
        return IdentifiedSystemTime.of(time, new UUID(0, 0));
    }

    @Test
    public void doesNotLogIfClockDoesNotGoBackwards() {
        when(delegate.getSystemTime())
                .thenReturn(st(1L))
                .thenReturn(st(2L))
                .thenReturn(st(3L));

        clock.getSystemTime();
        clock.getSystemTime();
        clock.getSystemTime();
    }

    @Test
    public void doesNotLogIfSystemChanges() {
        when(delegate.getSystemTime())
                .thenReturn(IdentifiedSystemTime.of(1L, new UUID(0, 0)))
                .thenReturn(IdentifiedSystemTime.of(-1L, new UUID(1, 1)));
        clock.getSystemTime();
        clock.getSystemTime();
    }

    @Test
    public void logsIfClockGoesBackwards() {
        when(delegate.getSystemTime())
                .thenReturn(st(5L))
                .thenReturn(st(2L));

        clock.getSystemTime();
        clock.getSystemTime();

        verify(events).clockWentBackwards(SERVER_NAME, 3L);
    }

    @Test
    public void returnsDelegateValueRegardlessOfWhetherClockGoesBackwards() {
        when(delegate.getSystemTime())
                .thenReturn(st(1L))
                .thenReturn(st(2L))
                .thenReturn(st(1L))
                .thenReturn(st(3L));

        assertThat(clock.getSystemTime()).isEqualTo(st(1));
        assertThat(clock.getSystemTime()).isEqualTo(st(2));
        assertThat(clock.getSystemTime()).isEqualTo(st(1));
        assertThat(clock.getSystemTime()).isEqualTo(st(3));

        verify(events).clockWentBackwards(SERVER_NAME, 1L);
    }

}
