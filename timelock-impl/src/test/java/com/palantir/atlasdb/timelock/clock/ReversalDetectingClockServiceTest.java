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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import org.junit.After;
import org.junit.Test;

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

    @Test
    public void doesNotLogIfClockDoesNotGoBackwards() {
        when(delegate.getSystemTimeInNanos())
                .thenReturn(1L)
                .thenReturn(2L)
                .thenReturn(3L);

        clock.getSystemTimeInNanos();
        clock.getSystemTimeInNanos();
        clock.getSystemTimeInNanos();
    }

    @Test
    public void logsIfClockGoesBackwards() {
        when(delegate.getSystemTimeInNanos())
                .thenReturn(5L)
                .thenReturn(2L);

        clock.getSystemTimeInNanos();
        clock.getSystemTimeInNanos();

        verify(events).clockWentBackwards(SERVER_NAME, 3L);
    }

    @Test
    public void returnsDelegateValueRegardlessOfWhetherClockGoesBackwards() {
        when(delegate.getSystemTimeInNanos())
                .thenReturn(1L)
                .thenReturn(2L)
                .thenReturn(1L)
                .thenReturn(3L);

        assertThat(clock.getSystemTimeInNanos()).isEqualTo(1);
        assertThat(clock.getSystemTimeInNanos()).isEqualTo(2);
        assertThat(clock.getSystemTimeInNanos()).isEqualTo(1);
        assertThat(clock.getSystemTimeInNanos()).isEqualTo(3);

        verify(events).clockWentBackwards(SERVER_NAME, 1L);
    }

}
