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
    public void testAimd() {
        int windowSize = 10;
        Random rand = new Random(0);

        for (double slope = 1; slope <= 6; slope += 0.2) {
            for (double x0 = 1.0; x0 <= 3; x0 += 0.2) {
                for (double systemLoadAvg = 0.5; systemLoadAvg <= 1; systemLoadAvg += 0.1) {
                    Histogram histogram = new Histogram(new HdrHistogramReservoir());
                    double qosProbability = Math.pow(systemLoadAvg / x0, slope) * Math.exp((systemLoadAvg / x0) - 1);
                    int currentSuccessCount = 0;
                    for (int i = 0; i < 100_000; i++) {
                        windowSize = Math.max(windowSize, 1);
                        histogram.update(windowSize);
                        if (currentSuccessCount == 10) {
                            currentSuccessCount = 0;
                            windowSize++;
                        }
                        if (rand.nextDouble() > 1 - qosProbability) {
                            currentSuccessCount = 0;
                            windowSize = (windowSize * 9) / 10;
                        } else {
                            currentSuccessCount++;
                        }
                    }
                    System.out.println("slope: " + slope
                            + " x0: " + x0
                            + " systemLoad: " + systemLoadAvg
                            + " redirect-probability " + qosProbability
                            + " concurrent-requests " + histogram.getSnapshot().getMean());
                }
            }
        }
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
