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

package com.palantir.atlasdb.timelock.transaction.client;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.github.benmanes.caffeine.cache.Ticker;
import com.google.common.collect.Iterables;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import org.jmock.lib.concurrent.DeterministicScheduler;
import org.junit.Before;
import org.junit.Test;

public class CachingPartitionAllocatorTest {
    private static final String KEY = "foo";

    private final DeterministicScheduler scheduler = new DeterministicScheduler();
    private final AtomicLong time = new AtomicLong();
    private final Ticker ticker = time::get;
    private final DistributingModulusGenerator generator = mock(DistributingModulusGenerator.class);
    private final CachingPartitionAllocator<String> allocator =
            new CachingPartitionAllocator<>(generator, scheduler, ticker, Duration.of(5, ChronoUnit.NANOS));

    @Before
    public void setUp() {
        when(generator.getAndMarkResidue()).thenReturn(0);
    }

    @Test
    public void cachesResults() {
        List<Integer> firstResponse = allocator.getRelevantModuli(KEY);
        List<Integer> secondResponse = allocator.getRelevantModuli(KEY);
        List<Integer> thirdResponse = allocator.getRelevantModuli(KEY);

        verify(generator, times(1)).getAndMarkResidue();
        verifyNoMoreInteractions(generator);

        assertThat(firstResponse).isEqualTo(secondResponse).isEqualTo(thirdResponse);
    }

    @Test
    public void unmarksResidueIfClientGoesAway() {
        List<Integer> firstResponse = allocator.getRelevantModuli(KEY);

        time.addAndGet(5_000_000L);

        allocator.getRelevantModuli(KEY);
        scheduler.runUntilIdle();
        verify(generator, times(2)).getAndMarkResidue();
        verify(generator, times(1)).unmarkResidue(Iterables.getOnlyElement(firstResponse));
        verifyNoMoreInteractions(generator);
    }
}
