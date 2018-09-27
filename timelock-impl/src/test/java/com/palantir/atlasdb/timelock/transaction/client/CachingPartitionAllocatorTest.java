/*
 * Copyright 2018 Palantir Technologies, Inc. All rights reserved.
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

package com.palantir.atlasdb.timelock.transaction.client;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.Before;
import org.junit.Test;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.github.benmanes.caffeine.cache.RemovalCause;
import com.github.benmanes.caffeine.cache.Ticker;
import com.google.common.collect.Iterables;

public class CachingPartitionAllocatorTest {
    private static final String KEY = "foo";

    private final AtomicLong time = new AtomicLong();
    private final Ticker ticker = time::get;
    private final DistributingModulusGenerator generator = mock(DistributingModulusGenerator.class);
    private final LoadingCache<String, Integer> loadingCache = Caffeine.newBuilder()
            .expireAfterAccess(5, TimeUnit.NANOSECONDS)
            .removalListener(
                    (String key, Integer value, RemovalCause cause) -> generator.unmarkResidue(value))
            .ticker(ticker)
            .build(unused -> generator.getAndMarkResidue());
    private final CachingPartitionAllocator<String> allocator = new CachingPartitionAllocator<>(loadingCache);

    @Before
    public void setUp() {
        when(generator.getAndMarkResidue()).thenReturn(0);
    }

    @Test
    public void cachesResults() {
        List<Integer> firstResponse = allocator.getRelevantModuli(KEY);
        List<Integer> secondResponse = allocator.getRelevantModuli(KEY);
        List<Integer> thirdResponse = allocator.getRelevantModuli(KEY);

        verify(generator).getAndMarkResidue(); // only once
        verifyNoMoreInteractions(generator);

        assertThat(firstResponse).isEqualTo(secondResponse).isEqualTo(thirdResponse);
    }

    @Test
    public void unmarksResidueIfClientGoesAway() {
        List<Integer> firstResponse = allocator.getRelevantModuli(KEY);

        time.addAndGet(5_000_000L);
        loadingCache.cleanUp();

        allocator.getRelevantModuli(KEY);
        verify(generator, times(2)).getAndMarkResidue();
        verify(generator, times(1)).unmarkResidue(Iterables.getOnlyElement(firstResponse));
        verifyNoMoreInteractions(generator);
    }
}
