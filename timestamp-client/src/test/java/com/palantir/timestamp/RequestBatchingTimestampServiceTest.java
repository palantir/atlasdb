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
package com.palantir.timestamp;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.Test;

import com.palantir.common.concurrent.PTExecutors;

public class RequestBatchingTimestampServiceTest {
    @Test
    public void delegatesInitializationCheck() {
        TimestampService delegate = mock(TimestampService.class);
        RequestBatchingTimestampService service = new RequestBatchingTimestampService(delegate);

        when(delegate.isInitialized())
                .thenReturn(false)
                .thenReturn(true);

        assertFalse(service.isInitialized());
        assertTrue(service.isInitialized());
    }

    @Test
    public void testRateLimiting() throws Exception {
        final long minRequestMillis = 100L;
        final long testDurationMillis = 2000L;
        final int numThreads = 3;

        final StatsTrackingTimestampService rawTs = new StatsTrackingTimestampService(new InMemoryTimestampService());
        final RequestBatchingTimestampService cachedTs = new RequestBatchingTimestampService(rawTs, minRequestMillis);
        final AtomicLong timestampsGenerated = new AtomicLong(0);
        final long startMillis = System.currentTimeMillis();

        ExecutorService executor = PTExecutors.newCachedThreadPool();
        try {
            for (int i = 0; i < numThreads; ++i) {
                executor.submit(() -> {
                    while (System.currentTimeMillis() - startMillis < testDurationMillis) {
                        cachedTs.getFreshTimestamp();
                        timestampsGenerated.incrementAndGet();
                    }
                    return null;
                });
            }
        } finally {
            executor.shutdown();
            executor.awaitTermination(1000, TimeUnit.SECONDS);
        }

        assertEquals(0, rawTs.getFreshTimestampReqCount.get());
        long approxFreshTimestampReqTotal = rawTs.getFreshTimestampsReqCount.get() * numThreads;
        assertEquals(approxFreshTimestampReqTotal, timestampsGenerated.get(), approxFreshTimestampReqTotal);
    }

    private static class StatsTrackingTimestampService implements TimestampService {
        AtomicLong getFreshTimestampReqCount = new AtomicLong(0);
        AtomicLong getFreshTimestampsReqCount = new AtomicLong(0);
        AtomicLong timestampsCount = new AtomicLong(0);

        final TimestampService delegate;

        StatsTrackingTimestampService(TimestampService delegate) {
            this.delegate = delegate;
        }

        @Override
        public long getFreshTimestamp() {
            getFreshTimestampReqCount.incrementAndGet();
            timestampsCount.incrementAndGet();
            return delegate.getFreshTimestamp();
        }

        @Override
        public TimestampRange getFreshTimestamps(int numTimestampsRequested) {
            getFreshTimestampsReqCount.incrementAndGet();
            timestampsCount.addAndGet(numTimestampsRequested);
            return delegate.getFreshTimestamps(numTimestampsRequested);
        }
    }
}
