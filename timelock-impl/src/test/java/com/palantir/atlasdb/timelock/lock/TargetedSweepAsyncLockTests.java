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

package com.palantir.atlasdb.timelock.lock;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import com.codahale.metrics.Meter;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.RateLimiter;
import com.palantir.lock.StringLockDescriptor;
import com.palantir.util.Pair;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.assertj.core.data.Offset;
import org.junit.After;
import org.junit.Test;

public class TargetedSweepAsyncLockTests {

    private static final int REQUESTS_PER_SECOND = 2;
    private final ListeningScheduledExecutorService executorService =
            MoreExecutors.listeningDecorator(Executors.newSingleThreadScheduledExecutor());
    private final ListeningScheduledExecutorService executorService2 =
            MoreExecutors.listeningDecorator(Executors.newSingleThreadScheduledExecutor());

    @After
    public void after() {
        executorService.shutdown();
        executorService2.shutdown();
    }

    @Test
    public void rateLimitsCorrectly() throws Exception {
        Pair<Meter, ListenableFuture<?>> exclusiveRate = run(
                new ExclusiveLock(StringLockDescriptor.of("non rate limited")),
                executorService);
        Pair<Meter, ListenableFuture<?>> rateLimitedRate = run(
                new TargetedSweepAsyncLock(
                        new ExclusiveLock(StringLockDescriptor.of("rate limited")),
                        RateLimiter.create(REQUESTS_PER_SECOND),
                        () -> true,
                        mock(ScheduledExecutorService.class),
                        AsyncLock::unlock),
                executorService2);
        ListenableFuture<List<Object>> futureOfAll = Futures.allAsList(exclusiveRate.rhSide, rateLimitedRate.rhSide);
        Thread.sleep(10_000);
        futureOfAll.cancel(true);

        assertThat(exclusiveRate.lhSide.getMeanRate())
                .isGreaterThan(REQUESTS_PER_SECOND);

        assertThat(rateLimitedRate.lhSide.getMeanRate())
                .isCloseTo(REQUESTS_PER_SECOND, Offset.offset(0.3));
    }

    private static Pair<Meter, ListenableFuture<?>> run(
            AsyncLock asyncLock,
            ListeningScheduledExecutorService executorService) {
        Meter meter = new Meter();
        ListenableFuture<?> future = executorService.scheduleWithFixedDelay(() -> {
            UUID uuid = UUID.randomUUID();
            asyncLock.lock(uuid);
            try {
                meter.mark();
            } finally {
                asyncLock.unlock(uuid);
            }
        }, 0, 50, TimeUnit.MILLISECONDS);
        return Pair.create(meter, future);
    }

}
