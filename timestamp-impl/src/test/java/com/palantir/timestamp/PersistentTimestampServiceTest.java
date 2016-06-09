/**
 * Copyright 2015 Palantir Technologies
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
import static org.junit.Assert.fail;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.atMost;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.jmock.Expectations;
import org.jmock.Mockery;
import org.jmock.lib.concurrent.Synchroniser;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.exceptions.verification.TooLittleActualInvocations;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Futures;
import com.jayway.awaitility.Awaitility;
import com.jayway.awaitility.Duration;
import com.palantir.common.concurrent.PTExecutors;
import com.palantir.common.remoting.ServiceNotAvailableException;
import com.palantir.common.time.Clock;

public class PersistentTimestampServiceTest {

    @Rule
    public final ExpectedException expectedException = ExpectedException.none();

    @Test
    public void testFastForward() {
        Mockery m = new Mockery();
        m.setThreadingPolicy(new Synchroniser());
        final TimestampBoundStore tbsMock = m.mock(TimestampBoundStore.class);
        final long initialValue = 1234567L;
        final long futureTimestamp = 12345678L;
        m.checking(new Expectations() {{
            oneOf(tbsMock).getUpperLimit(); will(returnValue(initialValue));
            oneOf(tbsMock).storeUpperLimit(initialValue + PersistentTimestampService.ALLOCATION_BUFFER_SIZE);
            oneOf(tbsMock).storeUpperLimit(futureTimestamp + PersistentTimestampService.ALLOCATION_BUFFER_SIZE);
        }});

        final PersistentTimestampService ptsService = PersistentTimestampService.create(tbsMock);
        for (int i = 1; i <= 1000; i++) {
            assertEquals(initialValue+i, ptsService.getFreshTimestamp());
        }

        ptsService.fastForwardTimestamp(futureTimestamp);
        for (int i = 1; i <= 1000; i++) {
            assertEquals(futureTimestamp+i, ptsService.getFreshTimestamp());
        }

        m.assertIsSatisfied();
    }

    @Test
    public void incrementUpperLimitIfOneMinuteElapsedSinceLastUpdate() throws InterruptedException {
        Clock clock = mock(Clock.class);
        when(clock.getTimeMillis())
                .thenReturn(0L,
                        minutesToMillis(2),
                        minutesToMillis(4),
                        minutesToMillis(6));
        TimestampBoundStore timestampBoundStore = initialTimestampBoundStore();
        PersistentTimestampService persistentTimestampService = PersistentTimestampService.create(timestampBoundStore, clock);

        persistentTimestampService.getFreshTimestamp();
        persistentTimestampService.getFreshTimestamp();
        Awaitility
                .await("awaitStoreUpperLimitCalledAtLeastTwice")
                .timeout(Duration.ONE_MINUTE)
                .ignoreExceptionsMatching(e -> e.getCause() instanceof TooLittleActualInvocations)
                .until(() -> {
                    try {
                        verify(timestampBoundStore, atLeast(2)).storeUpperLimit(anyLong());
                    } catch (TooLittleActualInvocations e) {
                        throw new RuntimeException(e);
                    }
                });
    }

    @Test
    public void doNotIncrementUpperLimitTooManyTimes() {
        // In the current implementation, it is possible for a single call to getFreshTimestamp to invoke storeUpperLimit more than once.
        // However, the number of such invocations should not be massive
        TimestampBoundStore timestampBoundStore = initialTimestampBoundStore();
        PersistentTimestampService persistentTimestampService = PersistentTimestampService.create(timestampBoundStore);

        persistentTimestampService.getFreshTimestamp();

        verify(timestampBoundStore, atMost(2)).storeUpperLimit(anyLong());
    }

    @Test
    public void incrementUpperLimitOnFirstFreshTimestampRequest() {
        TimestampBoundStore timestampBoundStore = initialTimestampBoundStore();
        PersistentTimestampService persistentTimestampService = PersistentTimestampService.create(timestampBoundStore);

        persistentTimestampService.getFreshTimestamp();

        verify(timestampBoundStore).storeUpperLimit(PersistentTimestampService.ALLOCATION_BUFFER_SIZE);
    }

    @Test
    public void multipleFreshTimestampRequestsShouldIncreaseUpperLimitOnlyOnce() {
        TimestampBoundStore timestampBoundStore = initialTimestampBoundStore();
        PersistentTimestampService persistentTimestampService = PersistentTimestampService.create(timestampBoundStore);

        getFreshTimestampsInParallel(persistentTimestampService, 20);

        verify(timestampBoundStore, times(1)).storeUpperLimit(PersistentTimestampService.ALLOCATION_BUFFER_SIZE);
    }

    @Test
    public void throwOnTimestampRequestIfBoundStoreCannotStoreNewUpperLimit() {
        PersistentTimestampService persistentTimestampService = PersistentTimestampService.create(failingTimestampBoundStore());

        expectedException.expect(ServiceNotAvailableException.class);
        persistentTimestampService.getFreshTimestamp();
    }

    @Test
    public void testLimit() throws InterruptedException {
        Mockery m = new Mockery();
        m.setThreadingPolicy(new Synchroniser());
        final TimestampBoundStore tbsMock = m.mock(TimestampBoundStore.class);
        final long initialValue = 72;
        m.checking(new Expectations() {{
            oneOf(tbsMock).getUpperLimit(); will(returnValue(initialValue));
            oneOf(tbsMock).storeUpperLimit(with(any(Long.class)));
            // Throws exceptions after here, which will prevent allocating more timestamps.
        }});

        // Use up all initially-allocated timestamps.
        final TimestampService tsService = PersistentTimestampService.create(tbsMock);
        for (int i = 1; i <= PersistentTimestampService.ALLOCATION_BUFFER_SIZE; ++i) {
            assertEquals(initialValue+i, tsService.getFreshTimestamp());
        }

        ExecutorService exec = PTExecutors.newSingleThreadExecutor();
        Future<?> f = exec.submit(new Runnable() {
            @Override
            public void run() {
                // This will block.
                tsService.getFreshTimestamp();
            }
        });

        try {
            f.get(10, TimeUnit.MILLISECONDS);
            fail("We should be blocking");
        } catch (ExecutionException e) {
            // we expect this failure because we can't allocate timestamps
        } catch (TimeoutException e) {
            // We expect this timeout, as we're blocking.
        } finally {
            f.cancel(true);
            exec.shutdown();
        }
    }

    private void getFreshTimestampsInParallel(PersistentTimestampService persistentTimestampService, int numTimes) {
        ExecutorService executorService = Executors.newFixedThreadPool(numTimes / 2);
        try {
            List<Future<Long>> futures = Lists.newArrayListWithExpectedSize(numTimes);
            for (int i = 0; i < numTimes; i++) {
                Future<Long> future = executorService.submit(new Callable<Long>() {
                    @Override
                    public Long call() throws Exception {
                        return persistentTimestampService.getFreshTimestamp();
                    }
                });
                futures.add(future);
            }
            for (int i = 0; i < futures.size(); i++) {
                Futures.getUnchecked(futures.get(i));
            }
        } finally {
            executorService.shutdown();
        }
    }

    private TimestampBoundStore initialTimestampBoundStore() {
        TimestampBoundStore timestampBoundStore = mock(TimestampBoundStore.class);
        when(timestampBoundStore.getUpperLimit()).thenReturn(0L);
        return timestampBoundStore;
    }

    private TimestampBoundStore failingTimestampBoundStore() {
        TimestampBoundStore timestampBoundStore = mock(TimestampBoundStore.class);
        when(timestampBoundStore.getUpperLimit()).thenReturn(0L);
        doThrow(new MultipleRunningTimestampServiceError("error")).when(timestampBoundStore).storeUpperLimit(anyLong());
        return timestampBoundStore;
    }

    private static long minutesToMillis(long minutes) {
        return TimeUnit.MINUTES.toMillis(minutes);
    }
}
